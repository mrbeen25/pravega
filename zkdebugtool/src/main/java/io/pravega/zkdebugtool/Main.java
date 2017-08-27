/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.zkdebugtool;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.ZKCheckpointStore;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.tables.IndexRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.Serializable;
import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException, CheckpointStoreException, ReinitializationRequiredException {
        System.out.println("Enter zkURL : ");
        Scanner scanner = new Scanner(System.in);
        String zkURL = scanner.nextLine();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

        while (true) {
            System.out.println("Enter Scope: ");
            String scope = scanner.nextLine();
            System.out.println("Enter Stream name: ");
            String stream = scanner.nextLine();

            System.out.println("1. Stream data \n" +
                    "2. Stream metadata \n" +
                    "3. Quit");
            Integer option = Integer.parseInt(scanner.nextLine());

            switch (option) {
                case 1:
                    readStreamData(scope, stream, executor);
                    break;
                case 2:
                    handleStreamMetadata(scope, stream, zkURL, executor);
                    break;
                default:
                    System.exit(0);
            }
        }

    }

    public static void printAllMetadata(String scope, String stream, String zkURL, ScheduledExecutorService executor) {
        try {
            String ns = "pravega/pravega-cluster";
            final CuratorFramework zkClient = createZKClient(zkURL, ns);

            ZKStream zkStream = new ZKStream(scope, stream, new ZKStoreHelper(zkClient, executor));
            StreamMetadataStore store = StreamStoreFactory.createZKStore(zkClient, executor);
            log.info("printing segment table");
            printSegmentTable(zkStream);
            log.info("printing history table");
            printHistoryTable(zkStream);
            store.getScaleMetadata(scope, stream, null, executor)
                    .thenAccept(scales -> scales.forEach(x -> {
                        System.out.println("scales --> " + x);
                    })).join();
            List<Segment> activeSegments = store.getActiveSegments(scope, stream, null, executor).join();
            activeSegments.stream()
                    .filter(x -> store.isCold(scope, stream, x.getNumber(), null, executor).join())
                    .forEach(x -> System.out.println("cold segment:: " + x.getNumber()));
            System.out.println("configuration = " + store.getConfiguration(scope, stream, null, executor).join());
            System.out.println("state = " + store.getState(scope, stream, null, executor).join());
            System.out.println("epoch = " + store.getActiveEpoch(scope, stream, null, true, executor).join());
            printIndexTable(zkStream);
        } catch (Exception e) {
            log.error("error thrown while reading stream metadata {}", e);
        }

    }

    private static void readStreamData(String scope, String stream, ScheduledExecutorService executor) throws ReinitializationRequiredException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("enter controller URI:");
        URI controllerUri = URI.create(scanner.nextLine());
        Controller controller = new ControllerImpl(controllerUri, ControllerImplConfig.builder().retryAttempts(1).build(), executor);

        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerUri);
        String readerGroupName = "RG" + scope + stream + new Random().nextInt(Integer.MAX_VALUE);
        ReaderGroupConfig.ReaderGroupConfigBuilder readerGroupConfigBuilder = ReaderGroupConfig.builder().startingTime(0);

        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfigBuilder.build(),
                Collections.singleton(stream));
        final EventStreamReader<Serializable> reader = clientFactory.createReader("solo",
                readerGroupName,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());

        System.out.println("1. Printing all events");
        while (true) {
            EventRead<Serializable> event = reader.readNextEvent(10000);
            if (event.getEvent() != null) {
                System.out.println(event.getEvent());
            } else {
                break;
            }
        }
    }

    private static void handleStreamMetadata(String scope, String stream, String zkURL, ScheduledExecutorService executor) throws ExecutionException, InterruptedException, CheckpointStoreException {
        // print all segment table records
        String ns = "pravega/pravega-cluster";
        final CuratorFramework zkClient = createZKClient(zkURL, ns);

        ZKStream zkStream = new ZKStream(scope, stream, new ZKStoreHelper(zkClient, executor));
        StreamMetadataStore store = StreamStoreFactory.createZKStore(zkClient, executor);
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("1. Segment Table \n" +
                    "2. History Table \n" +
                    "3. Scale Records \n" +
                    "4. Cold segments \n" +
                    "5. Stream configuration \n" +
                    "6. state \n" +
                    "7. epoch \n" +
                    "8. Transaction \n" +
                    "9. Checkpoint \n" +
                    "10. index table \n" +
                    "11. back");
            Integer option = Integer.parseInt(scanner.nextLine());

            switch (option) {
                case 1:
                    log.info("printing segment table");
                    printSegmentTable(zkStream);
                    break;
                case 2:
                    log.info("printing history table");
                    printHistoryTable(zkStream);
                    break;
                case 3:
                    store.getScaleMetadata(scope, stream, null, executor)
                            .thenAccept(scales -> scales.forEach(x -> {
                                System.out.println("scales --> " + x);
                            })).join();
                    break;
                case 4:
                    List<Segment> activeSegments = store.getActiveSegments(scope, stream, null, executor).join();
                    activeSegments.stream()
                            .filter(x -> store.isCold(scope, stream, x.getNumber(), null, executor).join())
                            .forEach(x -> System.out.println("cold segment:: " + x.getNumber()));
                    break;
                case 5:
                    System.out.println("configuration = " + store.getConfiguration(scope, stream, null, executor).join());
                    break;
                case 6:
                    System.out.println("state = " + store.getState(scope, stream, null, executor).join());
                    break;
                case 7:
                    System.out.println("epoch = " + store.getActiveEpoch(scope, stream, null, true, executor).join());
                    break;
                case 8:
                    System.out.println("Txn UUID: ");
                    String txID = scanner.nextLine();

                    if (!Strings.isNullOrEmpty(txID)) {
                        printTxnDetails(txID, zkStream);
                    }
                    break;
                case 9:
                    readCheckpoint(zkClient, scanner);
                    break;
                case 10:
                    printIndexTable(zkStream);
                    break;
                default:
                    return;
            }
            System.out.println("press any key to go back:");
            scanner.nextLine();
        }
    }

    private static void readCheckpoint(CuratorFramework zkClient, Scanner scanner) throws CheckpointStoreException {
        JavaSerializer<Position> serializer = new JavaSerializer<Position>();
        CheckpointStore checkpointStore = new ZKCheckpointStore(zkClient);
        Set<String> processes = checkpointStore.getProcesses();
        AtomicInteger i = new AtomicInteger(1);
        System.out.println("processes:");
        processes.forEach(x -> System.out.println(i.getAndIncrement() + ". = " + x));
        System.out.println("select");
        String process = Lists.newArrayList(processes).get(Integer.parseInt(scanner.nextLine()) - 1);
        List<String> readerGroups = checkpointStore.getReaderGroups(process);
        System.out.println("reader groups: ");
        readerGroups.forEach(x -> System.out.println(i.getAndIncrement() + ". = " + x));
        System.out.println("select");
        String readerGroup = Lists.newArrayList(processes).get(Integer.parseInt(scanner.nextLine()) - 1);
        Map<String, Position> positions = checkpointStore.getPositions(process, readerGroup);
        positions.forEach((key, value) -> System.out.println("reader:" + key + " position base64:" +
                Base64.getEncoder().encodeToString(serializer.serialize(value).array())));
    }

    private static void printTxnDetails(String txID, ZKStream zkStream) throws InterruptedException, ExecutionException {
        if ((txID != null) && !txID.isEmpty()) {
            zkStream.getTransactionData(UUID.fromString(txID)).thenAccept(d -> System.out.println("Txn Data: " + d)
            ).get();
        } else {
            System.out.println("TxID is null ");
        }
    }

    private static void printSegmentTable(ZKStream zkStream) throws ExecutionException, InterruptedException {
        zkStream.getSegmentTable().thenAccept(d -> {
            int numberOfEntries = d.getData().length / SegmentRecord.SEGMENT_RECORD_SIZE;
            List<SegmentRecord> records = SegmentRecord.readLastN(d.getData(), numberOfEntries);
            System.out.println("Number of SegmentRecords: " + records.size());
            System.out.println("Segment Table : ");
            records.forEach(r -> System.out.println(r));
        }).get();
    }

    private static void printIndexTable(ZKStream zkStream) throws ExecutionException, InterruptedException {
        zkStream.getIndexTable().thenAccept(d -> {
            int offset = d.getData().length - IndexRecord.INDEX_RECORD_SIZE;
            Optional<IndexRecord> record = IndexRecord.readLatestRecord(d.getData());
            System.out.println("Index Table : ");
            while (record.isPresent()) {
                System.out.println(record.get());
                record = IndexRecord.fetchPrevious(d.getData(), offset - IndexRecord.INDEX_RECORD_SIZE);
            }
        }).get();
    }

    private static CuratorFramework createZKClient(String zkURL, String ns) {
        //Create and initialize the curator client framework.
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkURL)
                .namespace(ns)
                .retryPolicy(new ExponentialBackoffRetry(1,
                        30 * 1000))
                .sessionTimeoutMs(60 * 1000)
                .build();
        zkClient.start();
        return zkClient;
    }

    private static void printHistoryTable(ZKStream stream) throws ExecutionException, InterruptedException {
        stream.getHistoryTable().thenAccept(d -> {
            Optional<HistoryRecord> firstRecord = HistoryRecord.readRecord(d.getData(), 0, false);
            System.out.println("History Table: ");
            printHistoryEntry(firstRecord, d.getData());
        }).get();
    }

    private static void printHistoryEntry(Optional<HistoryRecord> r, byte[] d) {
        if (r.isPresent()) {
            System.out.println(r.get());
            Optional<HistoryRecord> r1 = HistoryRecord.fetchNext(r.get(), d, false);
            printHistoryEntry(r1, d);
        }
    }
}

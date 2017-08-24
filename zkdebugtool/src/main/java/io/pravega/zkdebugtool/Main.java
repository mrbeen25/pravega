/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.zkdebugtool;
import com.google.common.base.Strings;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Enter zkURL : ");
        Scanner scanner = new Scanner(System.in);
        String zkURL = scanner.nextLine();
        System.out.println("Enter Scope: ");
        String scope = scanner.nextLine();
        System.out.println("Enter Stream name: ");
        String stream = scanner.nextLine();
        System.out.println("Txn UUID: ");
        String txID = scanner.nextLine();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

        // print all segment table records
        String ns = "pravega/pravega-cluster";

        final CuratorFramework zkClient = createZKClient(zkURL, ns);

        ZKStream zkStream = new ZKStream(scope, stream, new ZKStoreHelper(zkClient, executor));

        if (!Strings.isNullOrEmpty(txID)) {
            printTxnDetails(txID, zkStream);
        }

        log.info("printing history table");
        printHistoryTable(zkStream);
        log.info("printing segment table");
        printSegmentTable(zkStream);

        StreamMetadataStore store = StreamStoreFactory.createZKStore(zkClient, executor);

        log.info("state {}", store.getState(scope, stream, null, executor).join());
        log.info("epoch {}", store.getActiveEpoch(scope, stream, null, true, executor).join());

        store.getScaleMetadata(scope, stream, null, executor)
                .thenAccept(scales -> {
                    scales.stream().forEach(x -> {
                        log.info("scales --> {}", x);
                    });
                }).join();

        log.info("printing cold segments");
        List<Segment> activeSegments = store.getActiveSegments(scope, stream, null, executor).join();
        activeSegments.stream()
                .filter(x -> store.isCold(scope, stream, x.getNumber(), null, executor).join())
                .forEach(x -> log.info("cold segment:: {}", x.getNumber()));
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

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

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.Cache;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.store.stream.tables.TableHelper;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream {
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAM_PATH = SCOPE_PATH + "/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";
    private static final String MARKER_PATH = STREAM_PATH + "/markers";

    private final ZKStoreHelper store;
    private final String creationPath;
    private final String configurationPath;
    private final String statePath;
    private final String segmentPath;
    private final String historyPath;
    private final String indexPath;
    private final String activeTxRoot;
    private final String markerPath;
    private final String scopePath;
    private final String streamPath;

    private final String scope;
    private final String name;

    private final Cache<Integer> cache;

    public ZKStream(final String scopeName, final String streamName, ZKStoreHelper storeHelper) {
        name = streamName;
        scope = scopeName;
        store = storeHelper;
        scopePath = String.format(SCOPE_PATH, scopeName);
        streamPath = String.format(STREAM_PATH, scopeName, streamName);
        creationPath = String.format(CREATION_TIME_PATH, scopeName, streamName);
        configurationPath = String.format(CONFIGURATION_PATH, scopeName, streamName);
        statePath = String.format(STATE_PATH, scopeName, streamName);
        segmentPath = String.format(SEGMENT_PATH, scopeName, streamName);
        historyPath = String.format(HISTORY_PATH, scopeName, streamName);
        indexPath = String.format(INDEX_PATH, scopeName, streamName);
        activeTxRoot = String.format(ZKStoreHelper.STREAM_TX_ROOT, scopeName, streamName);
        markerPath = String.format(MARKER_PATH, scopeName, streamName);

        cache = new Cache<>(store::getData);
    }

    // region overrides

    /**
     * Method to check whether a scope exists before creating a stream under that scope.
     *
     * @return A future either returning a result or an exception.
     */
    public CompletableFuture<Void> checkScopeExists() {
        return store.checkExists(scopePath)
                .thenAccept(x -> {
                    if (!x) {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopePath);
                    }
                });
    }


    CompletableFuture<Data<Integer>> getMarkerData(int segmentNumber) {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));
        cache.getCachedData(path)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionHelpers.getRealException(ex);
                        if (cause instanceof StoreException.DataNotFoundException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(cause);
                        }
                    } else {
                        result.complete(res);
                    }
                });

        return result;
    }

    public CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch(boolean ignoreCached) {
        CompletableFuture<Data<Integer>> historyTableFuture = ignoreCached ? getHistoryTableFromStore() :
                getHistoryTable();

        return historyTableFuture.thenApply(table -> TableHelper.getActiveEpoch(table.getData()));
    }

    public CompletableFuture<Pair<Integer, List<Integer>>> getLatestEpoch() {
        return getHistoryTable().thenApply(table -> TableHelper.getLatestEpoch(table.getData()));
    }

    CompletableFuture<Integer> getTransactionEpoch(UUID txId) {
        return store.getChildren(activeTxRoot).thenCompose(list -> {
            Map<String, CompletableFuture<Boolean>> map = new HashMap<>();
            for (String str : list) {
                int epoch = Integer.parseInt(str);
                String activeTxnPath = getActiveTxPath(epoch, txId.toString());
                map.put(str, store.checkExists(activeTxnPath));
            }
            return FutureHelpers.allOfWithResults(map);
        }).thenApply(map -> {
            Optional<Map.Entry<String, Boolean>> opt = map.entrySet().stream().filter(Map.Entry::getValue).findFirst();
            if (opt.isPresent()) {
                return Integer.parseInt(opt.get().getKey());
            } else {
                throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                        "Stream: " + getName() + " Transaction: " + txId.toString());
            }
        });
    }

    //    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final int epoch, final UUID txId) {
        final String activeTxPath = getActiveTxPath(epoch, txId.toString());
        return store.getData(activeTxPath);
    }

    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return cache.getCachedData(configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x.getData()));
    }

    CompletableFuture<Data<Integer>> getStateData() {
        return cache.getCachedData(statePath);
    }

    public CompletableFuture<Segment> getSegmentRow(final int number) {
        return getSegmentTable()
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    public CompletableFuture<Data<Integer>> getSegmentTable() {
        return cache.getCachedData(segmentPath);
    }

    CompletableFuture<Data<Integer>> getSegmentTableFromStore() {
        cache.invalidateCache(segmentPath);
        return getSegmentTable();
    }

    CompletableFuture<Void> setSegmentTable(final Data<Integer> data) {
        return store.setData(segmentPath, data)
                .whenComplete((r, e) -> cache.invalidateCache(segmentPath));
    }

    public CompletableFuture<Data<Integer>> getHistoryTable() {
        return cache.getCachedData(historyPath);
    }

    CompletableFuture<Data<Integer>> getHistoryTableFromStore() {
        cache.invalidateCache(historyPath);
        return getHistoryTable();
    }

    public CompletableFuture<Data<Integer>> getIndexTable() {
        return cache.getCachedData(indexPath);
    }

    // endregion

    // region private helpers
    private String getActiveTxPath(final long epoch, final String txId) {
        return ZKPaths.makePath(ZKPaths.makePath(activeTxRoot, Long.toString(epoch)), txId);
    }

    public String getName() {
        return this.name;
    }

    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        return getTransactionEpoch(txId).thenCompose(epoch -> getActiveTx(epoch, txId)
                .thenApply(data -> {
                    ActiveTxnRecord activeTxnRecord = ActiveTxnRecord.parse(data.getData());
                    return new VersionedTransactionData(epoch, txId, data.getVersion(),
                            activeTxnRecord.getTxnStatus(), activeTxnRecord.getTxCreationTimestamp(),
                            activeTxnRecord.getMaxExecutionExpiryTime(), activeTxnRecord.getScaleGracePeriod());
                }));
    }

}

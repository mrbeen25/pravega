/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * Helper class that allows appending Log Operations to available InMemory Structures.
 */
@ThreadSafe
class MemoryStateUpdater {
    //region Private

    private final ReadIndex readIndex;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final Runnable flushCallback;
    @GuardedBy("transactions")
    private final ArrayDeque<Transaction> transactions;
    private final AtomicBoolean recoveryMode;
    private final Supplier<Long> nextTransactionId = new AtomicLong(0)::incrementAndGet;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex) {
        this(inMemoryOperationLog, readIndex, null);
    }

    /**
     * Creates a new instance of the MemoryStateUpdater class.
     *
     * @param inMemoryOperationLog InMemory Operation Log.
     * @param readIndex            The ReadIndex to update.
     * @param flushCallback        (Optional) A callback to be invoked whenever flush() is invoked.
     */
    MemoryStateUpdater(SequencedItemList<Operation> inMemoryOperationLog, ReadIndex readIndex, Runnable flushCallback) {
        Preconditions.checkNotNull(readIndex, "readIndex");
        Preconditions.checkNotNull(inMemoryOperationLog, "inMemoryOperationLog");

        this.inMemoryOperationLog = inMemoryOperationLog;
        this.readIndex = readIndex;
        this.flushCallback = flushCallback;
        this.transactions = new ArrayDeque<>();
        this.recoveryMode = new AtomicBoolean();
    }

    //endregion

    //region Operations

    /**
     * Puts the Log Updater in Recovery Mode, using the given Metadata Source as interim.
     *
     * @param recoveryMetadataSource The metadata to use during recovery.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
        this.readIndex.enterRecoveryMode(recoveryMetadataSource);
        this.recoveryMode.set(true);
    }

    /**
     * Puts the Log Updater out of Recovery Mode, using the given Metadata Source as final.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the operations may be reverted and
     *                           the contents of the memory structures may be cleared out.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException {
        this.readIndex.exitRecoveryMode(successfulRecovery);
        this.recoveryMode.set(false);
    }

    Transaction beginTransaction() {
        Preconditions.checkState(!this.recoveryMode.get(), "MemoryStateUpdater.Transactions are only applicable in non-recovery mode.");
        Transaction result = new Transaction(this.nextTransactionId.get(), this);
        synchronized (this.transactions) {
            this.transactions.addLast(result);
        }

        return result;
    }

    private void commitTransaction(Transaction txn) throws DataCorruptionException {
        HashSet<Long> segmentIds = new HashSet<>();
        synchronized (this.transactions) {
            Preconditions.checkArgument(!this.transactions.isEmpty()
                            && txn.id >= this.transactions.peekFirst().id
                            && txn.id <= this.transactions.peekLast().id,
                    "Given transaction has already been committed.");
            while (!this.transactions.isEmpty() && this.transactions.peekFirst().id <= txn.id) {
                Transaction toProcess = this.transactions.removeFirst();
                for (Operation operation : toProcess.operations) {
                    long segmentId = process(operation);
                    if (segmentId >= 0) {
                        segmentIds.add(segmentId);
                    }
                }
            }
        }

        this.readIndex.triggerFutureReads(segmentIds);
        if (this.flushCallback != null) {
            this.flushCallback.run();
        }
    }

    /**
     * Processes the given operation and applies it to the ReadIndex and InMemory OperationLog.
     *
     * @param operation The operation to process.
     * @throws DataCorruptionException If a serious, non-recoverable, data corruption was detected, such as trying to
     *                                 append operations out of order.
     */
    long process(Operation operation) throws DataCorruptionException {
        long segmentId = -1;
        if (!operation.canSerialize()) {
            // Nothing to do.
            return segmentId;
        }

        // Add entry to MemoryTransactionLog and ReadIndex/Cache. This callback is invoked from the QueueProcessor,
        // which always acks items in order of Sequence Number - so the entries should be ordered (but always check).
        if (operation instanceof StorageOperation) {
            segmentId = addToReadIndex((StorageOperation) operation);
            if (operation instanceof StreamSegmentAppendOperation) {
                // Transform a StreamSegmentAppendOperation into its corresponding Cached version.
                try {
                    operation = new CachedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operation);
                } catch (Throwable ex) {
                    if (ExceptionHelpers.mustRethrow(ex)) {
                        throw ex;
                    } else {
                        throw new DataCorruptionException(String.format("Unable to create a CachedStreamSegmentAppendOperation from operation '%s'.", operation), ex);
                    }
                }
            }
        }

        boolean added = this.inMemoryOperationLog.add(operation);
        if (!added) {
            // This is a pretty nasty one. It's safer to shut down the container than continue.
            // We either recorded the Operation correctly, but invoked this callback out of order, or we really
            // recorded the Operation in the wrong order (by sequence number). In either case, we will be inconsistent
            // while serving reads, so better stop now than later.
            throw new DataCorruptionException("About to have added a Log Operation to InMemoryOperationLog that was out of order.");
        }

        return segmentId;
    }

    /**
     * Registers the given operation in the ReadIndex.
     *
     * @param operation The operation to register.
     */
    private long addToReadIndex(StorageOperation operation) {
        if (operation instanceof StreamSegmentAppendOperation) {
            // Record a StreamSegmentAppendOperation. Just in case, we also support this type of operation, but we need to
            // log a warning indicating so. This means we do not optimize memory properly, and we end up storing data
            // in two different places.
            StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) operation;
            this.readIndex.append(appendOperation.getStreamSegmentId(),
                    appendOperation.getStreamSegmentOffset(),
                    appendOperation.getData());
        } else if (operation instanceof MergeTransactionOperation) {
            // Record a MergeTransactionOperation. We call beginMerge here, and the StorageWriter will call completeMerge.
            MergeTransactionOperation mergeOperation = (MergeTransactionOperation) operation;
            this.readIndex.beginMerge(mergeOperation.getStreamSegmentId(),
                    mergeOperation.getStreamSegmentOffset(),
                    mergeOperation.getTransactionSegmentId());
        } else {
            assert !(operation instanceof CachedStreamSegmentAppendOperation)
                    : "attempted to add a CachedStreamSegmentAppendOperation to the ReadIndex";
        }

        // Record recent activity on stream segment, if applicable.
        // We should record this for any kind of StorageOperation. When we issue 'triggerFutureReads' on the readIndex,
        // it should include 'sealed' StreamSegments too - any Future Reads waiting on that Offset will be cancelled.
        return operation.getStreamSegmentId();
    }

    //endregion

    @NotThreadSafe
    @RequiredArgsConstructor
    static class Transaction {
        private final long id;
        private final MemoryStateUpdater updater;
        private final ArrayList<Operation> operations = new ArrayList<>();

        void add(Operation operation) {
            this.operations.add(operation);
        }

        void commit() throws DataCorruptionException {
            this.updater.commitTransaction(this);
        }
    }
}
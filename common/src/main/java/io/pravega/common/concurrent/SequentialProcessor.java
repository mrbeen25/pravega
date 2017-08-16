/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.GuardedBy;

/**
 * A processor that runs the same task sync/async, but never concurrently. If multiple requests are made
 * during an existing execution of the task, it will be invoked exactly once after the current execution completes.
 */
public class SequentialProcessor implements AutoCloseable {
    //region Members

    private final Runnable runnable;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean runAgain;
    @GuardedBy("this")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Region Constructor.
     *
     * @param runnable The task to run.
     */
    public SequentialProcessor(Runnable runnable) {
        this.runnable = Preconditions.checkNotNull(runnable, "runnable");
    }

    //endregion

    //region Execution

    /**
     * Executes one instance of the task, or queues it up at most once should the task be currently running. If the task
     * is not currently running, it will be executed synchronously on the current thread. If the task is currently running,
     * the new execution will happen on the same thread that the previous task was running on.
     * <p>
     * NOTE: If the task is not currently running, and hence it executes synchronously, it is possible that this method
     * will not return after its execution completes. This happens if subsequent calls to run() or runAsync() are made
     * while the task is running, at which point the subsequent calls will happen on the same thread.
     */
    public void run() {
        // Determine if a task is running. If so, record the fact we want to have it run again, otherwise reserve our spot.
        synchronized (this) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.running) {
                this.runAgain = true;
                return;
            }

            this.running = true;
        }

        // Execute the task.
        boolean canContinue = true;
        while (canContinue) {
            try {
                this.runnable.run();
            } finally {
                // Determine if we need to run the task again. Otherwise release our spot.
                synchronized (this) {
                    canContinue = this.runAgain && !this.closed;
                    this.runAgain = false;
                    this.running = canContinue;
                }
            }
        }
    }

    /**
     * Executes one instance of the task, or queues it up at most once should the task be currently running. If the task
     * is not currently running, it will be invoked on a new thread on the given Executor. If the task is currently running,
     * the new execution will happen on the same thread that the previous task was running on.
     * @param executor An Executor to run the task on.
     */
    public void runAsync(Executor executor) {
        Preconditions.checkNotNull(executor, "executor");

        // Determine if a task is running. If so, record the fact we want to have it run again, otherwise reserve our spot.
        synchronized (this) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.running) {
                this.runAgain = true;
                return;
            }

            this.running = true;
        }

        // Execute the task.
        executor.execute(() -> {
            boolean canContinue = true;
            while (canContinue) {
                try {
                    this.runnable.run();
                } finally {
                    // Determine if we need to run the task again. Otherwise release our spot.
                    synchronized (this) {
                        canContinue = this.runAgain && !this.closed;
                        this.runAgain = false;
                        this.running = canContinue;
                    }
                }
            }
        });
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    //endregion
}

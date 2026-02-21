/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.util;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Utility class for making long-running tests cancellation-aware.
 * 
 * <p>This supports JUnit 6's fail-fast mode by allowing tests to check
 * for cancellation requests and exit gracefully, ensuring proper resource
 * cleanup even when the test suite is stopped early.</p>
 * 
 * <p>Usage example:</p>
 * <pre>{@code
 * @Test
 * @Timeout(value = 5, unit = TimeUnit.MINUTES)
 * void testLongRunningOperation() throws Exception {
 *     CancellationAwareTestUtils.CancellationToken token = 
 *         CancellationAwareTestUtils.createToken();
 *     
 *     try {
 *         while (hasMoreWork() && !token.isCancelled()) {
 *             doWork();
 *         }
 *     } finally {
 *         cleanup();
 *     }
 * }
 * }</pre>
 */
public class CancellationAwareTestUtils {

    /**
     * A simple cancellation token that can be checked during long-running operations.
     * The token is automatically cancelled when the current thread is interrupted.
     */
    public static class CancellationToken {
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        @Nullable
        private final BooleanSupplier additionalCheck;

        CancellationToken(@Nullable BooleanSupplier additionalCheck) {
            this.additionalCheck = additionalCheck;
        }

        /**
         * Check if cancellation has been requested.
         * This checks both explicit cancellation and thread interruption.
         * 
         * @return true if the operation should be cancelled
         */
        public boolean isCancelled() {
            if (cancelled.get()) {
                return true;
            }
            if (Thread.currentThread().isInterrupted()) {
                cancelled.set(true);
                return true;
            }
            if (additionalCheck != null && additionalCheck.getAsBoolean()) {
                cancelled.set(true);
                return true;
            }
            return false;
        }

        /**
         * Explicitly request cancellation.
         */
        public void cancel() {
            cancelled.set(true);
        }

        /**
         * Throws InterruptedException if cancellation has been requested.
         * Useful for cooperative cancellation in loops.
         * 
         * @throws InterruptedException if cancelled
         */
        public void throwIfCancelled() throws InterruptedException {
            if (isCancelled()) {
                throw new InterruptedException("Test cancelled");
            }
        }
    }

    /**
     * Creates a new cancellation token.
     * 
     * @return a new CancellationToken instance
     */
    @NonNull
    public static CancellationToken createToken() {
        return new CancellationToken(null);
    }

    /**
     * Creates a cancellation token with an additional cancellation condition.
     * 
     * @param additionalCheck additional condition that triggers cancellation
     * @return a new CancellationToken instance
     */
    @NonNull
    public static CancellationToken createToken(@NonNull BooleanSupplier additionalCheck) {
        return new CancellationToken(additionalCheck);
    }

    /**
     * Executes an operation with periodic cancellation checks.
     * 
     * @param token the cancellation token to check
     * @param operation the operation to execute (should be short-lived)
     * @param iterations number of times to execute the operation
     * @param checkInterval how often to check for cancellation (every N iterations)
     * @return the number of iterations actually completed
     * @throws InterruptedException if cancelled during execution
     */
    public static int executeWithCancellation(
            @NonNull CancellationToken token,
            @NonNull Runnable operation,
            int iterations,
            int checkInterval) throws InterruptedException {
        
        int completed = 0;
        for (int i = 0; i < iterations; i++) {
            if (i % checkInterval == 0) {
                token.throwIfCancelled();
            }
            operation.run();
            completed++;
        }
        return completed;
    }

    /**
     * Sleeps for the specified duration while remaining cancellation-aware.
     * Checks for cancellation every 100ms.
     * 
     * @param token the cancellation token
     * @param millis total milliseconds to sleep
     * @throws InterruptedException if cancelled or interrupted
     */
    public static void sleepWithCancellation(@NonNull CancellationToken token, long millis) 
            throws InterruptedException {
        long remaining = millis;
        while (remaining > 0) {
            token.throwIfCancelled();
            long sleepTime = Math.min(remaining, 100);
            Thread.sleep(sleepTime);
            remaining -= sleepTime;
        }
    }

    /**
     * Interface for operations that can be interrupted and resumed.
     * 
     * @param <T> the result type
     */
    @FunctionalInterface
    public interface CancellableOperation<T> {
        /**
         * Execute a portion of the operation.
         * 
         * @param token cancellation token to check
         * @return the result, or null if more work is needed
         * @throws Exception if the operation fails
         */
        @Nullable
        T execute(@NonNull CancellationToken token) throws Exception;
    }

    /**
     * Runs a cancellable operation, returning null if cancelled before completion.
     * 
     * @param <T> the result type
     * @param operation the operation to run
     * @return the result, or null if cancelled
     * @throws Exception if the operation fails (not due to cancellation)
     */
    @Nullable
    public static <T> T runCancellable(@NonNull CancellableOperation<T> operation) throws Exception {
        CancellationToken token = createToken();
        try {
            return operation.execute(token);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}

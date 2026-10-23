package com.anjali.distkv.server;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating properly bounded, named thread pools with predictable
 * rejection policies and clean graceful shutdown semantics.
 */
public final class ThreadPoolFactory {

    private ThreadPoolFactory() {}

    /**
     * Creates a bounded ThreadPoolExecutor suitable for TCP client request handling.
     *
     * @param corePoolSize    minimum number of worker threads kept alive
     * @param maxPoolSize     maximum ceiling of worker threads
     * @param queueCapacity   bounded capacity for pending tasks before rejection occurs
     * @param poolNamePrefix  prefix for naming worker threads (e.g. "distkv-worker")
     * @return a configured {@link ThreadPoolExecutor}
     */
    public static ThreadPoolExecutor createBoundedPool(
            int corePoolSize,
            int maxPoolSize,
            int queueCapacity,
            String poolNamePrefix
    ) {
        ThreadFactory threadFactory = new NamedThreadFactory(poolNamePrefix);

        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy() // Reject cleanly when saturated
        );
    }

    /**
     * Implements a standard two-phase graceful shutdown:
     * 1. Ceases accepting new tasks and waits up to timeoutMs for existing tasks to complete.
     * 2. If tasks remain after timeoutMs, issues interrupt via shutdownNow().
     */
    public static void gracefulShutdown(ExecutorService executor, long timeoutMs) {
        if (executor == null || executor.isShutdown()) {
            return;
        }

        executor.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait for existing tasks to terminate
            if (!executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow(); // Cancel currently executing tasks
                executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + "-" + threadNumber.getAndIncrement());
            thread.setDaemon(true); // Don't prevent JVM shutdown
            return thread;
        }
    }
}

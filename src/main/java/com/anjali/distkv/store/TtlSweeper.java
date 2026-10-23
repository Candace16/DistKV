package com.anjali.distkv.store;

import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Background TTL sweeper that actively purges expired keys.
 *
 * <p>Uses a Min-Heap ({@link PriorityQueue}) ordered by {@code expireAtMillis}.
 * This allows O(1) inspection of the earliest-expiring key and prevents scanning
 * the entire keyspace.
 *
 * <p>A {@link ScheduledExecutorService} periodically triggers {@link #sweep(long)}.
 */
public final class TtlSweeper implements AutoCloseable {

    public record TtlNode(String key, long expireAtMillis) implements Comparable<TtlNode> {
        @Override
        public int compareTo(TtlNode o) {
            return Long.compare(this.expireAtMillis, o.expireAtMillis);
        }
    }

    private final PriorityQueue<TtlNode> minHeap = new PriorityQueue<>();
    private final ReentrantLock heapLock = new ReentrantLock();
    private final ScheduledExecutorService scheduler;
    private final Consumer<String> onExpireCallback;

    /**
     * Constructs a TtlSweeper with an expiry callback and a sweep interval.
     *
     * @param onExpireCallback callback invoked when a key has expired (typically calls store.del(key))
     * @param intervalMs       interval in milliseconds between background sweep passes
     */
    public TtlSweeper(Consumer<String> onExpireCallback, long intervalMs) {
        this.onExpireCallback = onExpireCallback;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "distkv-ttl-sweeper");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleWithFixedDelay(
                this::sweepCurrent,
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Registers a key to be monitored for expiration.
     *
     * @param key            key name
     * @param expireAtMillis absolute expiration time in epoch milliseconds
     */
    public void schedule(String key, long expireAtMillis) {
        heapLock.lock();
        try {
            minHeap.add(new TtlNode(key, expireAtMillis));
        } finally {
            heapLock.unlock();
        }
    }

    /**
     * Executes a single sweep pass against the min-heap using current system time.
     */
    public void sweepCurrent() {
        sweep(System.currentTimeMillis());
    }

    /**
     * Checks the min-heap and triggers the expiry callback for all keys whose
     * expiry timestamp is &lt;= currentTimeMillis.
     *
     * @param currentTimeMillis timestamp against which to check expiry
     * @return number of expired keys swept in this pass
     */
    public int sweep(long currentTimeMillis) {
        int sweptCount = 0;
        heapLock.lock();
        try {
            while (!minHeap.isEmpty()) {
                TtlNode earliest = minHeap.peek();
                if (earliest.expireAtMillis() > currentTimeMillis) {
                    // Min-heap invariant: if the top element hasn't expired, none of the rest have!
                    break;
                }

                minHeap.poll(); // Remove expired node from heap
                sweptCount++;

                // Notify store to purge this key
                onExpireCallback.accept(earliest.key());
            }
        } finally {
            heapLock.unlock();
        }
        return sweptCount;
    }

    /**
     * Returns the number of entries currently scheduled in the min-heap.
     */
    public int heapSize() {
        heapLock.lock();
        try {
            return minHeap.size();
        } finally {
            heapLock.unlock();
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}

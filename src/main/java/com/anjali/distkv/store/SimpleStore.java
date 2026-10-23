package com.anjali.distkv.store;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A thread-safe, single-lock implementation of {@link Store} backed by {@link LRUCache}.
 *
 * <p>All operations are guarded by a single {@link ReentrantLock}.
 * Lazy TTL expiration is checked on every read/exists/ttl call.
 */
public final class SimpleStore implements Store {

    /**
     * Stored entry value along with its absolute expiration timestamp in epoch milliseconds.
     */
    public record StoredItem(String value, long expireAtMillis) {
        public static final long NO_EXPIRY = -1L;

        public boolean isExpired(long now) {
            return expireAtMillis != NO_EXPIRY && now >= expireAtMillis;
        }
    }

    private final LRUCache<String, StoredItem> cache;
    private final ReentrantLock lock = new ReentrantLock();

    // Metrics
    private final AtomicLong totalOps = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final long startTimeMillis = System.currentTimeMillis();

    /**
     * Creates a SimpleStore with the given maximum LRU capacity.
     *
     * @param capacity maximum number of keys to retain
     */
    public SimpleStore(int capacity) {
        this.cache = new LRUCache<>(capacity);
    }

    @Override
    public void set(String key, String value, OptionalLong ttlSeconds) {
        lock.lock();
        try {
            totalOps.incrementAndGet();
            long expireAt = ttlSeconds.isPresent()
                    ? System.currentTimeMillis() + (ttlSeconds.getAsLong() * 1000L)
                    : StoredItem.NO_EXPIRY;

            int sizeBefore = cache.size();
            cache.put(key, new StoredItem(value, expireAt));
            // If cache was already at capacity and a new key was inserted, an eviction occurred
            if (sizeBefore == cache.capacity() && !cache.containsKey(key)) {
                evictions.incrementAndGet();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String get(String key) {
        lock.lock();
        try {
            totalOps.incrementAndGet();
            StoredItem item = cache.get(key);
            if (item == null) {
                misses.incrementAndGet();
                return null;
            }

            long now = System.currentTimeMillis();
            if (item.isExpired(now)) {
                // Lazy expiration: delete expired item
                cache.remove(key);
                misses.incrementAndGet();
                return null;
            }

            hits.incrementAndGet();
            return item.value();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean del(String key) {
        lock.lock();
        try {
            totalOps.incrementAndGet();
            StoredItem removed = cache.remove(key);
            return removed != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean exists(String key) {
        lock.lock();
        try {
            totalOps.incrementAndGet();
            StoredItem item = cache.get(key);
            if (item == null) {
                return false;
            }

            if (item.isExpired(System.currentTimeMillis())) {
                cache.remove(key);
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long ttl(String key) {
        lock.lock();
        try {
            totalOps.incrementAndGet();
            StoredItem item = cache.get(key);
            if (item == null) {
                return -2; // Key does not exist
            }

            long now = System.currentTimeMillis();
            if (item.isExpired(now)) {
                cache.remove(key);
                return -2;
            }

            if (item.expireAtMillis() == StoredItem.NO_EXPIRY) {
                return -1; // Key exists with no expiry
            }

            long remainingSeconds = (item.expireAtMillis() - now) / 1000L;
            return Math.max(0, remainingSeconds);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void dump(java.util.function.BiConsumer<String, StoredItem> consumer) {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            cache.forEach((k, v) -> {
                if (!v.isExpired(now)) {
                    consumer.accept(k, v);
                }
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String statsJson() {
        lock.lock();
        try {
            long total = totalOps.get();
            long h = hits.get();
            long m = misses.get();
            long elapsedSeconds = Math.max(1, (System.currentTimeMillis() - startTimeMillis) / 1000L);
            long opsPerSec = total / elapsedSeconds;
            double hitRate = (h + m == 0) ? 0.0 : ((double) h / (h + m));

            return String.format(
                    "{\"ops_per_sec\":%d,\"hit_rate\":%.2f,\"keys\":%d,\"evictions\":%d,\"node_status\":\"up\"}",
                    opsPerSec, hitRate, cache.size(), evictions.get()
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            cache.clear();
        } finally {
            lock.unlock();
        }
    }
}

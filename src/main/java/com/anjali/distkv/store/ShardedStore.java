package com.anjali.distkv.store;

import java.util.OptionalLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A partitioned implementation of {@link Store} dividing the keyspace into 16 independent shards.
 *
 * <p>Concurrency Architecture:
 * <ul>
 *   <li>Each shard owns its independent {@link LRUCache} and dedicated {@link ReentrantLock}.</li>
 *   <li>Key routing uses {@code Math.floorMod(key.hashCode(), 16)}.</li>
 *   <li>Why reads require the exclusive shard lock: an LRU cache's {@link LRUCache#get(Object)}
 *       mutates internal doubly-linked list pointers to promote accessed keys to the head (MRU).
 *       Striping into 16 shards reduces contention by ~16x compared to a single global lock.</li>
 *   <li>Integrates {@link TtlSweeper} to actively purge expired entries in the background.</li>
 * </ul>
 */
public final class ShardedStore implements Store, AutoCloseable {

    public static final int DEFAULT_NUM_SHARDS = 16;

    /**
     * Internal partition containing a dedicated LRUCache and ReentrantLock.
     */
    private static final class Shard {
        final LRUCache<String, SimpleStore.StoredItem> cache;
        final ReentrantLock lock = new ReentrantLock();

        Shard(int capacity) {
            this.cache = new LRUCache<>(Math.max(1, capacity));
        }
    }

    private final int numShards;
    private final Shard[] shards;
    private final TtlSweeper ttlSweeper;

    // High-concurrency metrics counters using LongAdder
    private final LongAdder totalOps = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final long startTimeMillis = System.currentTimeMillis();

    /**
     * Constructs a ShardedStore with 16 shards and the given total capacity.
     *
     * @param totalCapacity total keys across all shards combined
     */
    public ShardedStore(int totalCapacity) {
        this(totalCapacity, DEFAULT_NUM_SHARDS);
    }

    /**
     * Constructs a ShardedStore with a custom number of shards.
     */
    public ShardedStore(int totalCapacity, int numShards) {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be > 0");
        }
        this.numShards = numShards;
        this.shards = new Shard[numShards];
        int perShardCapacity = Math.max(1, totalCapacity / numShards);

        for (int i = 0; i < numShards; i++) {
            shards[i] = new Shard(perShardCapacity);
        }

        // Active background TTL sweeper checking every 100ms
        this.ttlSweeper = new TtlSweeper(this::del, 100);
    }

    private Shard getShard(String key) {
        int index = Math.floorMod(key.hashCode(), numShards);
        return shards[index];
    }

    @Override
    public void set(String key, String value, OptionalLong ttlSeconds) {
        Shard shard = getShard(key);
        shard.lock.lock();
        try {
            totalOps.increment();
            long expireAt = ttlSeconds.isPresent()
                    ? System.currentTimeMillis() + (ttlSeconds.getAsLong() * 1000L)
                    : SimpleStore.StoredItem.NO_EXPIRY;

            int sizeBefore = shard.cache.size();
            shard.cache.put(key, new SimpleStore.StoredItem(value, expireAt));

            if (sizeBefore == shard.cache.capacity() && !shard.cache.containsKey(key)) {
                evictions.increment();
            }

            if (ttlSeconds.isPresent()) {
                ttlSweeper.schedule(key, expireAt);
            }
        } finally {
            shard.lock.unlock();
        }
    }

    @Override
    public String get(String key) {
        Shard shard = getShard(key);
        shard.lock.lock();
        try {
            totalOps.increment();
            SimpleStore.StoredItem item = shard.cache.get(key);
            if (item == null) {
                misses.increment();
                return null;
            }

            if (item.isExpired(System.currentTimeMillis())) {
                shard.cache.remove(key);
                misses.increment();
                return null;
            }

            hits.increment();
            return item.value();
        } finally {
            shard.lock.unlock();
        }
    }

    @Override
    public boolean del(String key) {
        Shard shard = getShard(key);
        shard.lock.lock();
        try {
            totalOps.increment();
            return shard.cache.remove(key) != null;
        } finally {
            shard.lock.unlock();
        }
    }

    @Override
    public boolean exists(String key) {
        Shard shard = getShard(key);
        shard.lock.lock();
        try {
            totalOps.increment();
            SimpleStore.StoredItem item = shard.cache.get(key);
            if (item == null) {
                return false;
            }
            if (item.isExpired(System.currentTimeMillis())) {
                shard.cache.remove(key);
                return false;
            }
            return true;
        } finally {
            shard.lock.unlock();
        }
    }

    @Override
    public long ttl(String key) {
        Shard shard = getShard(key);
        shard.lock.lock();
        try {
            totalOps.increment();
            SimpleStore.StoredItem item = shard.cache.get(key);
            if (item == null) {
                return -2;
            }

            long now = System.currentTimeMillis();
            if (item.isExpired(now)) {
                shard.cache.remove(key);
                return -2;
            }

            if (item.expireAtMillis() == SimpleStore.StoredItem.NO_EXPIRY) {
                return -1;
            }

            long remaining = (item.expireAtMillis() - now) / 1000L;
            return Math.max(0, remaining);
        } finally {
            shard.lock.unlock();
        }
    }

    @Override
    public void dump(java.util.function.BiConsumer<String, SimpleStore.StoredItem> consumer) {
        long now = System.currentTimeMillis();
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                shard.cache.forEach((k, v) -> {
                    if (!v.isExpired(now)) {
                        consumer.accept(k, v);
                    }
                });
            } finally {
                shard.lock.unlock();
            }
        }
    }

    @Override
    public String statsJson() {
        int totalKeys = 0;
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                totalKeys += shard.cache.size();
            } finally {
                shard.lock.unlock();
            }
        }

        long total = totalOps.sum();
        long h = hits.sum();
        long m = misses.sum();
        long elapsedSeconds = Math.max(1, (System.currentTimeMillis() - startTimeMillis) / 1000L);
        long opsPerSec = total / elapsedSeconds;
        double hitRate = (h + m == 0) ? 0.0 : ((double) h / (h + m));

        return String.format(
                "{\"ops_per_sec\":%d,\"hit_rate\":%.2f,\"keys\":%d,\"evictions\":%d,\"shards\":%d,\"node_status\":\"up\"}",
                opsPerSec, hitRate, totalKeys, evictions.sum(), numShards
        );
    }

    @Override
    public void clear() {
        for (Shard shard : shards) {
            shard.lock.lock();
            try {
                shard.cache.clear();
            } finally {
                shard.lock.unlock();
            }
        }
    }

    @Override
    public void close() {
        ttlSweeper.close();
    }
}

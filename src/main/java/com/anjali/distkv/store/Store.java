package com.anjali.distkv.store;

import java.util.OptionalLong;
import java.util.function.BiConsumer;

/**
 * Common contract for DistKV storage engines (both single-lock and sharded).
 */
public interface Store {

    /**
     * Associates the specified value with the specified key, with an optional TTL in seconds.
     *
     * @param key        non-null key
     * @param value      non-null value
     * @param ttlSeconds optional expiry duration in seconds
     */
    void set(String key, String value, OptionalLong ttlSeconds);

    /**
     * Internal/Direct setter with exact absolute expiration timestamp in epoch millis.
     */
    default void setWithExactExpiry(String key, String value, long expireAtMillis) {
        if (expireAtMillis == SimpleStore.StoredItem.NO_EXPIRY) {
            set(key, value, OptionalLong.empty());
        } else {
            long remainingSeconds = Math.max(0, (expireAtMillis - System.currentTimeMillis()) / 1000L);
            set(key, value, OptionalLong.of(remainingSeconds));
        }
    }

    /**
     * Retrieves the value associated with the specified key.
     * If the entry has expired, it is lazily removed and {@code null} is returned.
     *
     * @param key non-null key
     * @return the value, or {@code null} if absent or expired
     */
    String get(String key);

    /**
     * Removes the mapping for a key.
     *
     * @param key non-null key
     * @return true if the key existed and was removed, false otherwise
     */
    boolean del(String key);

    /**
     * Returns true if the key exists and has not expired.
     *
     * @param key non-null key
     * @return true if key exists, false otherwise
     */
    boolean exists(String key);

    /**
     * Returns the remaining time to live of a key that has a timeout.
     *
     * @param key non-null key
     * @return positive seconds remaining, -1 if key exists without TTL, or -2 if key does not exist
     */
    long ttl(String key);

    /**
     * Iterates over all non-expired key-value entries in the store (for snapshotting).
     *
     * @param consumer consumer receiving (key, storedItem)
     */
    void dump(BiConsumer<String, SimpleStore.StoredItem> consumer);

    /**
     * Returns a JSON-formatted snapshot of runtime performance statistics.
     *
     * @return JSON string representing store statistics
     */
    String statsJson();

    /**
     * Clears all stored data and counters.
     */
    void clear();
}

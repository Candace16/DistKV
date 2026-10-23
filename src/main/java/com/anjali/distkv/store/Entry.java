package com.anjali.distkv.store;

import java.util.Objects;

/**
 * Immutable holder for a key-value pair stored in the cache.
 *
 * @param <K> type of the key (must be immutable and correctly implement {@link Object#hashCode()} and {@link Object#equals()})
 * @param <V> type of the value
 */
public final class Entry<K, V> {
    private final K key;
    private final V value;

    /**
     * Constructs a cache entry.
     *
     * @param key   non-null key
     * @param value value associated with the key
     */
    public Entry(K key, V value) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entry<?, ?> entry = (Entry<?, ?>) o;
        return Objects.equals(key, entry.key) && Objects.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "Entry{" + "key=" + key + ", value=" + value + '}';
    }
}

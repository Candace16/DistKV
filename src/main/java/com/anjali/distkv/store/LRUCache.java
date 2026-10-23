package com.anjali.distkv.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A fixed-capacity, thread-unsafe Least Recently Used (LRU) cache.
 *
 * <p>Data structure design:
 * <ul>
 *   <li>{@link HashMap} maps keys to doubly-linked list nodes for O(1) random lookup.</li>
 *   <li>Custom doubly-linked list with sentinel {@code head} and {@code tail} nodes maintains
 *       access order. The most-recently used (MRU) node is directly after {@code head},
 *       while the least-recently used (LRU) node is directly before {@code tail}.</li>
 * </ul>
 *
 * <p>Time Complexity:
 * <ul>
 *   <li>{@link #get(Object)}: O(1)</li>
 *   <li>{@link #put(Object, Object)}: O(1) (including eviction)</li>
 *   <li>{@link #remove(Object)}: O(1)</li>
 *   <li>{@link #containsKey(Object)}: O(1)</li>
 *   <li>{@link #size()}: O(1)</li>
 *   <li>{@link #capacity()}: O(1)</li>
 *   <li>{@link #clear()}: O(1)</li>
 * </ul>
 * Space Complexity: O(capacity) auxiliary memory for map and node pointers.
 *
 * @param <K> key type (must be immutable and implement hashCode/equals)
 * @param <V> value type
 */
public final class LRUCache<K, V> {

    /**
     * Node in the custom doubly-linked list.
     */
    private static final class Node<K, V> {
        final K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final int capacity;
    private final Map<K, Node<K, V>> map;
    private final Node<K, V> head;
    private final Node<K, V> tail;

    /**
     * Constructs an LRUCache with a fixed maximum capacity.
     *
     * @param capacity maximum number of items the cache can hold (must be &gt; 0)
     * @throws IllegalArgumentException if capacity &lt;= 0
     */
    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive, got: " + capacity);
        }
        this.capacity = capacity;
        // Allocate HashMap capacity with load factor 0.75 to minimize rehash
        this.map = new HashMap<>((int) Math.ceil(capacity / 0.75f) + 1);

        // Sentinel dummy nodes eliminate edge cases when inserting/removing at boundaries
        this.head = new Node<>(null, null);
        this.tail = new Node<>(null, null);
        this.head.next = this.tail;
        this.tail.prev = this.head;
    }

    /**
     * Retrieves the value associated with the specified key and promotes it to MRU position.
     *
     * @param key non-null key to look up
     * @return the value associated with key, or {@code null} if not found
     * @throws NullPointerException if key is null
     */
    public V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        Node<K, V> node = map.get(key);
        if (node == null) {
            return null;
        }
        // Promotion: remove from current position and insert at head (MRU)
        moveToHead(node);
        return node.value;
    }

    /**
     * Inserts or updates a key-value pair.
     * <p>If key already exists, updates value and moves it to MRU position.
     * If key is new and cache exceeds capacity, evicts the least recently used entry (LRU) at tail.
     *
     * @param key   non-null key
     * @param value value to associate with key
     * @return previous value associated with key, or {@code null} if it was not present
     * @throws NullPointerException if key is null
     */
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        Node<K, V> node = map.get(key);
        if (node != null) {
            V oldValue = node.value;
            node.value = value;
            moveToHead(node);
            return oldValue;
        }

        Node<K, V> newNode = new Node<>(key, value);
        map.put(key, newNode);
        addFirst(newNode);

        if (map.size() > capacity) {
            evict();
        }
        return null;
    }

    /**
     * Removes the mapping for a key from this cache if it is present.
     *
     * @param key non-null key to remove
     * @return the previous value associated with key, or {@code null} if absent
     * @throws NullPointerException if key is null
     */
    public V remove(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        Node<K, V> node = map.remove(key);
        if (node == null) {
            return null;
        }
        unlink(node);
        return node.value;
    }

    /**
     * Checks whether the cache contains a mapping for the specified key.
     * Note: Unlike {@link #get(Object)}, this does NOT promote the entry in LRU order.
     *
     * @param key non-null key
     * @return true if key exists in cache, false otherwise
     * @throws NullPointerException if key is null
     */
    public boolean containsKey(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return map.containsKey(key);
    }

    /**
     * Returns the number of entries currently stored in the cache.
     *
     * @return current size (&lt;= capacity)
     */
    public int size() {
        return map.size();
    }

    /**
     * Returns the maximum capacity of this cache.
     *
     * @return capacity
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Removes all entries from the cache and resets sentinels.
     */
    public void clear() {
        map.clear();
        head.next = tail;
        tail.prev = head;
    }

    /**
     * Iterates over all current key-value pairs in the cache.
     */
    public void forEach(java.util.function.BiConsumer<K, V> action) {
        Objects.requireNonNull(action);
        Node<K, V> curr = head.next;
        while (curr != tail) {
            action.accept(curr.key, curr.value);
            curr = curr.next;
        }
    }

    /**
     * Returns a snapshot list of keys in recency order: MRU (head) to LRU (tail).
     * Useful for testing and inspection.
     *
     * @return list of keys from most recently used to least recently used
     */
    public List<K> keysInLruOrder() {
        List<K> keys = new ArrayList<>(map.size());
        Node<K, V> curr = head.next;
        while (curr != tail) {
            keys.add(curr.key);
            curr = curr.next;
        }
        return Collections.unmodifiableList(keys);
    }

    // ==================== Doubly-Linked List Helpers ====================

    /**
     * Adds node right after the head sentinel (MRU position).
     */
    private void addFirst(Node<K, V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    /**
     * Unlinks node from its current position in the doubly-linked list.
     */
    private void unlink(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    /**
     * Promotes an existing node to the MRU position (just after head).
     */
    private void moveToHead(Node<K, V> node) {
        unlink(node);
        addFirst(node);
    }

    /**
     * Evicts the LRU item, which sits right before tail sentinel.
     */
    private void evict() {
        Node<K, V> lru = tail.prev;
        if (lru == head) {
            return; // Cache is empty, should not happen
        }
        unlink(lru);
        map.remove(lru.key);
    }
}

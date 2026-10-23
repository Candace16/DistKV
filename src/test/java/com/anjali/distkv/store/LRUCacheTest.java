package com.anjali.distkv.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LRUCache Unit Tests")
class LRUCacheTest {

    @Test
    @DisplayName("Construct with invalid capacity should throw IllegalArgumentException")
    void testInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<String, String>(0));
        assertThrows(IllegalArgumentException.class, () -> new LRUCache<String, String>(-5));
    }

    @Test
    @DisplayName("Basic put and get operations")
    void testBasicPutAndGet() {
        LRUCache<String, String> cache = new LRUCache<>(3);
        assertNull(cache.put("k1", "v1"));
        assertNull(cache.put("k2", "v2"));

        assertEquals(2, cache.size());
        assertEquals("v1", cache.get("k1"));
        assertEquals("v2", cache.get("k2"));
        assertNull(cache.get("k3"));
    }

    @Test
    @DisplayName("Updating existing key overwrites value and promotes to MRU")
    void testUpdateExistingKey() {
        LRUCache<String, String> cache = new LRUCache<>(3);
        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");

        // Order is k3 (MRU), k2, k1 (LRU)
        assertEquals(List.of("k3", "k2", "k1"), cache.keysInLruOrder());

        // Update k1
        String prev = cache.put("k1", "v1_updated");
        assertEquals("v1", prev);
        assertEquals("v1_updated", cache.get("k1"));

        // Now k1 should be MRU
        assertEquals(List.of("k1", "k3", "k2"), cache.keysInLruOrder());
        assertEquals(3, cache.size());
    }

    @Test
    @DisplayName("Eviction occurs at capacity, removing the least recently used element")
    void testEvictionAtCapacity() {
        LRUCache<String, Integer> cache = new LRUCache<>(3);
        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        // Order: c, b, a (a is LRU)

        // Adding 4th item 'd' should evict 'a'
        cache.put("d", 4);
        assertEquals(3, cache.size());
        assertNull(cache.get("a"), "Key 'a' should have been evicted");
        assertEquals(4, cache.get("d"));
        assertEquals(2, cache.get("b"));
        assertEquals(3, cache.get("c"));

        // Order after gets: c (MRU), b, d (LRU)
        assertEquals(List.of("c", "b", "d"), cache.keysInLruOrder());
    }

    @Test
    @DisplayName("Get promotes accessed key to MRU")
    void testGetPromotesKey() {
        LRUCache<String, String> cache = new LRUCache<>(3);
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        // MRU -> c, b, a <- LRU

        // Access 'a', moving it to MRU
        assertEquals("1", cache.get("a"));
        assertEquals(List.of("a", "c", "b"), cache.keysInLruOrder());

        // Insert 'd' -> 'b' (now LRU) must be evicted
        cache.put("d", "4");
        assertNull(cache.get("b"), "Key 'b' should have been evicted");
        assertEquals(List.of("d", "a", "c"), cache.keysInLruOrder());
        assertNotNull(cache.get("a"), "Key 'a' was recently accessed and must not be evicted");
        assertEquals(List.of("a", "d", "c"), cache.keysInLruOrder(), "get(a) re-promotes 'a' to head");
    }

    @Test
    @DisplayName("Remove unlinks node, updates size and returns old value")
    void testRemove() {
        LRUCache<String, String> cache = new LRUCache<>(3);
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");

        assertEquals("2", cache.remove("b"));
        assertEquals(2, cache.size());
        assertNull(cache.get("b"));
        assertNull(cache.remove("b"), "Subsequent remove should return null");
        assertEquals(List.of("c", "a"), cache.keysInLruOrder());

        // Remove head
        assertEquals("3", cache.remove("c"));
        assertEquals(List.of("a"), cache.keysInLruOrder());

        // Remove tail
        assertEquals("1", cache.remove("a"));
        assertEquals(0, cache.size());
        assertTrue(cache.keysInLruOrder().isEmpty());
    }

    @Test
    @DisplayName("Clear empties the cache and resets structure")
    void testClear() {
        LRUCache<String, String> cache = new LRUCache<>(3);
        cache.put("a", "1");
        cache.put("b", "2");
        cache.clear();

        assertEquals(0, cache.size());
        assertNull(cache.get("a"));
        assertTrue(cache.keysInLruOrder().isEmpty());

        // Verify it can still accept new entries cleanly
        cache.put("x", "10");
        assertEquals(1, cache.size());
        assertEquals("10", cache.get("x"));
    }

    @Test
    @DisplayName("Capacity of 1 edge case")
    void testCapacityOne() {
        LRUCache<String, String> cache = new LRUCache<>(1);
        cache.put("a", "1");
        assertEquals("1", cache.get("a"));

        cache.put("b", "2");
        assertNull(cache.get("a"));
        assertEquals("2", cache.get("b"));
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("Null key checks")
    void testNullKeyThrows() {
        LRUCache<String, String> cache = new LRUCache<>(2);
        assertThrows(NullPointerException.class, () -> cache.get(null));
        assertThrows(NullPointerException.class, () -> cache.put(null, "val"));
        assertThrows(NullPointerException.class, () -> cache.remove(null));
        assertThrows(NullPointerException.class, () -> cache.containsKey(null));
    }

    @Test
    @DisplayName("Equivalence test: compare custom LRUCache behavior with Java LinkedHashMap")
    void testEquivalenceWithLinkedHashMap() {
        final int cap = 4;
        LRUCache<Integer, String> customCache = new LRUCache<>(cap);

        // Standard Java LRU implementation pattern using LinkedHashMap
        LinkedHashMap<Integer, String> referenceMap = new LinkedHashMap<>(cap, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > cap;
            }
        };

        int[] operations = {1, 2, 3, 4, 2, 5, 1, 6, 3, 4};
        for (int op : operations) {
            String val = "v" + op;
            customCache.put(op, val);
            referenceMap.put(op, val);
        }

        // Trigger some gets
        assertEquals(referenceMap.get(2), customCache.get(2));
        assertEquals(referenceMap.get(99), customCache.get(99));

        assertEquals(referenceMap.size(), customCache.size());
        for (Integer key : new java.util.ArrayList<>(referenceMap.keySet())) {
            assertTrue(customCache.containsKey(key));
            assertEquals(referenceMap.get(key), customCache.get(key));
        }
    }
}

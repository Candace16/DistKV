package com.anjali.distkv.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("TTL Expiration Tests (Lazy and Active Sweeper)")
class TtlTest {

    @Test
    @DisplayName("Lazy expiration: expired entries are removed upon get/exists")
    void testLazyExpiration() throws InterruptedException {
        SimpleStore store = new SimpleStore(100);
        store.set("temp", "val", OptionalLong.of(1)); // 1 second TTL

        assertTrue(store.exists("temp"));
        assertEquals("val", store.get("temp"));

        // Wait for TTL to elapse
        Thread.sleep(1100);

        // Lazy expiration check
        assertNull(store.get("temp"));
        assertFalse(store.exists("temp"));
        assertEquals(-2, store.ttl("temp"));
    }

    @Test
    @DisplayName("TTL reporting: -1 for persistent key, -2 for absent, >0 for expiring")
    void testTtlReturnCodes() {
        SimpleStore store = new SimpleStore(100);
        store.set("persistent", "data", OptionalLong.empty());
        store.set("expiring", "data", OptionalLong.of(60));

        assertEquals(-1, store.ttl("persistent"));
        assertEquals(-2, store.ttl("non_existent"));

        long remaining = store.ttl("expiring");
        assertTrue(remaining > 0 && remaining <= 60);
    }

    @Test
    @DisplayName("Active TtlSweeper min-heap purges expired items in O(1) peek time")
    void testActiveTtlSweeper() {
        AtomicInteger purgedCount = new AtomicInteger(0);
        long now = 1_000_000L;

        try (TtlSweeper sweeper = new TtlSweeper(key -> purgedCount.incrementAndGet(), 5000)) {
            // Schedule 3 keys with different expiration times out of order
            sweeper.schedule("k3", now + 3000);
            sweeper.schedule("k1", now + 1000);
            sweeper.schedule("k2", now + 2000);

            assertEquals(3, sweeper.heapSize());

            // 1. Sweep at now + 500ms -> none should expire
            int swept = sweeper.sweep(now + 500);
            assertEquals(0, swept);
            assertEquals(0, purgedCount.get());
            assertEquals(3, sweeper.heapSize());

            // 2. Sweep at now + 1500ms -> k1 should expire
            swept = sweeper.sweep(now + 1500);
            assertEquals(1, swept);
            assertEquals(1, purgedCount.get());
            assertEquals(2, sweeper.heapSize());

            // 3. Sweep at now + 3500ms -> k2 and k3 should expire
            swept = sweeper.sweep(now + 3500);
            assertEquals(2, swept);
            assertEquals(3, purgedCount.get());
            assertEquals(0, sweeper.heapSize());
        }
    }

    @Test
    @DisplayName("ShardedStore actively purges expired keys in the background")
    void testShardedStoreBackgroundSweep() throws InterruptedException {
        try (ShardedStore store = new ShardedStore(100, 4)) {
            store.set("short_lived", "data", OptionalLong.of(1));
            assertTrue(store.exists("short_lived"));

            // Wait 1.3 seconds for background thread to sweep without any client reads
            Thread.sleep(1300);

            // Entry must already be purged by background sweeper
            assertEquals(-2, store.ttl("short_lived"));
            assertNull(store.get("short_lived"));
        }
    }
}

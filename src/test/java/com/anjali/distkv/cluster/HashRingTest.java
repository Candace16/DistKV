package com.anjali.distkv.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Consistent HashRing & Distributed Replication Tests")
class HashRingTest {

    @Test
    @DisplayName("Virtual nodes achieve balanced key distribution across 3 nodes")
    void testVirtualNodeUniformDistribution() {
        HashRing ring = new HashRing(100, List.of("node1", "node2", "node3"));
        int totalKeys = 10_000;
        Map<String, Integer> distribution = new HashMap<>();

        for (int i = 0; i < totalKeys; i++) {
            String key = "customer:account:" + i;
            String owner = ring.getPrimary(key);
            distribution.merge(owner, 1, Integer::sum);
        }

        assertEquals(3, distribution.size());
        for (Map.Entry<String, Integer> entry : distribution.entrySet()) {
            double ratio = (double) entry.getValue() / totalKeys;
            // Each node should hold approximately 33.3% of keys (between 25% and 42%)
            assertTrue(ratio >= 0.25 && ratio <= 0.42,
                    "Expected balanced ratio near 0.33, got " + ratio + " for " + entry.getKey());
        }
    }

    @Test
    @DisplayName("Consistent Hashing Invariant: Adding a node moves only ~1/N of keys")
    void testMinimalKeyMovementOnNodeAddition() {
        HashRing ring = new HashRing(100, List.of("node1", "node2", "node3"));
        int totalKeys = 10_000;

        // Record initial owners for 3 nodes (N=3)
        Map<String, String> initialAssignment = new HashMap<>();
        for (int i = 0; i < totalKeys; i++) {
            String key = "key:sample:" + i;
            initialAssignment.put(key, ring.getPrimary(key));
        }

        // Add 4th node (N=4). Theoretical moved fraction = 1/4 = 25%
        ring.addNode("node4");

        int keysMoved = 0;
        for (int i = 0; i < totalKeys; i++) {
            String key = "key:sample:" + i;
            String newOwner = ring.getPrimary(key);
            if (!newOwner.equals(initialAssignment.get(key))) {
                keysMoved++;
            }
        }

        double movedRatio = (double) keysMoved / totalKeys;
        // With traditional modulo hash, ~75% to 100% of keys move.
        // With consistent hashing, only about 1/4 (~20% - 32%) of keys move!
        assertTrue(movedRatio >= 0.18 && movedRatio <= 0.32,
                "Expected ~25% of keys to move on adding 4th node, but " + (movedRatio * 100) + "% moved");
    }

    @Test
    @DisplayName("Replication Factor 2 selects Primary and distinct Replica")
    void testPrimaryAndReplicaSelection() {
        HashRing ring = new HashRing(100, List.of("node1", "node2", "node3"));

        for (int i = 0; i < 50; i++) {
            String key = "test:entity:" + i;
            List<String> owners = ring.getPrimaryAndReplica(key);

            assertEquals(2, owners.size());
            assertNotEquals(owners.get(0), owners.get(1), "Primary and Replica must be distinct physical nodes");
        }
    }

    @Test
    @DisplayName("Replication Failover: when Primary is down, Replica can serve read")
    void testReplicationFailover() {
        HashRing ring = new HashRing(100, List.of("node1", "node2", "node3"));
        String key = "mission_critical_key";
        List<String> owners = ring.getPrimaryAndReplica(key);

        String primary = owners.get(0);
        String replica = owners.get(1);

        assertNotNull(primary);
        assertNotNull(replica);
        assertNotEquals(primary, replica);

        // Simulate failure of primary by removing it from active ring
        ring.removeNode(primary);

        // When primary is removed, replica inherits primary ownership on the ring
        String fallbackOwner = ring.getPrimary(key);
        assertEquals(replica, fallbackOwner, "Replica must take over key when Primary is down");
    }
}

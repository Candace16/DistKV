package com.anjali.distkv.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent Hashing Ring with Virtual Nodes.
 *
 * <p>Implementation Details:
 * <ul>
 *   <li>Uses {@link TreeMap} (Red-Black tree) to maintain virtual node tokens in sorted order.</li>
 *   <li>Default 100 virtual nodes per physical node for statistically uniform key distribution.</li>
 *   <li>Uses the first 8 bytes of MD5 as a stable, uniform 64-bit hash function (unlike String.hashCode()).</li>
 *   <li>Clockwise ring lookup uses {@link TreeMap#ceilingEntry(Object)}, wrapping to {@link TreeMap#firstEntry()}.</li>
 * </ul>
 */
public final class HashRing {

    public static final int DEFAULT_VNODES_PER_NODE = 100;

    private final int vnodesPerNode;
    private final TreeMap<Long, String> ring = new TreeMap<>();
    private final Set<String> physicalNodes = new HashSet<>();
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

    public HashRing() {
        this(DEFAULT_VNODES_PER_NODE, Collections.emptyList());
    }

    public HashRing(int vnodesPerNode, Collection<String> initialNodes) {
        if (vnodesPerNode <= 0) {
            throw new IllegalArgumentException("vnodesPerNode must be > 0");
        }
        this.vnodesPerNode = vnodesPerNode;
        for (String node : initialNodes) {
            addNode(node);
        }
    }

    /**
     * Adds a physical node to the ring, allocating {@code vnodesPerNode} virtual tokens.
     */
    public void addNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        rwl.writeLock().lock();
        try {
            if (physicalNodes.add(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    long token = hash(nodeId + "#vnode#" + i);
                    ring.put(token, nodeId);
                }
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * Removes a physical node and all its virtual tokens from the ring.
     */
    public void removeNode(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        rwl.writeLock().lock();
        try {
            if (physicalNodes.remove(nodeId)) {
                for (int i = 0; i < vnodesPerNode; i++) {
                    long token = hash(nodeId + "#vnode#" + i);
                    ring.remove(token);
                }
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    /**
     * Finds the primary node responsible for the given key.
     *
     * @param key non-null key
     * @return physical node ID, or {@code null} if ring is empty
     */
    public String getPrimary(String key) {
        List<String> nodes = getPreferenceList(key, 1);
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    /**
     * Returns the primary and replica nodes for the given key (Replication Factor 2).
     *
     * @param key non-null key
     * @return list containing [primaryNode, replicaNode] (or single element if only 1 node in cluster)
     */
    public List<String> getPrimaryAndReplica(String key) {
        return getPreferenceList(key, 2);
    }

    /**
     * Computes the preference list: the first {@code count} distinct physical nodes
     * found by walking clockwise along the ring.
     */
    public List<String> getPreferenceList(String key, int count) {
        Objects.requireNonNull(key, "key cannot be null");
        rwl.readLock().lock();
        try {
            if (ring.isEmpty() || count <= 0) {
                return Collections.emptyList();
            }

            long keyHash = hash(key);
            List<String> result = new ArrayList<>(Math.min(count, physicalNodes.size()));
            Set<String> visited = new HashSet<>();

            // Find starting point (ceilingEntry or wraparound to firstEntry)
            Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
            if (entry == null) {
                entry = ring.firstEntry();
            }

            Long currentToken = entry.getKey();
            while (result.size() < count && visited.size() < physicalNodes.size()) {
                String node = ring.get(currentToken);
                if (visited.add(node)) {
                    result.add(node);
                }

                // Move to next token clockwise
                Map.Entry<Long, String> higher = ring.higherEntry(currentToken);
                if (higher != null) {
                    currentToken = higher.getKey();
                } else {
                    currentToken = ring.firstKey(); // Wrap around ring
                }
            }

            return result;
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * Computes a 64-bit stable hash from the first 8 bytes of an MD5 digest.
     */
    public static long hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buffer = ByteBuffer.wrap(digest);
            return buffer.getLong(); // First 8 bytes as 64-bit integer
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 digest algorithm not available", e);
        }
    }

    public Set<String> getPhysicalNodes() {
        rwl.readLock().lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(physicalNodes));
        } finally {
            rwl.readLock().unlock();
        }
    }

    public int size() {
        rwl.readLock().lock();
        try {
            return physicalNodes.size();
        } finally {
            rwl.readLock().unlock();
        }
    }

    public record VNode(long token, String nodeId, double angleNormalized) {}

    /**
     * Returns a snapshot of all virtual node tokens and their owners on the ring.
     */
    public List<VNode> getAllVNodes() {
        rwl.readLock().lock();
        try {
            List<VNode> list = new ArrayList<>(ring.size());
            for (Map.Entry<Long, String> entry : ring.entrySet()) {
                long token = entry.getKey();
                list.add(new VNode(token, entry.getValue(), hashToNormalized(token)));
            }
            return list;
        } finally {
            rwl.readLock().unlock();
        }
    }

    /**
     * Normalizes any 64-bit hash token to a floating-point fraction in [0.0, 1.0).
     *
     * <p><b>Mathematical Explanation:</b>
     * <ol>
     *   <li>Java's {@code long} is a signed two's-complement 64-bit integer spanning
     *       [-2^63, 2^63 - 1].</li>
     *   <li>The unsigned right shift {@code hash >>> 11} discards the bottom 11 bits and
     *       clears the sign bit, leaving the top 53 most-significant bits.</li>
     *   <li>The IEEE 754 standard for 64-bit {@code double} contains exactly 53 bits of
     *       precision in its significand (mantissa).</li>
     *   <li>Dividing by {@code (1L << 53)} (9,007,199,254,740,992.0) produces an exact,
     *       uniformly distributed value in the interval [0.0, 1.0) with zero rounding distortion.</li>
     *   <li>Multiplying this normalized fraction by {@code 360.0} gives the polar angle in degrees,
     *       or by {@code 2 * Math.PI} gives the radians for Cartesian circle coordinates:
     *       {@code x = cx + r * cos(theta)}, {@code y = cy + r * sin(theta)}.</li>
     * </ol>
     */
    public static double hashToNormalized(long hash) {
        return ((double) (hash >>> 11)) / (double) (1L << 53);
    }
}

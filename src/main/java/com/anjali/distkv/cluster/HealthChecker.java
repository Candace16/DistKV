package com.anjali.distkv.cluster;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background health checker tracking cluster peer status.
 *
 * <p>Failure Detection Algorithm:
 * <ul>
 *   <li>Sends a TCP PING to each peer node every 1 second.</li>
 *   <li>3-Strike Failure: Marks a peer DOWN after 3 consecutive missed replies.</li>
 *   <li>Recovery: Marks a peer UP immediately upon receiving a successful PONG.</li>
 *   <li>Structured logging: outputs one-line log events on status transitions.</li>
 * </ul>
 */
public final class HealthChecker implements Closeable {

    public enum NodeStatus { UP, DOWN }

    private static final class PeerTracker {
        final String host;
        final int port;
        volatile NodeStatus status = NodeStatus.UP;
        int consecutiveFailures = 0;

        PeerTracker(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private final String localNodeId;
    private final ReplicationClient client;
    private final Map<String, PeerTracker> peers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public HealthChecker(String localNodeId, ReplicationClient client) {
        this(localNodeId, client, 1000); // 1 second interval
    }

    public HealthChecker(String localNodeId, ReplicationClient client, long intervalMs) {
        this.localNodeId = localNodeId;
        this.client = client;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "distkv-health-checker");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleWithFixedDelay(this::checkAllPeers, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Registers a peer node to monitor.
     *
     * @param peerId identifier (e.g. "node2")
     * @param host   peer hostname/IP
     * @param port   peer TCP port
     */
    public void registerPeer(String peerId, String host, int port) {
        peers.put(peerId, new PeerTracker(host, port));
    }

    /**
     * Executes a single health check pass across all registered peers.
     */
    public void checkAllPeers() {
        for (Map.Entry<String, PeerTracker> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            PeerTracker tracker = entry.getValue();

            boolean alive = client.ping(tracker.host, tracker.port);
            if (alive) {
                if (tracker.status == NodeStatus.DOWN) {
                    tracker.status = NodeStatus.UP;
                    System.out.printf("[%s] %s marked UP after successful ping%n", localNodeId, peerId);
                }
                tracker.consecutiveFailures = 0;
            } else {
                tracker.consecutiveFailures++;
                if (tracker.consecutiveFailures >= 3 && tracker.status == NodeStatus.UP) {
                    tracker.status = NodeStatus.DOWN;
                    System.out.printf("[%s] %s marked DOWN after 3 missed pings%n", localNodeId, peerId);
                }
            }
        }
    }

    public boolean isNodeUp(String peerId) {
        if (peerId.equals(localNodeId)) {
            return true;
        }
        PeerTracker tracker = peers.get(peerId);
        return tracker != null && tracker.status == NodeStatus.UP;
    }

    /**
     * Returns a JSON-compatible map of all node statuses.
     */
    public Map<String, String> getStatusesSnapshot() {
        Map<String, String> map = new ConcurrentHashMap<>();
        map.put(localNodeId, "up");
        for (Map.Entry<String, PeerTracker> e : peers.entrySet()) {
            map.put(e.getKey(), e.getValue().status.name().toLowerCase());
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}

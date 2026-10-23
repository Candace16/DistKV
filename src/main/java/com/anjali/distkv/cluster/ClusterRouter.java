package com.anjali.distkv.cluster;

import com.anjali.distkv.protocol.Command;
import com.anjali.distkv.protocol.Response;
import com.anjali.distkv.storage.WriteAheadLog;
import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Routes requests across the distributed cluster using consistent hashing and replication.
 *
 * <p>Replication & Availability Strategy (RF=2):
 * <ul>
 *   <li><b>Writes:</b> Coordinated to both the Primary and Replica nodes.</li>
 *   <li><b>Reads:</b> Attempted on the Primary first; if the Primary is marked DOWN by
 *       {@link HealthChecker}, automatically falls back to the Replica.</li>
 * </ul>
 */
public final class ClusterRouter {

    public record NodeAddress(String host, int port) {}

    private final String localNodeId;
    private final Store localStore;
    private final WriteAheadLog wal;
    private final HashRing hashRing;
    private final HealthChecker healthChecker;
    private final ReplicationClient replicationClient;
    private final Map<String, NodeAddress> peerAddresses = new ConcurrentHashMap<>();

    public ClusterRouter(
            String localNodeId,
            Store localStore,
            WriteAheadLog wal,
            HashRing hashRing,
            HealthChecker healthChecker,
            ReplicationClient replicationClient
    ) {
        this.localNodeId = localNodeId;
        this.localStore = localStore;
        this.wal = wal;
        this.hashRing = hashRing;
        this.healthChecker = healthChecker;
        this.replicationClient = replicationClient;
    }

    public void registerNodeAddress(String nodeId, String host, int port) {
        peerAddresses.put(nodeId, new NodeAddress(host, port));
        hashRing.addNode(nodeId);
        if (!nodeId.equals(localNodeId) && healthChecker != null) {
            healthChecker.registerPeer(nodeId, host, port);
        }
    }

    /**
     * Executes a command with distributed routing and replication.
     */
    public String execute(Command command) {
        if (command instanceof Command.Ping) {
            return Response.PONG;
        } else if (command instanceof Command.Set set) {
            return handleSet(set.key(), set.value(), set.ttlSeconds());
        } else if (command instanceof Command.Get get) {
            return handleGet(get.key());
        } else if (command instanceof Command.Del del) {
            return handleDel(del.key());
        } else if (command instanceof Command.Exists exists) {
            return handleExists(exists.key());
        } else if (command instanceof Command.Ttl ttl) {
            return handleTtl(ttl.key());
        } else if (command instanceof Command.Stats) {
            return handleStats();
        } else if (command instanceof Command.Invalid invalid) {
            return Response.err(invalid.reason());
        } else {
            return Response.err("Unknown command");
        }
    }

    private String handleSet(String key, String value, OptionalLong ttlSeconds) {
        List<String> owners = hashRing.getPrimaryAndReplica(key);
        if (owners.isEmpty()) {
            return Response.err("No active cluster nodes");
        }

        String primary = owners.get(0);
        String replica = owners.size() > 1 ? owners.get(1) : null;

        boolean isLocalOwner = localNodeId.equals(primary) || localNodeId.equals(replica);

        // 1. Write locally if this node is an owner
        if (isLocalOwner) {
            writeLocal(key, value, ttlSeconds);
        }

        // 2. Replicate to peer owner if distinct
        String peerOwner = localNodeId.equals(primary) ? replica : (localNodeId.equals(replica) ? primary : null);
        if (peerOwner != null && healthChecker.isNodeUp(peerOwner)) {
            NodeAddress addr = peerAddresses.get(peerOwner);
            if (addr != null) {
                replicationClient.replicateSet(addr.host(), addr.port(), key, value, ttlSeconds);
            }
        } else if (!isLocalOwner) {
            // Forward write to primary (or replica if primary is down)
            String target = healthChecker.isNodeUp(primary) ? primary : replica;
            if (target != null && healthChecker.isNodeUp(target)) {
                NodeAddress addr = peerAddresses.get(target);
                if (addr != null) {
                    boolean ok = replicationClient.replicateSet(addr.host(), addr.port(), key, value, ttlSeconds);
                    return ok ? Response.OK : Response.err("Failed to forward SET to " + target);
                }
            }
        }

        return Response.OK;
    }

    private String handleGet(String key) {
        List<String> owners = hashRing.getPrimaryAndReplica(key);
        if (owners.isEmpty()) {
            return Response.NOT_FOUND;
        }

        String primary = owners.get(0);
        String replica = owners.size() > 1 ? owners.get(1) : null;

        // If local node is one of the owners, read locally
        if (localNodeId.equals(primary) || localNodeId.equals(replica)) {
            String val = localStore.get(key);
            if (val != null) {
                return Response.value(val);
            }
        }

        // Otherwise try primary first
        if (primary != null && !primary.equals(localNodeId) && healthChecker.isNodeUp(primary)) {
            NodeAddress addr = peerAddresses.get(primary);
            if (addr != null) {
                String val = replicationClient.fallbackGet(addr.host(), addr.port(), key);
                if (val != null) {
                    return Response.value(val);
                }
            }
        }

        // Fallback to replica if primary is DOWN or didn't respond
        if (replica != null && !replica.equals(localNodeId) && healthChecker.isNodeUp(replica)) {
            NodeAddress addr = peerAddresses.get(replica);
            if (addr != null) {
                String val = replicationClient.fallbackGet(addr.host(), addr.port(), key);
                if (val != null) {
                    return Response.value(val);
                }
            }
        }

        return Response.NOT_FOUND;
    }

    private String handleDel(String key) {
        List<String> owners = hashRing.getPrimaryAndReplica(key);
        boolean isLocalOwner = owners.contains(localNodeId);

        boolean deletedLocally = false;
        if (isLocalOwner) {
            deletedLocally = deleteLocal(key);
        }

        for (String owner : owners) {
            if (!owner.equals(localNodeId) && healthChecker.isNodeUp(owner)) {
                NodeAddress addr = peerAddresses.get(owner);
                if (addr != null) {
                    replicationClient.replicateDel(addr.host(), addr.port(), key);
                }
            }
        }

        return (deletedLocally || isLocalOwner) ? Response.OK : Response.NOT_FOUND;
    }

    private String handleExists(String key) {
        String getResp = handleGet(key);
        return getResp.startsWith("VALUE ") ? Response.EXISTS_TRUE : Response.EXISTS_FALSE;
    }

    private String handleTtl(String key) {
        List<String> owners = hashRing.getPrimaryAndReplica(key);
        if (owners.contains(localNodeId)) {
            return Response.ttl(localStore.ttl(key));
        }

        String primary = owners.get(0);
        String target = healthChecker.isNodeUp(primary) ? primary : (owners.size() > 1 ? owners.get(1) : null);
        if (target != null && !target.equals(localNodeId)) {
            NodeAddress addr = peerAddresses.get(target);
            if (addr != null) {
                String resp = replicationClient.sendCommand(addr.host(), addr.port(), "TTL " + key);
                if (resp != null) {
                    return resp;
                }
            }
        }
        return Response.ttl(-2);
    }

    private String handleStats() {
        String baseJson = localStore.statsJson();
        Map<String, String> statuses = (healthChecker != null)
                ? healthChecker.getStatusesSnapshot()
                : Map.of(localNodeId, "up");

        String nodeStatusJson = statuses.entrySet().stream()
                .map(e -> "\"" + e.getKey() + "\":\"" + e.getValue() + "\"")
                .collect(Collectors.joining(",", "{", "}"));

        // Inject "nodes" map into base JSON
        if (baseJson.endsWith("}")) {
            return baseJson.substring(0, baseJson.length() - 1) + ",\"nodes\":" + nodeStatusJson + "}";
        }
        return baseJson;
    }

    private void writeLocal(String key, String value, OptionalLong ttlSeconds) {
        if (wal != null) {
            try {
                long expireAt = ttlSeconds.isPresent()
                        ? System.currentTimeMillis() + (ttlSeconds.getAsLong() * 1000L)
                        : SimpleStore.StoredItem.NO_EXPIRY;
                wal.appendSet(key, value, expireAt);
            } catch (IOException ignored) {}
        }
        localStore.set(key, value, ttlSeconds);
    }

    private boolean deleteLocal(String key) {
        if (wal != null) {
            try {
                wal.appendDel(key);
            } catch (IOException ignored) {}
        }
        return localStore.del(key);
    }

    public HashRing getHashRing() {
        return hashRing;
    }

    public String getLocalNodeId() {
        return localNodeId;
    }

    public Store getLocalStore() {
        return localStore;
    }

    public HealthChecker getHealthChecker() {
        return healthChecker;
    }

    public Map<String, NodeAddress> getPeerAddresses() {
        return Collections.unmodifiableMap(peerAddresses);
    }

    public ReplicationClient getReplicationClient() {
        return replicationClient;
    }
}

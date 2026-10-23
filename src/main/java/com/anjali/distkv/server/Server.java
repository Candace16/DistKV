package com.anjali.distkv.server;

import com.anjali.distkv.protocol.Response;
import com.anjali.distkv.store.ShardedStore;
import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Concurrent TCP server for DistKV.
 *
 * <p>Uses a bounded {@link ThreadPoolExecutor} created by {@link ThreadPoolFactory}
 * to handle multiple simultaneous client connections without unconstrained thread creation.
 * Supports switching between single-lock ({@link SimpleStore}) and sharded ({@link ShardedStore}).
 */
public final class Server implements AutoCloseable {

    private static final int DEFAULT_CAPACITY = 100_000;
    private static final int DEFAULT_CORE_THREADS = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
    private static final int DEFAULT_MAX_THREADS = 64;
    private static final int DEFAULT_QUEUE_CAPACITY = 1000;

    private final int requestedPort;
    private final Store store;
    private final ThreadPoolExecutor workerPool;
    private final com.anjali.distkv.storage.WriteAheadLog wal;
    private final com.anjali.distkv.cluster.ClusterRouter router;

    private volatile boolean running = false;
    private ServerSocket serverSocket;
    private Thread acceptorThread;

    /**
     * Constructs a Server with the default {@link ShardedStore}.
     *
     * @param port TCP port to bind (0 for ephemeral port)
     */
    public Server(int port) {
        this(port, new ShardedStore(DEFAULT_CAPACITY), null, null);
    }

    /**
     * Constructs a Server with a specific storage engine.
     *
     * @param port  TCP port to bind
     * @param store underlying storage engine (e.g. SimpleStore or ShardedStore)
     */
    public Server(int port, Store store) {
        this(port, store, null, null);
    }

    /**
     * Constructs a Server with a storage engine and optional WriteAheadLog.
     */
    public Server(int port, Store store, com.anjali.distkv.storage.WriteAheadLog wal) {
        this(port, store, wal, null);
    }

    public Server(int port, Store store, int coreThreads, int maxThreads, int queueCapacity) {
        this(port, store, null, null, coreThreads, maxThreads, queueCapacity);
    }

    /**
     * Constructs a Server with storage, WAL, and optional ClusterRouter.
     */
    public Server(int port, Store store, com.anjali.distkv.storage.WriteAheadLog wal, com.anjali.distkv.cluster.ClusterRouter router) {
        this(port, store, wal, router, DEFAULT_CORE_THREADS, DEFAULT_MAX_THREADS, DEFAULT_QUEUE_CAPACITY);
    }

    /**
     * Constructs a Server with custom thread pool parameters.
     */
    public Server(int port, Store store, com.anjali.distkv.storage.WriteAheadLog wal, com.anjali.distkv.cluster.ClusterRouter router, int coreThreads, int maxThreads, int queueCapacity) {
        this.requestedPort = port;
        this.store = store;
        this.wal = wal;
        this.router = router;
        this.workerPool = ThreadPoolFactory.createBoundedPool(
                coreThreads,
                maxThreads,
                queueCapacity,
                "distkv-worker"
        );
    }

    /**
     * Starts listening for client connections on a dedicated acceptor thread.
     */
    public synchronized void start() throws IOException {
        if (running) {
            return;
        }
        serverSocket = new ServerSocket(requestedPort);
        running = true;

        acceptorThread = new Thread(this::acceptLoop, "distkv-acceptor");
        acceptorThread.setDaemon(true);
        acceptorThread.start();
    }

    private void acceptLoop() {
        while (running && !serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                try {
                    workerPool.execute(new ConnectionHandler(clientSocket, store, wal, router));
                } catch (RejectedExecutionException rex) {
                    // Pool queue is completely saturated; notify client and close immediately
                    rejectClient(clientSocket, "Server thread pool saturated, connection rejected");
                }
            } catch (SocketException e) {
                // ServerSocket closed during normal server shutdown
                break;
            } catch (IOException e) {
                if (running) {
                    System.err.println("[DistKV] Accept error: " + e.getMessage());
                }
            }
        }
    }

    private void rejectClient(Socket socket, String reason) {
        try (socket) {
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            out.write(Response.err(reason) + "\r\n");
            out.flush();
        } catch (IOException ignored) {}
    }

    public int getPort() {
        if (serverSocket == null || !serverSocket.isBound()) {
            return requestedPort;
        }
        return serverSocket.getLocalPort();
    }

    public Store getStore() {
        return store;
    }

    public ThreadPoolExecutor getWorkerPool() {
        return workerPool;
    }

    @Override
    public synchronized void close() {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
        }
        if (acceptorThread != null) {
            acceptorThread.interrupt();
        }

        // Graceful shutdown of worker threads
        ThreadPoolFactory.gracefulShutdown(workerPool, 5000);

        // Close WAL if active
        if (wal != null) {
            try {
                wal.close();
            } catch (IOException ignored) {}
        }

        // Close store if it manages background threads (e.g., TtlSweeper)
        if (store instanceof AutoCloseable ac) {
            try {
                ac.close();
            } catch (Exception ignored) {}
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 7000;
        int capacity = DEFAULT_CAPACITY;
        boolean useSharding = true;
        String dataDirPath = null;
        String nodeId = "node1";
        String peersArg = null;
        com.anjali.distkv.storage.WriteAheadLog.FsyncPolicy fsyncPolicy = com.anjali.distkv.storage.WriteAheadLog.FsyncPolicy.EVERY_SECOND;

        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            } else if ("--id".equals(args[i]) && i + 1 < args.length) {
                nodeId = args[++i];
            } else if ("--peers".equals(args[i]) && i + 1 < args.length) {
                peersArg = args[++i];
            } else if ("--capacity".equals(args[i]) && i + 1 < args.length) {
                capacity = Integer.parseInt(args[++i]);
            } else if ("--data-dir".equals(args[i]) && i + 1 < args.length) {
                dataDirPath = args[++i];
            } else if ("--fsync".equals(args[i]) && i + 1 < args.length) {
                fsyncPolicy = com.anjali.distkv.storage.WriteAheadLog.FsyncPolicy.fromString(args[++i]);
            } else if ("--sharded".equals(args[i])) {
                useSharding = true;
            } else if ("--single-lock".equals(args[i])) {
                useSharding = false;
            }
        }

        Store store = useSharding ? new ShardedStore(capacity) : new SimpleStore(capacity);
        com.anjali.distkv.storage.WriteAheadLog wal = null;
        com.anjali.distkv.storage.SnapshotManager snapshotManager = null;

        if (dataDirPath != null) {
            java.io.File dataDir = new java.io.File(dataDirPath);
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }

            // 1. Recover committed snapshot & replay WAL
            com.anjali.distkv.storage.RecoveryManager.RecoverySummary summary =
                    com.anjali.distkv.storage.RecoveryManager.recover(dataDir, store);
            System.out.printf("[%s] Recovery complete: %d snapshot keys loaded, %d WAL mutations replayed%n",
                    nodeId, summary.snapshotRecords(), summary.walRecordsReplayed());

            // 2. Initialize active WAL
            wal = new com.anjali.distkv.storage.WriteAheadLog(new java.io.File(dataDir, "wal.log"), fsyncPolicy);

            // 3. Start background snapshotting every 60s
            snapshotManager = new com.anjali.distkv.storage.SnapshotManager(dataDir, 60);
            snapshotManager.startPeriodic(store, wal, 60);
        }

        // Initialize cluster router and health checker if peers or node ID are configured
        com.anjali.distkv.cluster.ClusterRouter router = null;
        com.anjali.distkv.cluster.HealthChecker healthChecker = null;

        if (peersArg != null || !"node1".equals(nodeId)) {
            com.anjali.distkv.cluster.HashRing ring = new com.anjali.distkv.cluster.HashRing();
            com.anjali.distkv.cluster.ReplicationClient client = new com.anjali.distkv.cluster.ReplicationClient();
            healthChecker = new com.anjali.distkv.cluster.HealthChecker(nodeId, client);
            router = new com.anjali.distkv.cluster.ClusterRouter(nodeId, store, wal, ring, healthChecker, client);

            // Register self
            router.registerNodeAddress(nodeId, "127.0.0.1", port);

            // Register peers: e.g. "node2:7001,node3:7002" or "node2:host2:7001"
            if (peersArg != null && !peersArg.isBlank()) {
                for (String peerStr : peersArg.split(",")) {
                    peerStr = peerStr.trim();
                    if (peerStr.isEmpty()) continue;
                    String[] parts = peerStr.split(":");
                    if (parts.length == 2) {
                        router.registerNodeAddress(parts[0], "127.0.0.1", Integer.parseInt(parts[1]));
                    } else if (parts.length == 3) {
                        router.registerNodeAddress(parts[0], parts[1], Integer.parseInt(parts[2]));
                    }
                }
            }
        }

        Server server = new Server(port, store, wal, router);
        server.start();

        // Start Dashboard Server on port 8080 (or custom --web-port)
        DashboardServer dashboard = null;
        int webPort = 8080;
        try {
            dashboard = new DashboardServer(webPort, store, router);
            dashboard.start();
        } catch (IOException e) {
            System.err.println("[DistKV] Dashboard could not bind to port " + webPort + ": " + e.getMessage());
        }

        System.out.printf("[%s] DistKV Server running on port %d [%s] [Durability: %s] [Cluster: %s] (press Ctrl+C to stop)...%n",
                nodeId,
                server.getPort(),
                useSharding ? "16 Shards" : "Single Lock",
                (wal != null) ? ("WAL " + fsyncPolicy) : "In-Memory",
                (router != null) ? "RF=2 Active" : "Standalone");

        final com.anjali.distkv.storage.SnapshotManager sm = snapshotManager;
        final com.anjali.distkv.cluster.HealthChecker hc = healthChecker;
        final DashboardServer db = dashboard;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            if (sm != null) sm.close();
            if (hc != null) hc.close();
            if (db != null) db.close();
        }));

        Thread.currentThread().join();
    }
}

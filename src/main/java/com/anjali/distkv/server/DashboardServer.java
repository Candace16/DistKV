package com.anjali.distkv.server;

import com.anjali.distkv.cluster.ClusterRouter;
import com.anjali.distkv.cluster.HashRing;
import com.anjali.distkv.cluster.HealthChecker;
import com.anjali.distkv.protocol.Command;
import com.anjali.distkv.protocol.CommandParser;
import com.anjali.distkv.protocol.Response;
import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Embedded HTTP server for the DistKV Phase 7 single-page visual dashboard.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code GET /api/stats} - Live metrics, per-node up/down state, p50/p95/p99 latency, 60s throughput history.</li>
 *   <li>{@code GET /api/ring} - Ring topology, virtual node positions, and up to 300 sampled key coordinates.</li>
 *   <li>{@code POST /api/cmd} - Executes a command (SET, GET, DEL, TTL, PING) and returns reply + key coordinates.</li>
 *   <li>{@code GET /} - Serves static frontend assets (HTML, CSS, JS) from {@code src/main/resources/web/}.</li>
 * </ul>
 */
public final class DashboardServer implements Closeable {

    private final int port;
    private final Store store;
    private final ClusterRouter router;
    private final HttpServer server;
    private final ScheduledExecutorService scheduler;

    // Real-time metrics tracking
    private final AtomicLong opsInCurrentSecond = new AtomicLong(0);
    private final int[] throughputHistory = new int[60]; // 60 seconds rolling window
    private int historyIndex = 0;

    // Latency reservoir (nanoseconds) for real p50, p95, p99 calculation
    private final long[] latencyReservoir = new long[1000];
    private int reservoirIndex = 0;
    private int reservoirCount = 0;

    public DashboardServer(int port, Store store, ClusterRouter router) throws IOException {
        this.port = port;
        this.store = store;
        this.router = router;

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(Executors.newFixedThreadPool(4));

        this.server.createContext("/api/stats", new StatsHandler());
        this.server.createContext("/api/ring", new RingHandler());
        this.server.createContext("/api/cmd", new CommandHandler());
        this.server.createContext("/", new StaticFileHandler());

        // Background scheduler rolling 60s throughput window every second
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "distkv-dashboard-sampler");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::tickSecond, 1, 1, TimeUnit.SECONDS);
    }

    public void start() {
        server.start();
        System.out.println("[DistKV] Dashboard running at http://localhost:" + port);
    }

    private synchronized void tickSecond() {
        int ops = (int) opsInCurrentSecond.getAndSet(0);
        throughputHistory[historyIndex] = ops;
        historyIndex = (historyIndex + 1) % throughputHistory.length;
    }

    private synchronized void recordLatency(long nanos) {
        opsInCurrentSecond.incrementAndGet();
        latencyReservoir[reservoirIndex] = nanos;
        reservoirIndex = (reservoirIndex + 1) % latencyReservoir.length;
        if (reservoirCount < latencyReservoir.length) {
            reservoirCount++;
        }
    }

    private synchronized double[] computePercentiles() {
        if (reservoirCount == 0) {
            return new double[]{0.0, 0.0, 0.0};
        }
        long[] copy = new long[reservoirCount];
        System.arraycopy(latencyReservoir, 0, copy, 0, reservoirCount);
        Arrays.sort(copy);

        double p50 = copy[(int) (copy.length * 0.50)] / 1000.0; // microseconds
        double p95 = copy[(int) (copy.length * 0.95)] / 1000.0;
        double p99 = copy[(int) (copy.length * 0.99)] / 1000.0;
        return new double[]{p50, p95, p99};
    }

    private synchronized int[] getOrderedThroughputHistory() {
        int[] ordered = new int[throughputHistory.length];
        for (int i = 0; i < throughputHistory.length; i++) {
            ordered[i] = throughputHistory[(historyIndex + i) % throughputHistory.length];
        }
        return ordered;
    }

    // ==================== REST API HANDLERS ====================

    private final class StatsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            double[] percentiles = computePercentiles();
            int[] history = getOrderedThroughputHistory();
            int currentOpsPerSec = history[history.length - 1];

            // Build node list
            String localId = (router != null) ? router.getLocalNodeId() : "node1";
            HealthChecker hc = (router != null) ? router.getHealthChecker() : null;

            Map<String, String> statusMap = (hc != null) ? hc.getStatusesSnapshot() : Map.of(localId, "up");

            // Extract store stats
            int localKeyCount = 0;
            final int[] totalKeys = {0};
            store.dump((k, v) -> totalKeys[0]++);
            localKeyCount = totalKeys[0];

            StringBuilder nodesJson = new StringBuilder("[");
            int idx = 0;
            for (Map.Entry<String, String> entry : statusMap.entrySet()) {
                String id = entry.getKey();
                String status = entry.getValue();
                long downDuration = (hc != null) ? hc.getDownDurationSeconds(id) : 0L;

                if (idx++ > 0) nodesJson.append(",");
                nodesJson.append(String.format(
                        "{\"id\":\"%s\",\"status\":\"%s\",\"down_since_seconds\":%d,\"is_local\":%b}",
                        id, status, downDuration, id.equals(localId)
                ));
            }
            nodesJson.append("]");

            StringBuilder historyJson = new StringBuilder("[");
            for (int i = 0; i < history.length; i++) {
                if (i > 0) historyJson.append(",");
                historyJson.append(history[i]);
            }
            historyJson.append("]");

            String json = String.format(
                    "{\"ops_per_sec\":%d,\"p50_us\":%.2f,\"p95_us\":%.2f,\"p99_us\":%.2f,\"total_keys\":%d,\"nodes\":%s,\"throughput_history\":%s}",
                    currentOpsPerSec, percentiles[0], percentiles[1], percentiles[2], localKeyCount, nodesJson, historyJson
            );

            sendJson(exchange, json);
        }
    }

    private final class RingHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            HashRing ring = (router != null) ? router.getHashRing() : new HashRing();
            HealthChecker hc = (router != null) ? router.getHealthChecker() : null;
            String localId = (router != null) ? router.getLocalNodeId() : "node1";

            // If standalone ring has no nodes, register local
            if (ring.size() == 0) {
                ring.addNode(localId);
            }

            // 1. Virtual nodes
            List<HashRing.VNode> vnodes = ring.getAllVNodes();
            StringBuilder vnodesJson = new StringBuilder("[");
            for (int i = 0; i < vnodes.size(); i++) {
                HashRing.VNode vn = vnodes.get(i);
                if (i > 0) vnodesJson.append(",");
                vnodesJson.append(String.format("{\"node\":\"%s\",\"angle\":%.6f}", vn.nodeId(), vn.angleNormalized()));
            }
            vnodesJson.append("]");

            // 2. Physical nodes
            List<String> physicalNodes = new ArrayList<>(ring.getPhysicalNodes());
            Collections.sort(physicalNodes);
            StringBuilder nodesJson = new StringBuilder("[");
            for (int i = 0; i < physicalNodes.size(); i++) {
                String id = physicalNodes.get(i);
                boolean isUp = (hc == null) || hc.isNodeUp(id);
                long downSec = (hc != null) ? hc.getDownDurationSeconds(id) : 0L;
                if (i > 0) nodesJson.append(",");
                nodesJson.append(String.format(
                        "{\"id\":\"%s\",\"status\":\"%s\",\"down_since_seconds\":%d}",
                        id, isUp ? "up" : "down", downSec
                ));
            }
            nodesJson.append("]");

            // 3. Sample of keys (up to 300 keys)
            List<String> sampledKeys = new ArrayList<>(300);
            store.dump((k, v) -> {
                if (sampledKeys.size() < 300) {
                    sampledKeys.add(k);
                }
            });

            StringBuilder keysJson = new StringBuilder("[");
            for (int i = 0; i < sampledKeys.size(); i++) {
                String k = sampledKeys.get(i);
                long hash = HashRing.hash(k);
                double angle = HashRing.hashToNormalized(hash);

                List<String> owners = ring.getPrimaryAndReplica(k);
                String primary = owners.isEmpty() ? localId : owners.get(0);
                String replica = owners.size() > 1 ? owners.get(1) : primary;

                boolean primaryUp = (hc == null) || hc.isNodeUp(primary);
                String activeOwner = primaryUp ? primary : replica;

                if (i > 0) keysJson.append(",");
                keysJson.append(String.format(
                        "{\"key\":\"%s\",\"angle\":%.6f,\"primary\":\"%s\",\"replica\":\"%s\",\"active_owner\":\"%s\"}",
                        escapeJson(k), angle, primary, replica, activeOwner
                ));
            }
            keysJson.append("]");

            String json = String.format("{\"nodes\":%s,\"vnodes\":%s,\"keys\":%s}", nodesJson, vnodesJson, keysJson);
            sendJson(exchange, json);
        }
    }

    private final class CommandHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            // Handle plain string or JSON {"cmd":"..."}
            String commandLine = body;
            if (body.startsWith("{") && body.contains("\"cmd\"")) {
                int start = body.indexOf("\"cmd\"") + 5;
                start = body.indexOf("\"", start) + 1;
                int end = body.indexOf("\"", start);
                if (start > 0 && end > start) {
                    commandLine = body.substring(start, end);
                }
            }

            long startNanos = System.nanoTime();
            Command cmd = CommandParser.parse(commandLine);

            String reply;
            if (router != null) {
                reply = router.execute(cmd);
            } else {
                reply = executeStandalone(cmd);
            }
            long elapsedNanos = System.nanoTime() - startNanos;
            recordLatency(elapsedNanos);

            // Compute key routing coordinates if command has a key
            String key = extractKey(cmd);
            String primary = "";
            String replica = "";
            double angle = 0.0;

            if (key != null) {
                HashRing ring = (router != null) ? router.getHashRing() : null;
                if (ring == null) {
                    ring = new HashRing(100, List.of("node1"));
                }
                long hash = HashRing.hash(key);
                angle = HashRing.hashToNormalized(hash);
                List<String> owners = ring.getPrimaryAndReplica(key);
                if (!owners.isEmpty()) primary = owners.get(0);
                if (owners.size() > 1) replica = owners.get(1);
            }

            String json = String.format(
                    "{\"reply\":\"%s\",\"key\":%s,\"angle\":%.6f,\"primary\":\"%s\",\"replica\":\"%s\",\"elapsed_us\":%.2f}",
                    escapeJson(reply),
                    (key != null ? "\"" + escapeJson(key) + "\"" : "null"),
                    angle, primary, replica, (elapsedNanos / 1000.0)
            );
            sendJson(exchange, json);
        }

        private String executeStandalone(Command cmd) {
            if (cmd instanceof Command.Ping) {
                return Response.PONG;
            } else if (cmd instanceof Command.Set set) {
                store.set(set.key(), set.value(), set.ttlSeconds());
                return Response.OK;
            } else if (cmd instanceof Command.Get get) {
                String val = store.get(get.key());
                return (val != null) ? Response.value(val) : Response.NOT_FOUND;
            } else if (cmd instanceof Command.Del del) {
                return store.del(del.key()) ? Response.OK : Response.NOT_FOUND;
            } else if (cmd instanceof Command.Exists exists) {
                return store.exists(exists.key()) ? Response.EXISTS_TRUE : Response.EXISTS_FALSE;
            } else if (cmd instanceof Command.Ttl ttl) {
                return Response.ttl(store.ttl(ttl.key()));
            } else if (cmd instanceof Command.Stats) {
                return store.statsJson();
            } else if (cmd instanceof Command.Invalid inv) {
                return Response.err(inv.reason());
            }
            return Response.err("Unknown command");
        }

        private String extractKey(Command cmd) {
            if (cmd instanceof Command.Set s) return s.key();
            if (cmd instanceof Command.Get g) return g.key();
            if (cmd instanceof Command.Del d) return d.key();
            if (cmd instanceof Command.Exists e) return e.key();
            if (cmd instanceof Command.Ttl t) return t.key();
            return null;
        }
    }

    private final class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            if (path == null || path.equals("/") || path.isEmpty()) {
                path = "/index.html";
            }

            // Clean leading slashes
            while (path.startsWith("/")) {
                path = path.substring(1);
            }

            byte[] content = loadResourceOrFile(path);
            if (content == null) {
                sendResponse(exchange, 404, "File Not Found: " + path);
                return;
            }

            String contentType = "text/plain";
            if (path.endsWith(".html")) contentType = "text/html; charset=UTF-8";
            else if (path.endsWith(".css")) contentType = "text/css; charset=UTF-8";
            else if (path.endsWith(".js")) contentType = "application/javascript; charset=UTF-8";
            else if (path.endsWith(".svg")) contentType = "image/svg+xml";

            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(content);
            }
        }

        private byte[] loadResourceOrFile(String relativePath) {
            // 1. Try file on disk in src/main/resources/web/
            File diskFile = new File("src/main/resources/web/" + relativePath);
            if (diskFile.exists() && diskFile.isFile()) {
                try (InputStream fis = new FileInputStream(diskFile)) {
                    return fis.readAllBytes();
                } catch (IOException ignored) {}
            }

            // 2. Try classpath
            try (InputStream is = getClass().getResourceAsStream("/web/" + relativePath)) {
                if (is != null) {
                    return is.readAllBytes();
                }
            } catch (IOException ignored) {}

            return null;
        }
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String escapeJson(String raw) {
        if (raw == null) return "";
        return raw.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\b", "\\b")
                  .replace("\f", "\\f")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    @Override
    public void close() {
        if (scheduler != null) scheduler.shutdownNow();
        if (server != null) server.stop(0);
    }
}

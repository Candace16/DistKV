package com.anjali.distkv.store;

import com.anjali.distkv.server.Server;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Concurrency Stress Tests (50 Concurrent Clients)")
class StoreConcurrencyTest {

    private static final int CONCURRENT_CLIENTS = 50;
    private static final int OPS_PER_CLIENT = 200; // 10,000 total operations

    @Test
    @DisplayName("50 threads concurrently mutating SimpleStore (Single Lock)")
    void testConcurrentSimpleStore() throws InterruptedException {
        SimpleStore store = new SimpleStore(1000);
        runConcurrencyStress(store);
    }

    @Test
    @DisplayName("50 threads concurrently mutating ShardedStore (16 Shards)")
    void testConcurrentShardedStore() throws InterruptedException {
        try (ShardedStore store = new ShardedStore(1000, 16)) {
            runConcurrencyStress(store);
        }
    }

    private void runConcurrencyStress(Store store) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(CONCURRENT_CLIENTS);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 0; i < CONCURRENT_CLIENTS; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startGate.await(); // Thundering herd start
                    for (int op = 0; op < OPS_PER_CLIENT; op++) {
                        String key = "key:" + ((threadId + op) % 50);
                        String value = "val:" + threadId + ":" + op;

                        // Mix of writes, reads, and deletes
                        if (op % 4 == 0) {
                            store.set(key, value, OptionalLong.empty());
                        } else if (op % 4 == 1) {
                            store.get(key);
                        } else if (op % 4 == 2) {
                            store.exists(key);
                        } else {
                            store.del(key);
                        }
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    finishGate.countDown();
                }
            });
        }

        // Release all threads simultaneously
        startGate.countDown();
        boolean completed = finishGate.await(15, TimeUnit.SECONDS);

        executor.shutdownNow();

        assertTrue(completed, "Concurrency test timed out; possible deadlock");
        assertEquals(0, failureCount.get(), "Exceptions occurred during concurrent access");
    }

    @Test
    @DisplayName("50 simultaneous TCP client connections to Server thread pool")
    void testServerTcpConcurrencyWith50Clients() throws Exception {
        try (Server server = new Server(0, new ShardedStore(10_000, 16), 16, 64, 500)) {
            server.start();
            int port = server.getPort();

            ExecutorService clientPool = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch endGate = new CountDownLatch(CONCURRENT_CLIENTS);
            AtomicInteger successResponses = new AtomicInteger(0);

            for (int i = 0; i < CONCURRENT_CLIENTS; i++) {
                final int clientId = i;
                clientPool.submit(() -> {
                    try {
                        startGate.await();
                        try (Socket socket = new Socket("127.0.0.1", port);
                             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {

                            // 1. PING
                            writer.write("PING\r\n");
                            writer.flush();
                            String pong = reader.readLine();
                            if ("PONG".equals(pong)) {
                                successResponses.incrementAndGet();
                            }

                            // 2. SET client:X
                            writer.write("SET client:" + clientId + " val_" + clientId + "\r\n");
                            writer.flush();
                            String setResp = reader.readLine();
                            if ("OK".equals(setResp)) {
                                successResponses.incrementAndGet();
                            }

                            // 3. GET client:X
                            writer.write("GET client:" + clientId + "\r\n");
                            writer.flush();
                            String getResp = reader.readLine();
                            if (("VALUE val_" + clientId).equals(getResp)) {
                                successResponses.incrementAndGet();
                            }
                        }
                    } catch (Exception ignored) {
                    } finally {
                        endGate.countDown();
                    }
                });
            }

            startGate.countDown();
            boolean finished = endGate.await(20, TimeUnit.SECONDS);
            clientPool.shutdownNow();

            assertTrue(finished, "TCP clients timed out; possible connection starvation");
            // 50 clients * 3 operations each = 150 successful responses
            assertEquals(CONCURRENT_CLIENTS * 3, successResponses.get());
        }
    }
}

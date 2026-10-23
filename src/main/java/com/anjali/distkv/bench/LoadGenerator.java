package com.anjali.distkv.bench;

import com.anjali.distkv.store.ShardedStore;
import com.anjali.distkv.store.SimpleStore;
import com.anjali.distkv.store.Store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * High-performance load generator for benchmarking DistKV.
 *
 * <p>Benchmarking Methodology:
 * <ul>
 *   <li><b>JIT Warm-up Phase:</b> Executes warm-up requests to trigger HotSpot C2 JIT optimization
 *       and memory pre-allocation before measuring.</li>
 *   <li><b>Measurement Phase:</b> Releases concurrent worker threads simultaneously via {@link CountDownLatch}
 *       and records individual operation latencies in nanoseconds.</li>
 *   <li><b>Percentile Reporting:</b> Accurately computes p50, p95, and p99 latencies, along with throughput (ops/sec).</li>
 * </ul>
 */
public final class LoadGenerator {

    public record BenchmarkResult(
            String engineName,
            int concurrency,
            int totalOps,
            double durationSeconds,
            double opsPerSec,
            double p50Micros,
            double p95Micros,
            double p99Micros
    ) {
        @Override
        public String toString() {
            return String.format(
                    "| %-16s | %11d | %10d | %12.2f | %10.2f µs | %10.2f µs | %10.2f µs |",
                    engineName, concurrency, (long) opsPerSec, durationSeconds, p50Micros, p95Micros, p99Micros
            );
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int capacity = 100_000;
        int opsPerClient = 2_000;
        int[] concurrencyLevels = {1, 10, 50};

        System.out.println("==================================================================================================");
        System.out.println("                               DISTKV BENCHMARK SUITE");
        System.out.println("==================================================================================================");
        System.out.println("| Storage Engine   | Concurrency | Throughput | Elapsed (s)  | p50 Latency | p95 Latency | p99 Latency |");
        System.out.println("|------------------|-------------|------------|--------------|-------------|-------------|-------------|");

        for (int concurrency : concurrencyLevels) {
            // 1. Benchmark Single-Lock Store
            Store singleStore = new SimpleStore(capacity);
            BenchmarkResult singleRes = runBenchmark("Single-Lock", singleStore, concurrency, opsPerClient);
            System.out.println(singleRes);

            // 2. Benchmark 16-Shard Store
            try (ShardedStore shardedStore = new ShardedStore(capacity, 16)) {
                BenchmarkResult shardedRes = runBenchmark("16-Shards", shardedStore, concurrency, opsPerClient);
                System.out.println(shardedRes);
            }
            System.out.println("|------------------|-------------|------------|--------------|-------------|-------------|-------------|");
        }
        System.out.println("==================================================================================================");
    }

    public static BenchmarkResult runBenchmark(
            String name,
            Store store,
            int concurrency,
            int opsPerClient
    ) throws InterruptedException {
        // --- 1. JIT Warm-up (5,000 unmeasured ops) ---
        for (int i = 0; i < 5_000; i++) {
            store.set("warmup:" + i, "val", OptionalLong.empty());
            store.get("warmup:" + i);
        }

        // --- 2. Measurement Phase ---
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(concurrency);

        List<long[]> threadLatencies = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < concurrency; t++) {
            final int threadId = t;
            pool.submit(() -> {
                long[] latencies = new long[opsPerClient];
                try {
                    startGate.await();
                    ThreadLocalRandom random = ThreadLocalRandom.current();

                    for (int i = 0; i < opsPerClient; i++) {
                        String key = "key:" + (random.nextInt(10_000));
                        long start = System.nanoTime();

                        // 80% reads, 20% writes
                        if (random.nextInt(100) < 80) {
                            store.get(key);
                        } else {
                            store.set(key, "data", OptionalLong.empty());
                        }

                        latencies[i] = System.nanoTime() - start;
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    threadLatencies.add(latencies);
                    doneGate.countDown();
                }
            });
        }

        long startNanos = System.nanoTime();
        startGate.countDown(); // Start all threads at once
        doneGate.await(60, TimeUnit.SECONDS);
        long totalNanos = System.nanoTime() - startNanos;

        pool.shutdownNow();

        // --- 3. Aggregate Latency Percentiles ---
        int totalOps = concurrency * opsPerClient;
        long[] allLatencies = new long[totalOps];
        int offset = 0;
        for (long[] tLat : threadLatencies) {
            System.arraycopy(tLat, 0, allLatencies, offset, tLat.length);
            offset += tLat.length;
        }

        Arrays.sort(allLatencies);

        double durationSeconds = totalNanos / 1_000_000_000.0;
        double opsPerSec = totalOps / durationSeconds;
        double p50Micros = allLatencies[(int) (totalOps * 0.50)] / 1_000.0;
        double p95Micros = allLatencies[(int) (totalOps * 0.95)] / 1_000.0;
        double p99Micros = allLatencies[(int) (totalOps * 0.99)] / 1_000.0;

        return new BenchmarkResult(
                name,
                concurrency,
                totalOps,
                durationSeconds,
                opsPerSec,
                p50Micros,
                p95Micros,
                p99Micros
        );
    }
}

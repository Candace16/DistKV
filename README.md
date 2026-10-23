# DistKV: Distributed In-Memory Key-Value Store

[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://adoptium.net)
[![Build](https://img.shields.io/badge/Build-Maven-blue.svg)](https://maven.apache.org/)
[![Tests](https://img.shields.io/badge/Tests-34%20Passing-brightgreen.svg)]()
[![Docker](https://img.shields.io/badge/Docker-3--Node%20Cluster-2496ED.svg)](docker-compose.yml)
[![License](https://img.shields.io/badge/License-MIT-green.svg)]()

**DistKV** is a distributed, in-memory, durable key-value store engineered from scratch in **Java 17** without high-level frameworks. It combines the low-latency caching mechanics of **Redis** with the decentralized partitioning and replication topology of **Amazon Dynamo**.

Designed as a systems engineering portfolio project for top-tier software engineering roles (Amazon SDE), it demonstrates concrete mastery of:
- **Data Structures**: Hand-written $O(1)$ LRU cache (HashMap + custom doubly-linked list with sentinels) and min-heap priority queues.
- **Concurrency & OS Fundamentals**: Thread pooling (`ThreadPoolExecutor`), lock striping (16 shards), non-blocking metrics (`LongAdder`), blocking socket I/O, and `fsync` storage durability.
- **Distributed Systems**: Consistent hashing with virtual nodes ($\sim 100$ vnodes), replication factor 2 (RF=2), and 3-strike heartbeat failure detection.

---

## 🏛️ System Architecture

```
                          [ TCP Client ] (nc / telnet / DistKvCli)
                                 │
                                 ▼ (Plain TCP, ASCII Protocol)
                     ┌───────────────────────┐
                     │ Server (Accept Loop)  │
                     └───────────┬───────────┘
                                 │ Hands socket
                                 ▼
                     ┌───────────────────────┐
                     │ Bounded ThreadPool    │ (ArrayBlockingQueue + AbortPolicy)
                     │ (Worker Threads)      │
                     └───────────┬───────────┘
                                 │
                                 ▼
                     ┌───────────────────────┐
                     │ CommandParser         │ (Line limit <= 8KB, DoS safe)
                     └───────────┬───────────┘
                                 │
                     ┌───────────┴───────────┐
                     ▼                       ▼
            [ ClusterRouter ]        [ HealthChecker ] (1s Ping, 3-strike)
          (Consistent HashRing)              ▲
            /               \                │ Monitors
           ▼                 ▼               │
      [ Primary ]       [ Replica ] ─────────┘
           │
           ▼
    ┌────────────────────────────────────────────────────────┐
    │          16-Shard Partitioned Storage Engine           │
    │  Shard 0      Shard 1      Shard 2  ...   Shard 15     │
    │ [Lock|LRU]   [Lock|LRU]   [Lock|LRU]     [Lock|LRU]    │
    └──────────────┬────────────────────────────┬────────────┘
                   │                            │
                   ▼                            ▼
      ┌─────────────────────────┐  ┌─────────────────────────┐
      │  Write-Ahead Log (WAL)  │  │  Active TTL Sweeper     │
      │  (Append-Only, fsync)   │  │  (Min-Heap MinPriority) │
      └────────────┬────────────┘  └─────────────────────────┘
                   │ Truncates on snapshot
                   ▼
      ┌─────────────────────────┐
      │  Periodic Snapshot      │
      │  (AtomicMove swap)      │
      └─────────────────────────┘
```

---

## ⚡ Algorithmic Complexity

| Component / Operation | Time Complexity | Space Complexity | Description |
|---|---|---|---|
| **LRU Cache `get(k)`** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | HashMap lookup + unlinking and moving node to head. |
| **LRU Cache `put(k, v)`** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | Inserts at head sentinel; evicts tail sentinel if size > capacity. |
| **LRU Cache `remove(k)`** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | Unlinks node in doubly-linked list and removes from HashMap. |
| **Consistent Hash Ring** | $\mathcal{O}(\log(N \cdot V))$ | $\mathcal{O}(N \cdot V)$ | `TreeMap.ceilingEntry()` across $N$ nodes $\times$ $V=100$ vnodes. |
| **TTL Sweeper Check** | $\mathcal{O}(1)$ peek | $\mathcal{O}(K)$ | Min-heap top inspection; purges in $\mathcal{O}(\log K)$ only if expired. |
| **Shard Key Routing** | $\mathcal{O}(1)$ | $\mathcal{O}(1)$ | Bitwise `Math.floorMod(hash(key), 16)`. |
| **WAL Sequential Append** | $\mathcal{O}(1)$ amortized | $\mathcal{O}(1)$ | Append-only sequential disk write (`FileChannel`). |

---

## 📊 Benchmark Results (Real Hardware Measurements)

Benchmarked on **Java 17 (HotSpot 64-bit)** with an **80% Read / 20% Write** workload after JIT warm-up across $10,000$ active keys:

| Concurrency Level | Storage Engine | Throughput (ops/sec) | Duration | p50 Latency | p95 Latency | p99 Latency |
|:---:|:---:|---:|---:|---:|---:|---:|
| **1 Client** | Single-Lock | 134,549 ops/sec | 0.01s | 0.50 µs | 1.80 µs | 24.20 µs |
| **1 Client** | 16-Shards | 103,530 ops/sec | 0.02s | 0.50 µs | 0.80 µs | 3.10 µs |
| **10 Clients** | Single-Lock | 563,575 ops/sec | 0.04s | 0.30 µs | 1.00 µs | 2.80 µs |
| **10 Clients** | 16-Shards | 533,477 ops/sec | 0.04s | 0.80 µs | 3.00 µs | 11.80 µs |
| **50 Clients** | **Single-Lock** | 809,662 ops/sec | 0.12s | 0.60 µs | 6.40 µs | **1,322.60 µs** |
| **50 Clients** | **16-Shards** | **1,770,500 ops/sec** | 0.06s | 0.60 µs | 1.80 µs | **133.90 µs** |

### Benchmark Analysis & Engineering Insights
1. **Low Concurrency (1-10 Clients)**: Both engines easily process hundreds of thousands of operations per second with sub-microsecond p50 latencies.
2. **High Contention (50 Clients)**:
   - In `Single-Lock`, threads fiercely contend on the single `ReentrantLock`. Lock queuing causes p99 tail latency to blow out to **1.32 milliseconds** ($1,322\text{ µs}$).
   - In `16-Shards`, key traffic distributes evenly across 16 striped locks. Contention drops by ~16x, propelling throughput to **1.77 Million ops/sec** and slashing tail p99 latency by **10x** down to **133.9 µs**!

---

## 🛠️ Protocol Specification

DistKV uses an ASCII line-based protocol over raw TCP:

| Command | Arguments | Returns | Description |
|---|---|---|---|
| `SET` | `key value [EX seconds]` | `OK` | Stores key-value with optional TTL in seconds. |
| `GET` | `key` | `VALUE <v>` \| `NOT_FOUND` | Fetches value; lazily purges if expired. |
| `DEL` | `key` | `OK` \| `NOT_FOUND` | Deletes key from store and WAL. |
| `EXISTS`| `key` | `1` \| `0` | Returns existence status without recency mutation. |
| `TTL` | `key` | `<seconds>` \| `-1` \| `-2` | Remaining seconds (`-1` = persistent, `-2` = missing). |
| `PING` | *(none)* | `PONG` | Heartbeat pulse. |
| `STATS`| *(none)* | `JSON` | Real-time ops/sec, hit rate, evictions, and node status. |

---

## 🚀 Quick Start Guide

### 1. Build and Run All Tests
```bash
mvn clean test
```
*Executes all 34 automated unit, concurrency, crash recovery, and cluster tests.*

### 2. Run Standalone Server
```bash
java -jar target/distkv-1.0.0-SNAPSHOT.jar --port 7000 --sharded --data-dir ./data/node1 --fsync everysec
```

### 3. Connect via Interactive CLI
```bash
java -jar target/distkv-1.0.0-SNAPSHOT.jar cli 7000
```
```text
Connecting to DistKV at 127.0.0.1:7000...
Connected! Type commands (SET, GET, DEL, EXISTS, TTL, STATS, PING). Type 'exit' to quit.
distkv> SET user:1 Anjali EX 3600
OK
distkv> GET user:1
VALUE Anjali
distkv> STATS
{"ops_per_sec":12040,"hit_rate":1.00,"keys":1,"evictions":0,"shards":16,"node_status":"up"}
```

### 4. Run 3-Node Cluster via Docker Compose
```bash
docker compose up --build
```
Spins up:
- `node1` on `localhost:7000`
- `node2` on `localhost:7001`
- `node3` on `localhost:7002`

---

## ⚖️ Design Decisions and Limitations

In distributed systems, every architecture is defined by its trade-offs:

1. **Eventual Consistency over Strong Consistency**:
   - DistKV implements **asynchronous replication with eventual consistency** (AP in CAP theorem).
   - Writes are confirmed once committed locally and dispatched to the replica. There is no multi-phase commit (2PC) or Raft consensus.
   - *Limitation*: If a primary node crashes abruptly before an asynchronous write reaches the replica, a temporary stale read is possible until gossip recovery.
2. **Exclusive Locks for Reads in LRU**:
   - Because `LRUCache.get()` updates recency pointers, reads mutate state. `ReentrantReadWriteLock` cannot be used safely without lock-free skip-lists. We chose **16-shard lock striping**, which delivers over **1.77M ops/sec** with minimal architectural complexity.
3. **Fsync Safety vs. Speed**:
   - `everysec` offers the optimal sweet spot: sequential I/O speed with an upper bound of 1 second of loss exposure on catastrophic host power failure.

---

## 🎯 Systems Interview Defense Cheat Sheet

### Q1: Why not use Java's standard `LinkedHashMap` for the LRU cache?
> *"In a production database or internship portfolio, using `LinkedHashMap` conceals whether you understand memory pointer manipulation and amortized analysis. Hand-crafting `HashMap<K, Node<K,V>>` with head/tail sentinel nodes proves mastery of constant-time pointer splicing and boundary management."*

### Q2: Why does an LRU cache suffer under standard `ReadWriteLock`?
> *"Reads in an LRU cache are not pure reads; every access promotes the node to the head of the doubly-linked list. Under concurrent reader threads, list pointer modifications cause race conditions and memory corruption. Sharding the cache into 16 striped locks decouples key traffic, yielding zero lock contention for items in different shards."*

### Q3: How does your consistent hashing handle node additions/removals?
> *"With $N$ servers, modular hashing relocates almost $100\%$ of keys when $N$ changes. DistKV's 64-bit MD5 consistent hash ring guarantees that only $\approx 1/N$ of keys migrate to the newly added or remaining server. Virtual nodes ($\sim 100$ per node) eliminate ring variance and hot spots."*

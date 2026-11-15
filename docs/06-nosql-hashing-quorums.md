# Module 06: Distributed Systems, Consistent Hashing & Quorum Consensus

> **Component Locations:** 
> * `src/distributed/snowflake.py`
> * `src/distributed/consistent_hash.py`
> * `src/distributed/quorum_simulator.py`

>**Test Suite:** `tests/test_distributed.py`  
> **Architectural Scope:** Lock-Free Distributed Primary Keys, Dynamo-Style Ring Sharding, and Tunable Quorum Consistency ($W + R > RF$).

---

## Part 1: 64-Bit Snowflake ID Generator (`src/distributed/snowflake.py`)

### 1. What Is It?
A **Snowflake ID** is a deterministic, time-ordered **64-bit integer** (`BIGINT` in SQL) generated at the application layer rather than inside a database engine. Instead of relying on random characters or centralized database counters, it bit-packs four distinct metadata segments into a single 64-bit binary number:

```text
 63  62                                        22 21        12 11          0
+---+--------------------------------------------+------------+-------------+
| 0 |             41-bit Timestamp               | 10-bit Node| 12-bit Seq  |
+---+--------------------------------------------+------------+-------------+
  |                       |                            |             |
  |                       |                            |             +-- 0 to 4,095 IDs per ms
  |                       |                            +---------------- 0 to 1,023 physical/virtual nodes
  |                       +--------------------------------------------- Milliseconds since custom Epoch
  +--------------------------------------------------------------------- Sign bit (always 0 = positive integer)

```

* **Bit 63 (1 Bit — Sign Bit):** Pinned to `0` so every generated ID is a positive 64-bit signed integer.
* **Bits 22–62 (41 Bits — Timestamp):** Milliseconds elapsed since a custom epoch (`1704067200000` = Jan 1, 2024 UTC). $2^{41}$ milliseconds provides **~69.7 years** of continuous operation.
* **Bits 12–21 (10 Bits — Node/Machine ID):** Uniquely identifies up to $2^{10} = 1,024$ independent backend instances or containers.
* **Bits 0–11 (12 Bits — Sequence Counter):** Per-millisecond local counter allowing each node to generate up to $2^{12} = 4,096$ unique IDs within the exact same millisecond (**4.096 million IDs/second per node**).

---

### 2. Why Do We Need It?

When an application scales beyond a single database instance into a sharded or multi-master cluster, database-level `AUTO_INCREMENT` (`SERIAL`) breaks down:

1. **Collision Across Shards:** Two database nodes incrementing locally will generate identical primary keys (`ID = 105` on Node A and `ID = 105` on Node B).
2. **Centralized Coordinator Bottleneck:** Using a single dedicated "ID Server" reintroduces a **Single Point of Failure (SPOF)** and adds network round-trip latency to every `INSERT`.
3. **Why Not UUIDv4?** While 128-bit random `UUIDv4` values prevent collisions without coordination, they consume **2x the storage** (16 bytes vs. 8 bytes), cause severe **B-Tree index page fragmentation** on disk due to random insertion order, and **cannot be sorted chronologically**.

---

### 3. How It Works in PulseStream

Our `SnowflakeGenerator` class enforces thread-safety, clock-drift protection, and constant-time $O(1)$ bit-packing:

1. **Thread Synchronization:** Uses a `threading.Lock()` so concurrent worker threads on the same API node never read or mutate `self.sequence` simultaneously.
2. **NTP Clock-Rollback Guard:** Compares `current_ts` against `self.last_timestamp`. If the system clock drifts backward (`current_ts < self.last_timestamp`), the generator immediately raises a `RuntimeError` to prevent duplicate ID generation.
3. **Sub-Millisecond Burst Handling:** If multiple requests arrive in the exact same millisecond (`current_ts == self.last_timestamp`), `self.sequence` increments and masks against `0xFFF` (`4095`). If a burst exceeds 4,096 IDs in 1ms (`self.sequence == 0`), the generator spin-waits until the next millisecond ticks.
4. **Bitwise Encoding & Zero-Query Decoding:**
* **Encoding:** `((current_ts - EPOCH) << 22) | (node_id << 12) | sequence`
* **Decoding:** Because the creation time is embedded in the top 41 bits, the API can extract the exact creation timestamp (`(snowflake_id >> 22) + EPOCH`) without querying a `created_at` column from the database.



---

### 4. Comparison of Distributed ID Strategies

| ID Strategy | Size | Time-Sortable | B-Tree Index Locality | Coordination Needed | Primary Engineering Trade-Off |
| --- | --- | --- | --- | --- | --- |
| DB `AUTO_INCREMENT` | 64-bit | Yes | Excellent | High (Central DB) | Single Point of Failure; fails in multi-node write clusters |
| Natural Composite Key | Variable | No | Poor | None | Bloats foreign keys; business attributes change over time |
| `UUIDv4` (Random) | 128-bit | No | Terrible (Random I/O) | None | 2x memory footprint; cannot sort feeds by ID |
| **Snowflake (64-bit)** | **64-bit** | **Yes ($k$-sorted)** | **Excellent (Append-only)** | **None (Config `node_id`)** | **Requires monotonic NTP clock discipline** |

---

### 5. Problem & Solution Summary

* **Problem:** In PulseStream's high-throughput comment engine, storing millions of comments across partitioned tables requires a globally unique key that also serves as a **Clustering / Sort Key** so fetching the newest comments (`ORDER BY comment_id DESC`) requires zero secondary sorting indexes.
* **Solution:** Generate 64-bit Snowflake IDs at the stateless API tier (`src/distributed/snowflake.py`). Composite primary keys `(post_id, comment_id)` group all comments for a post onto the same partition (`post_id`) and automatically store them in chronological order on disk (`comment_id`).

---

### 6. Real-World Applications

* **Discord:** Stores trillions of chat messages in **ScyllaDB** using the composite primary key `(channel_id, message_id)`. Every `message_id` is a 64-bit Snowflake ID, allowing Discord to perform fast range scans for channel history and extract message timestamps directly on the client.
* **X (Twitter):** Created the original Snowflake service when migrating core tweet storage from monolithic MySQL to distributed Cassandra clusters.
* **Instagram:** Uses a modified 64-bit Snowflake layout inside horizontally sharded PostgreSQL schemas (41 bits timestamp, 13 bits logical shard ID, 10 bits table sequence).

---

---

## Part 2: Consistent Hashing Ring (`src/distributed/consistent_hash.py`)

### 1. What Is It?

**Consistent Hashing** is a distributed routing and sharding algorithm that maps both **storage nodes (shards)** and **data partition keys** onto a shared, circular 128-bit integer space ($0$ to $2^{128}-1$). To determine which database node owns a given record, the system hashes the record's partition key, locates that integer on the circle, and walks **clockwise** to the nearest node.

```text
                       0 / 2^128 - 1
                         [shard-A#0]
                        /           \
       key("post:99") -> *           [shard-B#0]
                      /                 \
              [shard-C#1]             [shard-A#1]
                      \                 /
                       \               * <- key("post:42")
                        \             /     (Routes clockwise to shard-B#1)
                         [shard-B#1]

```

---

### 2. Why Do We Need It?

In a distributed cache or sharded NoSQL database, we must deterministically route any `post_id` to the exact node storing its data—without consulting a centralized lookup directory on every request.

* **The Modulo Hashing Disaster (`hash(key) % N`):** If you route keys across $N = 3$ servers using `hash(key) % 3` and one server crashes (or you scale out to $N = 4$), the divisor changes. Mathematically, **$\frac{N}{N+1} = 75\%$ of all keys change their target server simultaneously!** In production, this triggers a **cache stampede** (75% of cached keys instantly miss and hammer the primary database) or requires terabytes of data to be moved across the network simultaneously.
* **The Consistent Hashing Guarantee:** By placing nodes on a fixed numeric ring, adding or removing a node only affects the segment of the ring immediately counter-clockwise to that node. Scaling from $N$ to $N+1$ nodes relocates only **$\approx \frac{1}{N+1}$** of keys (e.g., ~25% when going from 3 to 4 nodes), leaving 75% of the cluster completely untouched.

---

### 3. How It Works in PulseStream

1. **128-Bit Deterministic Hashing:** Keys and node identifiers are hashed via MD5 (`hashlib.md5`) and converted into 128-bit integers to guarantee uniform placement across the ring.
2. **Virtual Nodes (Vnodes) to Eliminate Skew:** If a cluster only has 3 physical servers, placing just 3 points on a circle produces wildly uneven arc lengths (one server might own 60% of the ring). PulseStream assigns `virtual_nodes = 100` labels per physical node (`shard-1#vnode0` ... `shard-1#vnode99`), interleaving 300 points around the ring so every physical server owns an almost identical $33.3\%$ share of keys.
3. **$O(\log V)$ Binary Search Routing:** All virtual node integer positions are maintained in a sorted array (`self.sorted_keys`). Looking up a key uses `bisect.bisect_right` to find the clockwise node in logarithmic time.
4. **Replication Preference List (`get_preference_list`):** To eliminate Single Points of Failure on any single shard, PulseStream walks clockwise from the key's initial position and collects `rf` (Replication Factor, e.g., $RF = 3$) **distinct physical nodes**, automatically wrapping around index `0` using modulo arithmetic (`(start_idx + i) % len(self.sorted_keys)`).

---

### 4. Comparison of Cluster Routing Architectures

| Routing Strategy | Keys Remapped on Scale ($3 \to 4$ Nodes) | Load Distribution Balance | Lookup Complexity | Fault-Tolerant Replication |
| --- | --- | --- | --- | --- |
| Modulo Sharding (`hash(k) % N`) | **~75%** (Catastrophic reshuffle) | High | $O(1)$ | None (Requires manual pairing) |
| Central Directory Lookup Table | **~25%** | Exact | $O(1)$ + Network hop | High (Directory is a SPOF) |
| Consistent Ring (1 Node = 1 Point) | **~25%** | Poor (High variance/hotspots) | $O(\log N)$ | Clockwise walk |
| **Consistent Ring + Virtual Nodes** | **~25% (Minimal movement)** | **Near-Perfect (<5% variance)** | **$O(\log(N \times V))$** | **Clockwise distinct-node walk ($RF$)** |

---

### 5. Problem & Solution Summary

* **Problem:** PulseStream must partition high-volume post and comment streams across multiple storage shards while supporting elastic horizontal scaling (adding/removing nodes under load) and replicating each partition across $RF = 3$ distinct nodes for fault tolerance.
* **Solution:** Implement `ConsistentHashRing` with configurable virtual nodes and a clockwise distinct-node selector (`get_preference_list(key, rf=3)`). When a node fails, requests automatically route to the remaining healthy replicas in the preference list, and adding a new shard migrates only $\frac{1}{N}$ of keys.

---

### 6. Real-World Applications

* **Apache Cassandra & ScyllaDB:** Use a consistent hashing token ring with virtual nodes (`num_tokens: 256`) where any node receiving a client request acts as the **Coordinator**, hashes the partition key, and forwards replicas to the top $RF$ clockwise physical nodes.
* **Amazon DynamoDB:** Uses consistent hashing across storage partitions paired with Quorum Consensus ($W + R > RF$) to deliver single-digit millisecond latency at planet scale.
* **Discord (Elixir Guild Routing) & Cloudflare CDN:** Route stateful WebSocket guild processes and edge HTTP cache lookups via consistent hash rings so server deployments and node restarts never drop unrelated user connections.

---

---

## Part 3: Quorum Consensus & CAP Theorem Simulator (`src/distributed/quorum_simulator.py`)

### 1. What Is It?

In a leaderless or multi-master distributed database (like Apache Cassandra, ScyllaDB, or Amazon DynamoDB), there is no single "Primary" node. Any node can accept a write or read request (acting as the **Coordinator**) and replicate the data across $RF$ (Replication Factor) nodes on the Consistent Hashing Ring. **Quorum Consensus** defines how many replica nodes must acknowledge a Write ($W$) and a Read ($R$) before the coordinator returns a success response to the client.

```text
               Replication Factor (RF = 3)
        +---------------------------------------+
        |  [Shard-1]    [Shard-2]    [Shard-3]  |
        |   (v2, t=9)    (v2, t=9)    (v1, t=1) |
        +---------------------------------------+
        |--- Write Quorum (W = 2) --->|         |
        |         |<--- Read Quorum (R = 2) ----|
                  +-------------------+
                     Overlap = Shard-2
              (Guarantees latest version v2!)

```

---

### 2. Why Do We Need It? (CAP Theorem in Practice)

The **CAP Theorem** states that in the presence of a **Network Partition ($P$)**—where some nodes are unreachable or delayed—a distributed data store must choose between:

* **Consistency ($CP$):** Every read receives the most recent write or an error (`QuorumNotMetError`).
* **Availability ($AP$):** Every request receives a non-error response, without the guarantee that it contains the most recent write (Eventual Consistency).

Instead of hardcoding the entire database as strictly $CP$ or strictly $AP$, Quorum Consensus lets engineers tune consistency **per query** using three integers ($RF$, $W$, and $R$):

* **Strong Consistency Formula ($W + R > RF$):** By the Pigeonhole Principle, if the number of nodes written to ($W$) plus the number of nodes read from ($R$) exceeds total replicas ($RF$), at least one node in the read set is mathematically guaranteed to hold the latest write. Since each write is tagged with a monotonically increasing **64-bit Snowflake ID** (or timestamp), the Coordinator simply compares the versions returned by the $R$ nodes and returns the one with the highest `version_id`.
* **Eventual Consistency ($W + R \le RF$):** Setting $W = 1, R = 1$ with $RF = 3$ minimizes latency and survives 2 simultaneous node failures ($AP$), but a read hitting the 3rd node before background replication finishes will return stale data.

---

### 3. How It Works in PulseStream

1. **Replica Selection via Ring:** Given a `key`, the Coordinator calls `self.ring.get_preference_list(key, rf=self.rf)` to identify the $RF$ physical nodes responsible for that partition.
2. **Versioned Writes (`write`):** Generates a new 64-bit Snowflake ID as the record's `version_id`. Attempts to write `{"value": value, "version_id": version_id}` to the $RF$ target nodes:
* Healthy nodes immediately store the new version (`synchronous` ack).
* Partitioned/down nodes are skipped, and nodes experiencing replication lag defer the write until background sync (`eventual` propagation).
* If fewer than $W$ nodes acknowledge the write, raises `QuorumError` ($CP$ write protection).


3. **Version-Reconciled Reads (`read`):** Queries healthy nodes from the $RF$ preference list until $R$ responses are collected:
* If fewer than $R$ nodes respond (due to network partitions), raises `QuorumError`.
* Compares the `version_id` (Snowflake ID) across all $R$ responses and returns the value with the **maximum `version_id**` (Last-Write-Wins / LWW conflict resolution).



---

### 4. Comparison of Quorum Configurations ($RF = 3$)

| Configuration | Formula ($W + R > RF$) | Consistency Guarantee | Write Fault Tolerance | Read Fault Tolerance | Ideal PulseStream Endpoint |
| --- | --- | --- | --- | --- | --- |
| `W=2, R=2` (`QUORUM`) | $2 + 2 = 4 > 3$ | **Strong ($CP$)** | Tolerates 1 dead node | Tolerates 1 dead node | Financial tips, account auth, moderation bans |
| `W=3, R=1` (`ALL/ONE`) | $3 + 1 = 4 > 3$ | **Strong ($CP$)** | 0 dead nodes (fragile writes) | Tolerates 2 dead nodes (ultra-fast reads) | Read-heavy config/profile metadata |
| `W=1, R=1` (`ONE/ONE`) | $1 + 1 = 2 \le 3$ | **Eventual ($AP$)** | Tolerates 2 dead nodes | Tolerates 2 dead nodes | High-throughput live comment feeds & view counters |

---

### 5. Problem & Solution Summary

* **Problem:** PulseStream handles both casual comment likes (where speed and availability matter more than a 200ms replication delay) and user account moderation/billing states (where reading stale data across nodes causes security bugs).
* **Solution:** Implement `QuorumCluster` on top of `ConsistentHashRing` and `SnowflakeGenerator`, allowing each controller action to specify `w` and `r` dynamically (`w=2, r=2` for strict consistency; `w=1, r=1` for low-latency eventual consistency).

---

### 6. Real-World Applications

* **Apache Cassandra & ScyllaDB:** Expose per-query consistency levels (`ONE`, `QUORUM`, `LOCAL_QUORUM`, `ALL`) using Last-Write-Wins timestamps and Read Repair.
* **Amazon DynamoDB:** Lets callers pass `ConsistentRead=True` ($R=\text{Quorum}$) or `ConsistentRead=False` ($R=1$, half the price and lower latency) on every `GetItem` API call.
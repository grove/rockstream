# RockStream: Massively-Parallel Incremental View Maintenance on SlateDB

A design for a horizontally-scalable, full-SQL incremental view maintenance (IVM)
system inspired by Feldera (DBSP), Materialize (Differential Dataflow), RisingWave,
and Snowflake Dynamic Tables — built on a mesh of SlateDB instances backed by
object storage.

> **Status**: Design v2 (supersedes v1). The v1 design had a single-writer
> bottleneck, no SQL compiler, no shuffle operator, and a scalar watermark
> protocol that could not correctly handle multi-input operators. This document
> addresses all of those.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Theoretical Foundation: DBSP & Differential Dataflow](#2-theoretical-foundation-dbsp--differential-dataflow)
3. [System Topology](#3-system-topology)
4. [SQL Compilation Pipeline](#4-sql-compilation-pipeline)
5. [Per-Shard SlateDB Storage Layout](#5-per-shard-slatedb-storage-layout)
6. [Operator Catalog & State Encodings](#6-operator-catalog--state-encodings)
7. [The Exchange (Shuffle) Subsystem](#7-the-exchange-shuffle-subsystem)
8. [Frontier Protocol & Progress Tracking](#8-frontier-protocol--progress-tracking)
9. [Atomic Epoch Commit Protocol](#9-atomic-epoch-commit-protocol)
10. [Elasticity: Adding, Removing, and Rebalancing Shards](#10-elasticity-adding-removing-and-rebalancing-shards)
11. [Fault Tolerance & Exactly-Once Semantics](#11-fault-tolerance--exactly-once-semantics)
12. [Query Serving](#12-query-serving)
13. [Connectors & External I/O](#13-connectors--external-io)
14. [Observability, Backpressure, Admission Control](#14-observability-backpressure-admission-control)
15. [Comparison to Prior Art](#15-comparison-to-prior-art)
16. [Appendix: Key Encoding Reference](#appendix-key-encoding-reference)

---

## 1. Design Principles

| # | Principle | Consequence |
|---|---|---|
| P1 | **Compute and storage are separated.** | Workers are stateless; all state lives in SlateDB. Workers can be added, removed, or replaced freely. |
| P2 | **Storage is sharded across many SlateDB databases.** | No global write bottleneck. Each shard is independent; SlateDB's single-writer constraint applies only within a shard. |
| P3 | **Exchanges are first-class operators.** | Re-partitioning between stages is an explicit dataflow node, not a hidden bulk transfer. The same primitive handles joins, group-bys, distincts, and recursion. |
| P4 | **Object storage is the universal substrate.** | State (SlateDB SSTs), shuffle payloads, checkpoints, and the WAL all live in S3/GCS/ABS. No node owns data exclusively. |
| P5 | **Frontiers, not watermarks.** | Progress is tracked as an antichain of timestamps per operator input, following Differential Dataflow. This handles multi-input operators and out-of-order data correctly. |
| P6 | **DBSP semantics.** | Every operator is a Z-set transformer. Updates are `(row, weight)` pairs. Negative weights express retractions. This gives mathematically provable equivalence with batch SQL. |
| P7 | **Full SQL via a real compiler.** | SQL → DataFusion logical plan → incremental physical plan (with explicit exchanges) → distributed plan → operator graph. No DSL, no Turing-incomplete subset. |
| P8 | **Exactly-once end-to-end.** | Source offsets and sink commits are integrated into the epoch commit protocol via a two-phase commit on connector state. |
| P9 | **Adaptive parallelism per operator.** | Different operators in the same query can run at different widths. A hot aggregation can use 100 shards while a small lookup uses 4. |
| P10 | **Idempotent everything.** | Every side effect — shuffle write, sink write, state mutation — is keyed so that replay is a no-op. |

---

## 2. Theoretical Foundation: DBSP & Differential Dataflow

We adopt the **DBSP** formalism (Budiu et al., VLDB 2023) for the operator semantics
and the **Differential Dataflow** progress model (Murray, McSherry et al., SOSP 2013)
for distributed coordination.

### Z-Sets

A **Z-set** is a multiset with integer multiplicities. Every collection in the system
— base tables, intermediate results, view outputs — is conceptually a Z-set. An
*insert* contributes `(row, +1)`; a *delete* contributes `(row, -1)`; an *update* is
`(old_row, -1)` plus `(new_row, +1)`. Aggregations sum weights group-wise.

### Incremental Operators

For every relational operator `f`, DBSP defines its incremental form `f^Δ` such that

```
f^Δ(C, ΔC) = f(C ⊎ ΔC) - f(C)
```

— it computes the change in output given the current collection `C` and a delta `ΔC`,
without re-reading all of `C`. The DBSP paper proves this works compositionally for
the entire relational algebra including recursion.

### Timestamps & Frontiers

Each Z-set entry carries a **logical timestamp** (a vector that can include epoch,
iteration count for recursion, and source-position metadata). A **frontier** is an
antichain of timestamps such that an operator promises not to emit any future updates
at timestamps `≤` any element of the frontier. Frontiers advance monotonically.

This is the only correct way to track progress through multi-input operators
(joins, unions, recursive queries). Materialize uses this; we use the same primitive.

### Recursion

Recursive queries (`WITH RECURSIVE`, transitive closure, graph algorithms) use
**semi-naive evaluation** inside a fixed-point loop. DBSP gives this a clean
formalization via the `I` (integrate) and `D` (differentiate) operators applied
in nested time scopes.

---

## 3. System Topology

```
                       ┌────────────────────────────┐
                       │   Control Plane (3 nodes)   │
                       │   (Raft / HA via SlateDB    │
                       │    catalog + DbReader fan-  │
                       │    out for query routing)   │
                       │                            │
                       │  • SQL compiler            │
                       │  • Plan optimizer          │
                       │  • Cluster topology        │
                       │  • Shard placement         │
                       │  • Frontier aggregator     │
                       │  • Connector orchestrator  │
                       └─────────┬──────────────────┘
                                 │ assignments
       ┌─────────────────────────┼─────────────────────────┐
       │                         │                         │
┌──────▼──────┐         ┌────────▼─────┐          ┌───────▼──────┐
│  Worker 0    │         │   Worker 1   │          │   Worker N   │
│              │ ◄─────► │              │ ◄──────► │              │
│ pin: shards  │ shuffle │ pin: shards  │  shuffle │ pin: shards  │
│  S0, S3, S6  │         │  S1, S4, S7  │          │  S2, S5, S8  │
└──┬─┬──┬──────┘         └──┬──┬──┬─────┘          └──┬──┬──┬─────┘
   │ │  │                   │  │  │                   │  │  │
   │ │  │                   │  │  │                   │  │  │   (one SlateDB
   ▼ ▼  ▼                   ▼  ▼  ▼                   ▼  ▼  ▼    instance per
 ┌──┐┌──┐┌──┐             ┌──┐┌──┐┌──┐             ┌──┐┌──┐┌──┐  shard; single-
 │S0││S3││S6│             │S1││S4││S7│             │S2││S5││S8│  writer rule
 └──┘└──┘└──┘             └──┘└──┘└──┘             └──┘└──┘└──┘  holds locally)

       Shared Object Storage (S3/GCS/ABS) — holds:
         • all SlateDB SSTs (per-shard prefixed)
         • all WAL files (per-shard)
         • all shuffle payloads (per-exchange / per-epoch)
         • all checkpoint manifests
         • all connector offset stores
```

### Three Logical Tiers

1. **Control plane** (≥3 nodes, HA).
   Stateless except for a dedicated **control SlateDB** holding the catalog,
   cluster membership, and shard-placement map. Compiles SQL, places shards
   on workers, aggregates frontiers, drives checkpoints.

2. **Worker plane** (elastic, N ≫ 1).
   Each worker hosts some number of **shards**. A shard is the unit of placement
   and writer-exclusivity. A worker process opens the SlateDB for each shard it
   owns as the sole writer. Other workers may open the same shard as readers
   (`DbReader`) for joins/lookups.

3. **Storage plane** (object storage).
   Object storage holds *all* durable state. Workers and the control plane have
   no local persistent state (modulo a small write-through cache).

### Why Shards (and not "one SlateDB")

SlateDB is single-writer per database. To exceed one writer's throughput we run
**many SlateDBs**. A *shard* is one SlateDB instance. The system is a mesh of
hundreds or thousands of shards. Each operator instance pins to a shard for its
state. Throughput scales linearly with shard count; latency stays flat because
each shard's working set is small.

### What a "Worker" Owns

A worker is a process (typically one per host or container). It:
- Runs the writer for each of its assigned shards.
- Hosts operator instances whose state lives on those shards.
- Maintains a network port for shuffle send/receive.
- Reports frontiers and metrics to the control plane.

---

## 4. SQL Compilation Pipeline

```
SQL text
   │
   ▼
[1] Parse + bind (sqlparser-rs + custom catalog binder)
   │
   ▼
[2] Logical plan (Apache DataFusion LogicalPlan)
   │
   ▼
[3] Rule-based optimizer (DataFusion's optimizer + custom IVM rules:
     predicate pushdown, projection pruning, constant folding,
     join reordering, subquery decorrelation)
   │
   ▼
[4] Incrementalization pass (SQL → DBSP):
     • Replace every relational op with its incremental form
     • Insert I/D operators around recursion blocks
     • Lower window functions to incremental windowed Z-sets
     • Lower aggregates to (group-key → partial state) maps
   │
   ▼
[5] Distribution pass:
     • Annotate every operator with its required input partitioning
     • Insert Exchange operators wherever partitioning differs
     • Assign per-operator parallelism width (cost-based)
   │
   ▼
[6] Physical plan (DAG of (Operator, parallelism, partition-key))
   │
   ▼
[7] Shard placement:
     • Map each operator instance to a shard
     • Reuse shards across operators where partition keys align
     • Co-locate operators that share state to avoid cross-shard reads
   │
   ▼
[8] Deployment:
     • Write the plan to the catalog
     • Push operator-instance assignments to workers
     • Workers materialize empty operator state on their SlateDB shards
     • Connectors start feeding source operators
```

### Why DataFusion (not Calcite)

- Rust-native: integrates directly with the rest of the codebase, no JVM.
- Mature SQL frontend with full ANSI coverage.
- Pluggable logical plan; we extend it with DBSP-specific physical nodes.
- Substrait support for cross-language tooling.
- Active community; used by InfluxDB IOx, Comet, Ballista, etc.

We borrow ideas (and possibly code) from Feldera's `sql-to-dbsp` for the
incrementalization pass — that compiler has the most complete coverage of SQL
semantics under incremental evaluation.

---

## 5. Per-Shard SlateDB Storage Layout

Each shard has its own SlateDB. Within a shard, we use a layout designed
specifically for the operator catalog and the shuffle subsystem.

### 5.1 Shard-Local Namespaces

```
Prefix  Namespace            Purpose
──────  ─────────────────    ──────────────────────────────────────────────────
0x01    op_state/            All operator state for operators placed on this shard
0x02    op_index/            Secondary indexes over op_state (sorted MIN/MAX, etc.)
0x03    view_output/         Materialized view outputs whose partition lives here
0x04    shuffle_inbox/       Incoming shuffle batches awaiting consumption
0x05    shuffle_outbox/      Outgoing shuffle batches awaiting upload/ack
0x06    shard_meta/          Per-shard frontiers, epoch markers, connector offsets
```

### 5.2 Control-Plane SlateDB

A small SlateDB cluster (one writer, ≥2 readers) holds:

```
0x01    catalog/             Tables, views, pipelines, schemas
0x02    plan/                Compiled physical plans, operator-instance assignments
0x03    topology/            Worker registry, shard placement, lease state
0x04    frontier/            Aggregated per-operator frontier (driven by workers)
0x05    checkpoints/         Cluster-wide checkpoint references
0x06    connector/           External-source offsets, sink commit state
```

Workers read this database (via `DbReader` pinned to fresh checkpoints) on
startup and subscribe to its CDC feed (`WalReader`) for plan changes and
topology updates. Writes to the control DB go through the control-plane leader.

---

## 6. Operator Catalog & State Encodings

Every operator instance has an `op_id` (16-byte ULID assigned by the compiler).
State keys begin with the op_id so different operators on the same shard never
collide.

### 6.1 Stateless Operators

`Map`, `Filter`, `Project`: no SlateDB state. Pure functions applied to incoming
deltas; output deltas forwarded to the next operator (possibly through an
Exchange).

### 6.2 Aggregation (`SUM`, `COUNT`, `AVG` decomposed to SUM/COUNT)

```
0x01 0xAG op_id(16) group_key(var) → partial_state_bytes
```

Updates use `db.merge()` with an associative `AggregateMergeOp`. Output deltas
are computed lazily: on read, finalize partial state, compare with last-emitted
value (kept in `op_index/`), emit `(old, -1), (new, +1)`.

### 6.3 Aggregation with Retractions: `MIN`, `MAX`, `MEDIAN`, `PERCENTILE`

These cannot use a pure merge operator because retraction (`weight = -1`) may
require knowing all current values. We maintain an indexed multiset:

```
0x01 0xMM op_id(16) group_key(var) value_bytes(var) row_hash(8) → i64 weight
0x02 0xMM op_id(16) group_key(var) → current_extremum
```

On insert with weight +1: scan to find new min/max; if changed, update the cached
extremum and emit a delta.
On delete with weight -1: if the deleted value was the extremum, scan the indexed
multiset (sorted by value within the group prefix) to find the new extremum.

### 6.4 Equi-Join

Two-sided join state, indexed by join key. The `Exchange` operator before each
side guarantees both inputs are partitioned by the join key, so each shard sees
matching pairs locally.

```
0x01 0xJL op_id(16) join_key(var) row_id(16) → row_bytes  (left arrangement)
0x01 0xJR op_id(16) join_key(var) row_id(16) → row_bytes  (right arrangement)
```

For each incoming left delta `(row_L, +1)`:
- Scan `0x01 0xJR op_id(16) join_key(L)..` for matching right rows.
- Emit `(row_L ⋈ row_R, +1)` for each match.
- Insert `(row_L, +1)` into the left arrangement.

Retractions handled symmetrically with -1.

### 6.5 Theta-Join / Cross-Join

Falls back to broadcast: one side is broadcast to all shards of the other side.
The compiler picks the smaller side. Broadcast happens via Exchange with target
list = `[all shards]`.

### 6.6 Distinct / Union (Set Semantics)

```
0x01 0xDS op_id(16) row_hash(16) → i64 weight
```

`MergeOperator` sums weights. Output emits delta when weight transitions
between zero and non-zero. Compaction filter drops weight-zero entries.

### 6.7 Window Functions (ROW_NUMBER, RANK, LAG, LEAD, sliding aggregates)

```
0x01 0xWN op_id(16) partition_key(var) order_key(var) row_id(16) → row_bytes
```

The order_key in the key gives natural ordering. For LAG/LEAD, scan the
neighboring entries. For sliding aggregates, maintain a segment tree per
partition; segment-tree nodes are stored under `op_index/`.

### 6.8 Recursion (`WITH RECURSIVE`, fixed points)

State for the recursive variable is stored normally. Iteration is driven by the
operator scheduler: each iteration produces new deltas that feed back as input
deltas at the next iteration timestamp. The frontier protocol naturally handles
the inner-time dimension (it's another component of the timestamp vector).

Convergence detection: iteration stops when the input frontier advances past
the iteration timestamp with no new deltas produced.

### 6.9 Time Windows (Tumbling, Hopping, Session)

```
0x01 0xTW op_id(16) window_id(16) key(var) → partial_state
```

`window_id` is computed from the event-time of the row. Window expiry uses
SlateDB **TTL** based on event-time-derived deadlines, combined with a
compaction filter that drops state past the watermark.

### 6.10 Top-K

Sorted index keyed by value-descending:

```
0x01 0xTK op_id(16) partition_key(var) value_desc_bytes(var) row_id(16) → row_bytes
```

`scan(prefix..).take(K)` returns the top-K. Maintenance is incremental:
insert/delete updates the index; if the change crosses the K-th boundary, emit a
delta replacing the displaced entry.

---

## 7. The Exchange (Shuffle) Subsystem

Exchange is the operator that re-partitions a stream from upstream's
partition key to downstream's partition key. It is the only mechanism that
crosses shard boundaries.

### 7.1 Partition Function

For partition key `k` and target width `W`:

```
target_shard = consistent_hash(k, W)
```

We use **rendezvous hashing** so that adding or removing one shard moves only
`1/W` of the keyspace.

### 7.2 Hybrid Transport: Direct + Object-Store

Each shuffle has two paths:

**Fast path (default)**: gRPC stream directly between worker processes. Low
latency (≈ network RTT), no object-store cost. Each batch is buffered in the
sender's `shuffle_outbox/` (in SlateDB on the sender's shard) until the
receiver ACKs.

**Durable path (fallback / recovery / large batches)**: sender uploads the batch
as an object to `s3://bucket/shuffle/{exchange_id}/{epoch}/{src_shard}/{seq}.arrow`.
Receiver polls / is notified of new objects and ingests them into its
`shuffle_inbox/`.

The fast path is used for small low-latency batches; the durable path is used
when the receiver is unreachable, when batches exceed a threshold, or as the
backup for fault tolerance. Either way, the canonical record is in object
storage — direct delivery is an optimization.

### 7.3 Outbox & Inbox Encoding

```
shuffle_outbox/ key:
  0x05 exchange_id(16) target_shard(4) epoch(8 BE) seq(8 BE)
  value: Arrow IPC batch (compressed)

shuffle_inbox/ key:
  0x04 exchange_id(16) src_shard(4) epoch(8 BE) seq(8 BE)
  value: Arrow IPC batch (compressed)
```

Entries are deleted only after the consuming operator's frontier advances past
the epoch (see §8).

### 7.4 Why Arrow

Arrow gives us:
- Columnar, vectorized in-memory format that operators can process at SIMD speed.
- Zero-copy slicing for sub-batches.
- IPC format that doubles as the wire and on-disk format.
- Native interop with DataFusion expression evaluation.

---

## 8. Frontier Protocol & Progress Tracking

### 8.1 Timestamp Type

A timestamp is a vector:

```
Timestamp {
  source_epoch: u64,   // monotonic epoch from ingestion
  iteration:    u32,   // for recursion; 0 outside recursive scopes
  sub_epoch:    u32,   // for nested scopes (windows, scoped recursion)
}
```

Ordering is product order: `t1 ≤ t2` iff every component of `t1` ≤ corresponding
component of `t2`. Two timestamps may be incomparable — hence we need antichains.

### 8.2 Frontier

A frontier is a minimal antichain of timestamps. An operator's *input frontier*
on a given input is the smallest antichain `F` such that no future delta on
that input will have a timestamp `t` with `t ≮ any f ∈ F`. The operator's
*output frontier* on a given output is derived from its input frontiers and its
operator-specific delay (most operators: identity; recursion: increment iteration).

### 8.3 Per-Shard Reporting

Every shard maintains, per operator instance and per output:

```
shard_meta/ key:
  0x06 0xFR op_id(16) output_port(2) → encoded_frontier (antichain bytes)
```

The shard's writer task periodically (e.g., every 10 ms or every epoch boundary)
flushes its current output frontier to its shard SlateDB and pushes a small
delta to the control plane.

### 8.4 Control-Plane Aggregation

The control plane subscribes via `WalReader` to each shard's `shard_meta/0x06 0xFR`
changes (cheap; one tiny write per operator per flush). It computes the
**cluster frontier** for each operator as the meet (greatest lower bound) of all
shard frontiers, and publishes:

```
control: frontier/op_id → cluster_frontier
```

Downstream operators read input frontiers from the control plane (cached &
subscribed). When the input frontier advances, the operator may:
- Release retained state (e.g., close a window).
- Compact garbage in its arrangements.
- Acknowledge upstream shuffle batches for deletion.

### 8.5 Garbage Collection of Shuffle Buffers

When a receiver operator's input frontier on exchange `E` advances past epoch
`e`, the receiver writes:

```
control: frontier/exchange_e/consumed → e
```

Senders observe this and **range-delete** all `shuffle_outbox/` entries with
`epoch ≤ e`. Receivers similarly range-delete their `shuffle_inbox/` entries.
This is exact (no TTL guessing).

---

## 9. Atomic Epoch Commit Protocol

Each operator instance commits its state changes for an epoch as a single
SlateDB `WriteBatch` on its shard. The batch includes:

1. All `op_state/` puts/deletes/merges produced by the operator.
2. All `op_index/` updates.
3. All `view_output/` puts/deletes if this is a leaf operator.
4. All `shuffle_outbox/` puts for batches that will be sent.
5. All `shuffle_inbox/` deletes for batches just consumed.
6. The new `shard_meta/0x06 0xFR` output frontier.

```rust
let mut batch = WriteBatch::new();

// state updates (using merge where associative)
for (k, delta) in agg_deltas    { batch.merge(k, delta); }
for (k, row)   in join_inserts  { batch.put(k, row); }
for k          in join_deletes  { batch.delete(k); }

// view outputs (for leaf operators)
for (k, v) in view_upserts { batch.put(k, v); }
for k       in view_deletes { batch.delete(k); }

// new shuffle outbox
for (k, batch_bytes) in outbox_writes { batch.put(k, batch_bytes); }

// consumed shuffle inbox cleanup
for k in inbox_acks { batch.delete(k); }

// frontier advance — included atomically so crash = retry same epoch
batch.put(frontier_key, encode_frontier(&new_output_frontier));

shard_db.write(batch).await?;
```

This is the **only** durability event per epoch per operator instance. SlateDB's
WAL guarantees atomicity. Recovery is automatic: on restart, the operator reads
its current frontier and processes inputs from that frontier forward — by
construction, idempotent.

---

## 10. Elasticity: Adding, Removing, and Rebalancing Shards

### 10.1 The Shard Map

The control plane holds a versioned **shard map** for each exchange:

```
control: topology/shard_map/exchange_id → ShardMap {
  version: u64,
  ring: Vec<(virtual_node_hash, shard_id)>,   // rendezvous ring
}
```

When a new shard is added, the control plane bumps the version and publishes
a new ring. Workers observe the version change and gracefully cut over at an
epoch boundary.

### 10.2 Adding a Shard

1. Provision a new SlateDB instance (just a path on object storage).
2. Assign it to a worker (capacity-based scheduling).
3. Compute the new shard map; identify keys that will move from existing shards
   to the new one (consistent hashing ⇒ small fraction).
4. For each existing shard losing keys:
   - Snapshot the relevant key range via SlateDB `Checkpoint`.
   - The new shard reads the range from the donor shard's checkpoint (via
     `DbReader`) and ingests into its own SlateDB.
   - Once caught up, the control plane atomically flips the shard map to the
     new version at a chosen epoch boundary.
5. After cutover, the donor shards range-delete the migrated keys.

### 10.3 Removing a Shard (Graceful)

Reverse of the above. The shard's keys are migrated to other shards via
checkpoint reads, then the SlateDB is decommissioned. SST GC will eventually
reclaim its object-store footprint.

### 10.4 Fault-Driven Reassignment

If a worker dies, its shards are re-leased to another worker. SlateDB's
single-writer enforcement (via the manifest fence epoch) prevents split-brain:
the old writer cannot commit after a new writer opens the same shard.

### 10.5 Per-Operator Parallelism

Operator parallelism is independent of the cluster's shard count. A small
aggregation might pin to 4 shards; a hot join might span 200 shards. The
compiler picks parallelism per operator based on:
- Estimated cardinality.
- Available cluster capacity.
- Historical execution statistics (collected via the observability stack).

Adaptive re-planning: if an operator's metrics show skew, the control plane can
re-shard that operator's state online while the rest of the pipeline keeps
running.

---

## 11. Fault Tolerance & Exactly-Once Semantics

### 11.1 The Three Boundaries

| Boundary | Mechanism |
|---|---|
| **Within an epoch on one operator** | `WriteBatch` is atomic. |
| **Across operators in the same cluster** | Frontier protocol + idempotent operator state keyed by source epoch. |
| **External sources & sinks** | Two-phase commit on connector state; sink writes are keyed by `(source_epoch, output_position)`. |

### 11.2 Cluster Checkpoints

Every `T` seconds (or every `N` epochs), the control plane runs a
**barrier-based** checkpoint inspired by Flink Chandy-Lamport:

1. Inject a checkpoint barrier into every source operator with a fresh
   `checkpoint_id`.
2. Barriers flow through the DAG, aligned at multi-input operators (the operator
   waits until the barrier arrives on all inputs).
3. When a barrier passes through an operator, that operator creates a SlateDB
   `Checkpoint` on its shard and records `(checkpoint_id, shard_checkpoint_id)`
   in the control plane.
4. When all operators have reported, the control plane commits the cluster
   checkpoint atomically: writes `control: checkpoints/{checkpoint_id}` with the
   full map of per-shard checkpoints.
5. Old cluster checkpoints (beyond the retention horizon) are released, allowing
   SlateDB GC to reclaim SSTs.

### 11.3 Recovery

To recover the cluster:
1. Pick the latest committed cluster checkpoint.
2. Open every shard's `DbReader` pinned to its recorded checkpoint.
3. Each worker brings up its assigned shards as writers, starting from the
   checkpointed state.
4. Source connectors resume from offsets recorded in `control: connector/`.
5. Frontiers held in the checkpoint resume; processing continues.

### 11.4 Exactly-Once Sinks

Sink connectors implement the standard two-phase commit:

```
Pre-commit (during epoch):
  - Stage outgoing rows in a sink-specific transactional buffer.
    For Kafka: producer transaction, no flush yet.
    For S3:    write to "_pending/{epoch}/..." path.
  - Stage atomically committed in the shard's WriteBatch via a
    sink_state/ entry recording the pending position.

Commit (after cluster checkpoint succeeds):
  - For Kafka: commit producer transaction.
  - For S3:    atomic rename _pending → final.
  - Update sink_state/ to mark epoch as committed.
```

Replay after crash: on recovery, the connector inspects `sink_state/`:
- If pre-committed but not committed: re-run commit (idempotent).
- If neither: epoch's data will be reproduced; nothing to do.

---

## 12. Query Serving

### 12.1 Three Query Modes

| Mode | Mechanism | Latency |
|---|---|---|
| **Materialized view lookup** | `DbReader` on the shard holding the view-output partition | µs–ms |
| **Materialized view range scan** | `scan()` on the relevant shard(s); merge results on the gateway | ms |
| **Ad-hoc SQL over views** | DataFusion query executes against a `Snapshot` of materialized views (no incremental engine involvement) | ms–s |

### 12.2 Query Gateway

A stateless query gateway service:
1. Parses incoming SQL.
2. Looks up which views satisfy the query (or rejects if none).
3. Routes range scans to the appropriate shards via `DbReader` connections.
4. Merges results with DataFusion's local executor.

Gateways are stateless and horizontally scalable. They pin to a recent cluster
checkpoint so concurrent queries see a consistent snapshot.

### 12.3 Subscribe / Streaming Queries

Clients can subscribe to a view's change stream. Implemented by tailing the
shard's `WalReader` filtered to `view_output/` for the requested view-id prefix.

---

## 13. Connectors & External I/O

### 13.1 Source Connectors

Each source operator is connected to an external system via a **source
connector** (Kafka, Postgres logical replication, S3 + manifest, HTTP webhook,
…). The connector:
- Reads from the external source.
- Decodes records into Z-set deltas.
- Assigns each delta a source epoch (typically the connector's native offset
  packed into the `source_epoch` field).
- Pushes deltas into the source operator (which lives on some shard).
- Updates `control: connector/{connector_id}/offset` atomically with the
  shard's commit.

### 13.2 Sink Connectors

Symmetric: a sink operator collects committed view-output deltas (from
`WalReader`), buffers them per the 2PC protocol in §11.4, and commits to the
external system after cluster checkpoints.

### 13.3 Connector Catalog

Connector types are pluggable. The control plane catalogs available connector
types and routes connector instances to workers. Connector processes are
independent of operator processes — they can be co-located or run as a separate
"connector tier" for isolation.

---

## 14. Observability, Backpressure, Admission Control

### 14.1 Metrics

Every shard and every operator instance reports:
- Throughput (rows/sec in & out).
- Latency (epoch processing time, end-to-end frontier lag).
- State size (bytes & rows in op_state).
- Shuffle traffic (bytes sent/received per exchange).
- Compaction stats.

Exported via Prometheus / OpenTelemetry.

### 14.2 Frontier Lag = Freshness

The cluster-wide frontier lag = `now - max(source_epoch_timestamp ≤ frontier)`.
This is the operational SLO: "how stale is the freshest queryable view?"

### 14.3 Backpressure

The exchange subsystem implements credit-based flow control:
- Receivers grant N credits per sender.
- Senders may have at most N unacked batches outstanding.
- When credits exhaust, the sender blocks, which propagates upstream naturally.

`shuffle_outbox/` size on a sender is itself a backpressure signal: if it grows
beyond a threshold, the upstream operator pauses (its `WriteBatch` includes a
wait on outbox-size).

### 14.4 Admission Control

The control plane refuses to start new pipelines if cluster utilization would
exceed thresholds. It can pause low-priority pipelines to free capacity for
high-priority ones (via a priority field in the pipeline config).

---

## 15. Comparison to Prior Art

| Aspect | Feldera | Materialize | RisingWave | Snowflake DT | **RockStream** |
|---|---|---|---|---|---|
| **SQL coverage** | Full ANSI + recursion | Full ANSI + recursion | Full ANSI | Subset (no recursion) | Full ANSI + recursion |
| **Theoretical model** | DBSP | Differential Dataflow | DBSP-like | Proprietary refresh | DBSP + DD frontiers |
| **State backend** | RocksDB (local NVMe) | LSM in-memory + S3 spill | Hummock (S3-native) | Internal | **SlateDB** (S3-native) |
| **Compute-storage split** | Tight | Tight | Decoupled | Decoupled | **Fully decoupled** |
| **Single-node baseline** | Excellent | Excellent | Good | N/A | Good |
| **Horizontal scale** | Limited (single-node focus) | Limited | Excellent | Excellent | **Excellent** |
| **Object-storage native** | No | Partial | Yes | Yes | **Yes (end-to-end)** |
| **Open source** | Yes | Yes | Yes | No | Yes |

The unique positioning: **end-to-end object-storage native** (no NVMe required,
no local-state assumptions) **+ full SQL via DBSP** (correctness guarantees) **+
adaptive per-operator parallelism**.

---

## Appendix: Key Encoding Reference

```
Per-shard SlateDB:
  op_state/agg:        0x01 0xAG op_id(16) group_key(var)
  op_state/minmax:     0x01 0xMM op_id(16) group_key(var) value(var) row_hash(8)
  op_state/join_L:     0x01 0xJL op_id(16) join_key(var) row_id(16)
  op_state/join_R:     0x01 0xJR op_id(16) join_key(var) row_id(16)
  op_state/distinct:   0x01 0xDS op_id(16) row_hash(16)
  op_state/window:     0x01 0xWN op_id(16) part_key(var) order_key(var) row_id(16)
  op_state/timewin:    0x01 0xTW op_id(16) window_id(16) key(var)
  op_state/topk:       0x01 0xTK op_id(16) part_key(var) value_desc(var) row_id(16)

  op_index/cached_extremum: 0x02 0xMM op_id(16) group_key(var)
  op_index/segtree:         0x02 0xST op_id(16) part_key(var) node_id(8)

  view_output:         0x03 view_id(16) output_key(var)

  shuffle_inbox:       0x04 exchange_id(16) src_shard(4) epoch(8 BE) seq(8 BE)
  shuffle_outbox:      0x05 exchange_id(16) target_shard(4) epoch(8 BE) seq(8 BE)

  shard_meta/frontier: 0x06 0xFR op_id(16) output_port(2 BE)
  shard_meta/sink:     0x06 0xSK connector_id(16) epoch(8 BE)
  shard_meta/epoch:    0x06 0xEP

Control-plane SlateDB:
  catalog/table:       0x01 0x01 table_id(16)
  catalog/view:        0x01 0x02 view_id(16)
  catalog/pipeline:    0x01 0x03 pipeline_id(16)

  plan/physical:       0x02 0x01 pipeline_id(16)
  plan/assignment:     0x02 0x02 pipeline_id(16) op_id(16) instance(4 BE)

  topology/worker:     0x03 0x01 worker_id(16)
  topology/shard:      0x03 0x02 shard_id(16)
  topology/shard_map:  0x03 0x03 exchange_id(16) → versioned ShardMap

  frontier/op:         0x04 op_id(16) output_port(2 BE)
  frontier/consumed:   0x04 0xEX exchange_id(16)

  checkpoints/cluster: 0x05 checkpoint_id(16)

  connector/offset:    0x06 0x01 connector_id(16)
  connector/sink:      0x06 0x02 connector_id(16)
```

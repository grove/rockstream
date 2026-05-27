# RockStream: Incremental View Maintenance on SlateDB

A design for a scalable IVM system in the spirit of Feldera (DBSP), RisingWave, and
Snowflake Dynamic Tables, using SlateDB as the sole durable storage layer.

---

## Table of Contents

1. [Background & Goals](#1-background--goals)
2. [SlateDB Features Used](#2-slatedb-features-used)
3. [Key-Space Layout](#3-key-space-layout)
4. [Storage Namespaces In Detail](#4-storage-namespaces-in-detail)
5. [System Architecture](#5-system-architecture)
6. [Worker Processing Loop](#6-worker-processing-loop)
7. [Atomicity & Consistency Guarantees](#7-atomicity--consistency-guarantees)
8. [Fault Tolerance & Checkpointing](#8-fault-tolerance--checkpointing)
9. [Scalability: Partitioned Workers](#9-scalability-partitioned-workers)
10. [Downstream Consumption via CDC](#10-downstream-consumption-via-cdc)
11. [SlateDB Feature→IVM Role Summary](#11-slatedb-featureivm-role-summary)

---

## 1. Background & Goals

**Incremental View Maintenance (IVM)** keeps materialized query results up-to-date by
processing only the *delta* (changes) of the input data rather than recomputing from
scratch. The theoretical foundation used here is **DBSP** (Feldera's model): queries
operate on *Z-sets* — multisets with integer weights — where +1 represents an insert
and -1 represents a delete. This lets every relational operator (filter, project, join,
aggregate, window) be expressed as an incremental streaming operator.

### Goals

- Use **SlateDB** as the only durable store (no separate state backend, no external
  coordinator database).
- Support the full operator set: filter, project, join, aggregation, distinct, window.
- Allow a **scalable pool of workers** that own disjoint partitions of the keyspace and
  process epochs concurrently.
- Provide **exactly-once semantics** and **crash recovery** via SlateDB checkpoints.
- Expose materialized view results as **sorted, range-scannable key-value pairs** for
  low-latency ad-hoc queries.
- Allow downstream systems to consume view changes via **CDC** on the WAL.

---

## 2. SlateDB Features Used

| SlateDB Feature | Why It's Needed |
|---|---|
| `WriteBatch` | Atomically commit operator-state + view-output + watermark in one epoch |
| `MergeOperator` | Lock-free partial aggregation (SUM, COUNT, append-to-join-index) |
| `DbTransaction` (SI / SSI) | Worker lease acquisition, partition assignment, coordinator elections |
| `DbSnapshot` | Consistent read-only view of outputs served to query clients |
| `Checkpoint` + `DbReader` | Pinned recovery point; read-only query serving without blocking writers |
| `Clone` | Branch a pipeline for blue/green, testing, or debugging |
| `WalReader` (CDC RFC-0019) | Stream view-output changes to downstream consumers |
| `scan()` range queries | Prefix scans over operator state and view outputs |
| `TTL` + compaction filters | Auto-expire old changelog entries and time-window state |
| `CompactionFilter` | Drop zero-weight Z-set tombstones during compaction |
| Sequence numbers | Causal ordering, idempotent replay after failure |
| Seq → timestamp mapping | Watermark management, event-time window semantics |

---

## 3. Key-Space Layout

SlateDB exposes a **single sorted byte-string keyspace**. Column families are
simulated via fixed-length key prefixes. All numeric components are encoded as
**big-endian fixed-width integers** so the LSM sort order equals numeric order,
enabling efficient range scans.

```
Prefix  Namespace            Key Structure
──────  ─────────────────    ──────────────────────────────────────────────────
0x01    catalog/             type_tag(1) / id(16 uuid)
0x02    changelog/           table_id(16) / epoch(8 u64 BE) / seq(8 u64 BE)
0x03    op_state/            op_type(1) / op_id(16) / partition(2 u16 BE) / subkey…
0x04    view_output/         view_id(16) / partition(2 u16 BE) / output_key…
0x05    worker_coord/        coord_type(1) / pipeline_id(16) / sub_id…
0x06    delta_buffer/        pipeline_id(16) / epoch(8) / partition(2) / op_id(16) / key…
```

All prefixes are disjoint, so a `scan(0x03..)` stays entirely within `op_state/`.

---

## 4. Storage Namespaces In Detail

### 4.1 `catalog/` — Schema & Pipeline Metadata

Stores the structural definitions of all tables, views, and pipelines.  
Written only when a pipeline is created or altered; read at startup by workers.

```
0x01 | 0x01 | table_id(16)     → Protobuf TableSchema
                                  { name, columns[], primary_key[], source_connector }

0x01 | 0x02 | view_id(16)      → Protobuf ViewDefinition
                                  { name, sql, operator_dag[], dependency_view_ids[] }

0x01 | 0x03 | pipeline_id(16)  → Protobuf PipelineConfig
                                  { partition_count, parallelism, checkpoint_interval_ms }
```

**SlateDB usage**: plain `put`/`get`. Transactions guard concurrent ALTER operations.

---

### 4.2 `changelog/` — Input Delta Log

Every change to every base table lands here, as a timestamped **Z-set entry**:
a `(key, weight)` pair where `weight = +1` (insert) or `-1` (delete).
Updates are represented as delete-old + insert-new.

```
0x02 | table_id(16) | epoch(8 BE) | seq(8 BE)  →  DeltaRecord
                                                    { key_bytes, row_bytes, weight: i8 }
```

- **epoch** is a monotonically increasing batch number assigned by the ingestion layer.
- **seq** is the SlateDB sequence number of the write, used for idempotency.
- Old epochs are deleted via **range deletion** + **TTL** after the global frontier
  advances past them (`db.delete_range(changelog_prefix(table, 0)..=changelog_prefix(table, committed_epoch))`).

**SlateDB usage**: `WriteBatch` (ingestion layer groups CDC events into atomic epoch batches).  
TTL set to `retention_duration` on each entry. `scan(0x02 | table_id | epoch..)` to read all
changes for a table in an epoch.

---

### 4.3 `op_state/` — Operator State

Persistent state for each stateful operator, sharded by `partition`. Workers scan only
their assigned partitions, so there is zero cross-worker contention.

#### 4.3.1 Aggregation State (SUM, COUNT, MIN/MAX per group)

```
0x03 | 0xAG | op_id(16) | partition(2) | group_key…  →  PartialAggregate
                                                          { sum: i128, count: i64, min: Bytes,
                                                            max: Bytes, ... }
```

Aggregation state is updated via **MergeOperator** — no read-modify-write cycle:

```rust
// Encoding: delta encoded as (sum_delta: i64, count_delta: i64)
db.merge(agg_key, &encode_agg_delta(+5, +1)).await?;

struct AggMergeOperator;
impl MergeOperator for AggMergeOperator {
    fn merge(&self, _key, existing, operand) -> Result<Bytes, _> {
        let acc  = existing.map(decode_agg).unwrap_or_default();
        let delta = decode_agg_delta(operand);
        Ok(encode_agg(acc + delta))
    }
}
```

MIN/MAX require a **sorted secondary index** within `op_state/` (see §4.3.4).

#### 4.3.2 Join Index State (one side of every join)

Each side of a binary join stores its rows indexed by the join key so the other side
can efficiently look up matches.

```
0x03 | 0xJL | op_id(16) | partition(2) | join_key… | row_id(8)  →  Row (left side)
0x03 | 0xJR | op_id(16) | partition(2) | join_key… | row_id(8)  →  Row (right side)
```

- `scan(join_left_prefix(op_id, partition, join_key)..)` returns all left rows for a
  given join key — this drives the nested-loop join for each delta row on the right.
- Rows are inserted/deleted via `WriteBatch` as part of the same epoch commit.

#### 4.3.3 Distinct / Deduplication State

Tracks how many times each row has been seen (Z-set weight). Rows with weight > 0
contribute to the output; reaching 0 removes them.

```
0x03 | 0xDS | op_id(16) | partition(2) | row_hash(8)  →  i64 (weight, via MergeOperator)
```

Compaction filter drops entries where `weight == 0`.

#### 4.3.4 Top-K / MIN / MAX Auxiliary Index

A sorted secondary index enabling efficient MIN/MAX computation without full scans:

```
0x03 | 0xTK | op_id(16) | partition(2) | group_key… | value_bytes(var) | row_id(8)  →  i64 weight
```

`scan(topk_prefix(op_id, partition, group_key)..)` gives the minimum value first,
allowing O(log N) MIN/MAX updates.

#### 4.3.5 Window State

Time-windowed rows, stored with **TTL** equal to the window duration so they expire
automatically without explicit cleanup.

```
0x03 | 0xWN | op_id(16) | partition(2) | window_end_ts(8 BE) | row_id(8)  →  Row
                                                               (TTL = window_duration)
```

`scan(window_prefix(op_id, partition, window_start)..window_prefix(op_id, partition, window_end))`
retrieves all rows in a window range for re-aggregation when a window fires or closes.

---

### 4.4 `view_output/` — Materialized View Results

The final query output. Updated atomically alongside `op_state/` in the same `WriteBatch`.
Keyed so that the natural LSM sort order matches the query's output ordering.

```
0x04 | view_id(16) | partition(2) | output_key…  →  OutputRow (encoded result row)
```

- `scan(0x04 | view_id | partition..)` serves point and range queries.
- `DbSnapshot` gives clients a consistent view without blocking ongoing epoch processing.
- `DbReader` (read-only, from a `Checkpoint`) serves query traffic on a separate process.

**Delta propagation**: when a downstream view depends on an upstream view, the upstream
view's output changes are the downstream view's changelog. The system reads the upstream
`view_output/` changes from the WAL via `WalReader` (CDC).

---

### 4.5 `worker_coord/` — Worker Coordination

Stores all cluster-coordination state. Written via **optimistic transactions**
(`IsolationLevel::SnapshotIsolation`) to prevent split-brain.

#### 4.5.1 Worker Leases

```
0x05 | 0xWL | pipeline_id(16) | worker_id(16)  →  WorkerLease
                                                    { expires_at_ms: u64, partitions: [u16],
                                                      last_heartbeat_seq: u64 }
```

Workers renew their lease every `lease_ttl / 3` ms. A coordinator steals expired leases
and re-assigns those partitions to healthy workers.

```rust
// Lease renewal with optimistic transaction
let txn = db.begin_transaction(IsolationLevel::SnapshotIsolation).await?;
let existing: WorkerLease = txn.get(lease_key).await?.expect("lease gone");
assert_eq!(existing.worker_id, my_worker_id, "lease stolen");
txn.put(lease_key, &encode_lease(refreshed_lease));
txn.commit().await?; // ConflictError → back-off and retry
```

#### 4.5.2 Watermarks

Each worker records the highest epoch it has fully committed for each of its partitions.

```
0x05 | 0xWM | pipeline_id(16) | partition(2)  →  u64 BE epoch
```

#### 4.5.3 Global Frontier

The minimum watermark across all partitions — the epoch up to which the entire view
is consistent and queryable.

```
0x05 | 0xGF | pipeline_id(16)  →  u64 BE epoch (frontier)
```

Updated by the coordinator after scanning all per-partition watermarks.

#### 4.5.4 Checkpoint References

```
0x05 | 0xCP | pipeline_id(16)  →  CheckpointRef { checkpoint_id: Uuid, frontier_epoch: u64 }
```

---

### 4.6 `delta_buffer/` — In-Flight Epoch Deltas (ephemeral)

Intermediate deltas computed by operators but not yet committed to `view_output/`.
Used when an operator graph has multiple levels (e.g., a view over a view); the
inner view's output delta is materialized here before being fed to the outer operator.

```
0x06 | pipeline_id(16) | epoch(8) | partition(2) | op_id(16) | key…  →  DeltaRecord
```

These entries are given a short **TTL** (e.g., `2 × epoch_interval`) so they are
automatically cleaned up by compaction even if an explicit delete is missed.

---

## 5. System Architecture

```
┌────────────────────────────────────────────────────────────┐
│                      Input Sources                         │
│           Kafka / PostgreSQL CDC / HTTP / S3               │
└──────────────────────┬─────────────────────────────────────┘
                       │ decoded change events
┌──────────────────────▼─────────────────────────────────────┐
│                   Ingestion Layer                           │
│  - groups events into epochs (micro-batches)               │
│  - encodes (row, weight) as DeltaRecord                    │
│  - writes changelog/ atomically via WriteBatch             │
│  - assigns monotonic epoch numbers                         │
└──────────────────────┬─────────────────────────────────────┘
                       │ SlateDB WAL / WalReader
┌──────────────────────▼─────────────────────────────────────┐
│                  Pipeline Coordinator                       │
│  - monitors worker leases in worker_coord/                 │
│  - re-assigns expired partitions via DbTransaction (SSI)   │
│  - advances global frontier (worker_coord/0xGF)            │
│  - triggers checkpoints every N epochs                     │
└──────┬───────────────┬─────────────────┬───────────────────┘
       │               │                 │   partition assignments
┌──────▼──────┐ ┌──────▼──────┐ ┌───────▼─────┐
│  Worker 0   │ │  Worker 1   │ │  Worker N   │
│ parts:0,3,6 │ │ parts:1,4,7 │ │ parts:2,5,8 │
│             │ │             │ │             │
│ reads:      │ │ reads:      │ │ reads:      │
│  changelog/ │ │  changelog/ │ │  changelog/ │
│  op_state/  │ │  op_state/  │ │  op_state/  │
│             │ │             │ │             │
│ writes:     │ │ writes:     │ │ writes:     │
│  op_state/  │ │  op_state/  │ │  op_state/  │
│  view_output│ │  view_output│ │  view_output│
│  watermark  │ │  watermark  │ │  watermark  │
└──────┬──────┘ └──────┬──────┘ └───────┬─────┘
       └───────────────┴─────────────────┘
                       │ all writes go to one logical SlateDB instance
┌──────────────────────▼─────────────────────────────────────┐
│              SlateDB  (object storage backed)               │
│                                                            │
│  catalog/       changelog/      op_state/                  │
│  view_output/   worker_coord/   delta_buffer/              │
│                                                            │
│  WAL ──► WalReader CDC ──► Downstream consumers           │
└────────────────────────────────────────────────────────────┘
```

**Deployment options**:

- *Single process*: Coordinator + N workers share one `Db` handle (in-process
  `Arc<Db>`). Suitable for a laptop or single-node deployment.
- *Multi-process*: Each worker is a separate OS process opening the same SlateDB path
  on shared object storage. The writer is one designated process; others use `DbReader`
  for read-only access. The Coordinator is a third process. This matches SlateDB's
  existing Writer + Compactor + GC separation.
- *Distributed compaction*: RFC-0025 (accepted) describes distributed compaction which
  allows the compactor to run as a separate scalable service — this offloads LSM
  compaction from worker CPU entirely.

---

## 6. Worker Processing Loop

```
for each assigned partition p:
  1. Refresh lease (DbTransaction/SSI)
  2. Read current watermark w  = worker_coord/watermark[pipeline_id][p]
  3. Collect input deltas:
       for each base table t in pipeline:
         scan(changelog/ | t | epoch=w+1 .. epoch=latest_ingested)
         → list of DeltaRecord[]
  4. Process operator DAG (topological order):
       for each operator op:
         a. Read relevant op_state/ entries for partition p
         b. Apply incremental operator logic → (new_op_state, output_deltas)
         c. If op has downstream operators: write output_deltas to delta_buffer/
         d. If op is a leaf (view output): accumulate (view_id, key, new_value) tuples
  5. Atomic epoch commit (single WriteBatch):
       for each modified op_state entry:   batch.put(op_state_key, new_state)
                                           OR batch.merge(agg_key, delta_bytes)
       for each view output change:        batch.put(view_output_key, row)
                                           OR batch.delete(view_output_key)
       watermark update:                   batch.put(watermark_key, (w+1).to_be_bytes())
       db.write(batch).await?;             // atomic, durable, WAL-backed
  6. Coordinator (async) scans all watermarks → advances global frontier
  7. Coordinator triggers Checkpoint every checkpoint_interval epochs
```

### Crash Recovery

On worker restart:
1. Read `worker_coord/watermark[pipeline_id][partition]` → last committed epoch `w`.
2. Re-process epoch `w+1` from `changelog/` (idempotent because the watermark only
   advances *inside* the `WriteBatch` — if the process died before the batch landed, the
   watermark is still at `w` and we replay correctly).
3. Renew lease before processing.

---

## 7. Atomicity & Consistency Guarantees

### Within an epoch: `WriteBatch`

All state changes for one epoch on one partition are submitted as a single `WriteBatch`.
SlateDB's WAL guarantees these writes are atomic: either all are durable or none are.

```rust
let mut batch = WriteBatch::new();

// operator state deltas
batch.merge(agg_state_key,  &encode_agg_delta(sum_delta, count_delta));
batch.put(join_index_key,   &encode_row(&new_row));
batch.delete(join_index_key_for_deleted_row);

// view output
batch.put(view_output_key,  &encode_output_row(&result));

// advance watermark — included atomically
batch.put(watermark_key,    &epoch.to_be_bytes());

db.write(batch).await?; // <- single WAL append
```

### Cross-worker coordination: `DbTransaction` (SSI)

Worker lease steal / renewal and partition re-assignment use
`IsolationLevel::Serializable` transactions to prevent two workers from claiming the
same partition simultaneously.

### Serving consistent snapshots: `DbSnapshot` / `DbReader`

Query clients open a `DbSnapshot` or a `DbReader` pinned to a `Checkpoint`.  
These provide point-in-time reads that are unaffected by concurrent epoch processing —
identical to Snapshot Isolation in a traditional OLTP database.

---

## 8. Fault Tolerance & Checkpointing

SlateDB's `Checkpoint` API pins a version of the manifest. No SSTs referenced by a
live checkpoint can be garbage-collected.

### Checkpoint Protocol

```
Coordinator every N epochs:
  1. Wait until global frontier >= target_epoch
  2. db.create_checkpoint(None).await?   → CheckpointCreateResult { id, manifest_id }
  3. WriteBatch:
       put(worker_coord/checkpoint/pipeline_id,
           CheckpointRef { checkpoint_id: id, frontier_epoch: frontier })
  4. Delete old checkpoint reference (GC will collect unreferenced SSTs)
```

### Worker Recovery from Checkpoint

```rust
// On startup or after crash:
let cp_ref: CheckpointRef = db.get(checkpoint_key).await?.unwrap();
let reader = DbReaderBuilder::new(path, object_store)
    .checkpoint(cp_ref.checkpoint_id)
    .build().await?;

// Replay changelog/ from cp_ref.frontier_epoch onward
// (changes already in view_output/ at checkpoint time are intact)
```

### Clone for Blue/Green Deployments

```rust
// Fork the entire pipeline state for a schema migration or A/B test:
let new_db_path = "/pipelines/v2";
db.create_clone(checkpoint_id, new_db_path, object_store.clone()).await?;
// New pipeline processes same base data independently, sharing immutable SSTs
```

---

## 9. Scalability: Partitioned Workers

### Partition Assignment

Base table rows are hash-partitioned by their primary key:
```
partition = hash(primary_key) % partition_count
```

All operator state keys include the partition number, so each worker's keyspace is
strictly disjoint. Workers never need distributed locks for their hot path.

### Adding Workers (Elastic Scale-Out)

1. New worker process starts and registers a lease with an empty partition list.
2. Coordinator detects under-loaded worker; transfers some partitions to the new worker
   via an SSI transaction (reads old lease, writes new assignment atomically).
3. The donating worker commits its in-flight epoch, then stops processing transferred
   partitions. The new worker starts from the last committed watermark for those
   partitions.
4. No data migration is needed — SlateDB's sorted keyspace already contains all
   partition data; the new worker simply starts scanning its assigned prefix ranges.

### Multiple SlateDB Instances (Extreme Scale)

For pipelines with very high write throughput, shard the keyspace across multiple
SlateDB instances (one per group of partitions). Each instance is an independent
object-storage-backed database. The `WalReader` CDC API is used to propagate
cross-shard joins and view dependencies.

---

## 10. Downstream Consumption via CDC

SlateDB's `WalReader` (RFC-0019) reads WAL files in order from object storage, exposing
a stream of raw key-value mutations. The `view_output/` prefix filter yields a clean
stream of view result changes:

```rust
let wal_reader = WalReader::open(db_path, object_store).await?;
let mut position = load_consumer_position(); // stored externally or in SlateDB

loop {
    for wal_file in wal_reader.list_after(position).await? {
        for entry in wal_file.iter().await? {
            if entry.key.starts_with(&VIEW_OUTPUT_PREFIX) {
                let (view_id, partition, output_key) = decode_view_key(&entry.key);
                match entry.value {
                    ValueDeletable::Value(v) => emit_upsert(view_id, output_key, v),
                    ValueDeletable::Tombstone   => emit_delete(view_id, output_key),
                }
            }
        }
        position = wal_file.id;
        save_consumer_position(position);
    }
    tokio::time::sleep(poll_interval).await;
}
```

This enables:
- **Kafka sink**: emit view output changes to a Kafka topic for downstream consumers.
- **Cache invalidation**: invalidate Redis / Memcached entries when their backing row changes.
- **Secondary index maintenance**: build inverted indexes over view outputs.
- **Nested pipelines**: one pipeline's `view_output/` is another pipeline's `changelog/`.

---

## 11. SlateDB Feature→IVM Role Summary

| SlateDB Feature | IVM Role | Namespace Used |
|---|---|---|
| `WriteBatch` | Atomic epoch commit (state + output + watermark) | all |
| `MergeOperator` | Lock-free partial aggregation; append-to-join-list | `op_state/agg`, `op_state/distinct` |
| `DbTransaction` (SSI) | Worker lease, partition re-assignment, coordinator election | `worker_coord/` |
| `DbSnapshot` | Consistent point-in-time reads for query clients | `view_output/` |
| `Checkpoint` | Durable epoch recovery point; SST pinning against GC | `worker_coord/checkpoint` |
| `DbReader` | Read-only query serving on separate process/node | `view_output/` |
| `Clone` | Blue/green pipeline deployments, schema migrations | top-level |
| `WalReader` (CDC) | Stream view-output deltas to downstream consumers | `view_output/` |
| `scan()` range queries | Join index lookup, window range, group aggregation | `op_state/`, `view_output/` |
| TTL on entries | Auto-expire time-window state, old changelog epochs | `op_state/window`, `changelog/` |
| `CompactionFilter` | Drop zero-weight Z-set entries at compaction time | `op_state/distinct` |
| Range deletion | Bulk-delete committed changelog epochs | `changelog/` |
| Sequence numbers | Causal ordering; idempotent epoch replay after crash | WAL |
| Seq→timestamp map | Event-time watermark management | `worker_coord/watermark` |
| Partitioned keyspace | Shard operator state across workers with no coordination | `op_state/`, `view_output/` |
| Distributed compaction (RFC-0025) | Offload LSM compaction from worker processes | background |

---

## Appendix: Key Encoding Reference

```
Notation: X(N) = field X encoded as N bytes, BE = big-endian.

catalog/table:     0x01 0x01 table_id(16)
catalog/view:      0x01 0x02 view_id(16)
catalog/pipeline:  0x01 0x03 pipeline_id(16)

changelog entry:   0x02 table_id(16) epoch(8 BE) seq(8 BE)

op_state/agg:      0x03 0xAG op_id(16) partition(2 BE) group_key(var)
op_state/join_L:   0x03 0xJL op_id(16) partition(2 BE) join_key(var) row_id(8 BE)
op_state/join_R:   0x03 0xJR op_id(16) partition(2 BE) join_key(var) row_id(8 BE)
op_state/distinct: 0x03 0xDS op_id(16) partition(2 BE) row_hash(8 BE)
op_state/topk:     0x03 0xTK op_id(16) partition(2 BE) group_key(var) value(var) row_id(8)
op_state/window:   0x03 0xWN op_id(16) partition(2 BE) window_end_ts(8 BE) row_id(8 BE)

view_output:       0x04 view_id(16) partition(2 BE) output_key(var)

coord/lease:       0x05 0xWL pipeline_id(16) worker_id(16)
coord/watermark:   0x05 0xWM pipeline_id(16) partition(2 BE)
coord/frontier:    0x05 0xGF pipeline_id(16)
coord/checkpoint:  0x05 0xCP pipeline_id(16)

delta_buffer:      0x06 pipeline_id(16) epoch(8 BE) partition(2 BE) op_id(16) key(var)
```

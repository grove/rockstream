# RockStream: Massively-Parallel Incremental View Maintenance on SlateDB

A design for a horizontally-scalable, full-SQL incremental view maintenance (IVM)
system inspired by Feldera (DBSP), Materialize (Differential Dataflow), RisingWave,
and Snowflake Dynamic Tables — built on a mesh of SlateDB instances backed by
object storage.

> **Status**: Design v3.6. v3 reframed the engine around DBSP-native operators
> with pg_trickle as a correctness oracle and SlateDB's real API surface as a
> hard constraint. v3.1 added causal time, async scheduling, and explicit
> SlateDB operational budgets. v3.2 added the operability foundation
> (deployment ladder, `EXPLAIN INCREMENTAL`, frontier-lag diagnostics, four
> knobs). **v3.3 raises operability to a first-class system property**:
>
> 1. **SLO-driven, not knob-driven.** Operators state a freshness target;
>    the system tunes epoch sizing, parallelism, and scheduling to meet it.
>    Manual knobs remain as overrides, not primary controls.
> 2. **Self-tuning by default.** Adaptive parallelism, adaptive epoch
>    sizing, and adaptive scheduling are on out of the box. Operators set
>    intent (SLOs, quotas, priorities); the control plane decides
>    mechanism.
> 3. **One binary, one CLI, one config.** Every node role is a flag on the
>    same `rockstream` binary. The CLI surface is pipelines and views, never
>    shards or antichains.
> 4. **Unable to surprise you.** Cost preview before deploy
>    (`EXPLAIN INCREMENTAL ESTIMATE`), enforced per-pipeline quotas, an
>    auditable event log of every control action, a single-command support
>    bundle, and a documented error-code taxonomy. Pipelines that cannot
>    meet their SLO degrade with a named reason instead of failing
>    silently.
>
> Carrying forward from earlier revisions:
> 1. **Time is causal, not global** (P13).
> 2. **Scheduling is async** (P14).
> 3. **SlateDB is respected as-is** (P12).
>
> **v3.4 is a coherence and hardening pass.** It resolves the control-plane
> HA contradiction, specifies merge-backed read semantics, outer-join state,
> stable row identity, schema evolution, connector contracts, bounded barrier
> alignment, shard-level checkpointing, shuffle fan-out limits, query freshness
> tokens, backfill/recovery lifecycle states, and the production storage
> profiles needed for predictable operation.
>
> **v3.5 fills the remaining design gaps**: authentication and authorization,
> source statistics and cost-model inputs for `EXPLAIN ESTIMATE`, cluster
> bootstrap and worker discovery, storage format versioning and rolling
> upgrades, late-data policy connected to the frontier model, fault-driven
> shard-state recovery path, the adaptive scheduling loop replaced with a
> concrete source-rate throttle loop, and multi-region added as an explicit
> non-goal.
>
> **v3.6 adds Postgres wire protocol compatibility**: pgwire gateway layer,
> `READ COMMITTED` / `REPEATABLE READ` isolation via the vector-frontier model,
> `pg_catalog` / `information_schema` stubs for ORM compatibility, Postgres type
> OID mapping, and an internal source connector so clients can write rows
> directly without an external Kafka or Postgres. `SERIALIZABLE` is explicitly
> out of scope (§1.1, §12.6). Positioning: same tier as Materialize / RisingWave,
> not a Neon-style Postgres drop-in.
>
> **Companion documents**:
> - [IVM.md](IVM.md) — deep design of the incremental-view-maintenance engine
>   itself (PlanIR, the differentiation pass, the per-operator rules, the
>   circuit runtime, arrangements on SlateDB). This DESIGN document tells you
>   *what* the system is; IVM.md tells you *how the IVM core works*.
> - [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) — phased build plan that
>   operationalizes both.

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
14. [Operations: Deploy, Monitor, Diagnose](#14-operations-deploy-monitor-diagnose)
15. [Comparison to Prior Art](#15-comparison-to-prior-art)
16. [Optimality Assessment (v3.4)](#16-optimality-assessment-v34)
17. [Appendix: Key Encoding Reference](#appendix-key-encoding-reference)

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
| P11 | **DBSP is the runtime truth.** | Native DBSP-style operators define behavior. pg_trickle's SQL delta engine is used as an oracle and regression corpus, not copied blindly as runtime machinery. |
| P12 | **Respect SlateDB's real constraints.** | We rely on supported features (single-writer fencing, WriteBatch, DbReader, checkpoints, merge operators, TTL, compaction filters, WAL reader, segments) and do not assume missing APIs such as range deletion. |
| P13 | **Causal time.** | Progress is an antichain over `(shard_id, source_epoch)` pairs. There is no global LSN. The cluster frontier is computed asynchronously from per-shard frontiers and is allowed to lag by a bounded budget. |
| P14 | **Async scheduling.** | Operators are long-lived async tasks. There is no synchronous global scheduler tick and no per-stream ownership checker. Backpressure flows via credits; progress flows via frontiers. |
| P15 | **Bounded staleness for cross-shard reads.** | Query gateways pin to a published cluster frontier (a vector of per-shard checkpoints) rather than to wall-clock "fresh"; the staleness budget is documented and observable. |
| P16 | **Operability is a first-class system property.** | The system is SLO-driven (operators state intent; the control plane chooses mechanism), self-tuning by default, deployable as a single binary, observable by construction, and unable to surprise its operator: every degradation has a named reason, every control action is auditable, and every pipeline runs inside enforced quotas. One number (frontier lag against the SLO) answers "is it healthy?"; everything else is a drill-down. |

### 1.1 Non-Goals (Explicit)

The following are intentionally **out of scope** because they conflict with
horizontal scale on object storage, and the v3.1 review confirmed that
attempting them would compromise the rest of the design:

- **Distributed IMMEDIATE / synchronous IVM.** pg_trickle's IMMEDIATE mode
  takes table-level locks and runs inside one PostgreSQL transaction; it does
  not generalize to a sharded cluster. RockStream's default is deferred,
  low-latency epochs. A restricted IMMEDIATE mode may exist for single-shard
  scan chains, but cluster-wide synchronous IVM is not a goal.
- **Global linearizable snapshots across all shards in the hot path.** Reads
  see a causally consistent vector frontier, not a single global LSN. Queries
  that demand global linearizability must opt in to a cluster checkpoint and
  accept higher latency.
- **SERIALIZABLE isolation via cross-shard conflict detection.** True
  `SERIALIZABLE` requires tracking read-write conflicts across shards via a
  global conflict detector or per-shard SILock tables. This needs a global write
  sequence number, which is an explicit non-goal (see below). `READ COMMITTED`
  and `REPEATABLE READ` are fully supported by the existing vector-frontier
  model (§12.6) and cover the vast majority of analytical and streaming
  workloads. SERIALIZABLE is available only within a single shard as a future
  extension.
- **A global write sequence number.** SlateDB's per-DB sequence is local. We
  do not synthesize a cluster-wide sequence on top of it.
- **Loading or linking pg_trickle / Feldera at runtime.** Neither is a Cargo
  dependency. They are reference material and test oracles only.
- **Active-active multi-region writes.** The single-writer fence per shard is
  a hard constraint against concurrent writers in different regions. Multi-region
  active-passive (read replicas via `DbReader` on a cross-region object-store
  bucket) is future work, not v1.
- **Per-query cost accounting ($/query) in the hot path.** Cost visibility in
  `EXPLAIN ESTIMATE` is a design goal; per-query billing middleware and
  chargeback to tenants is an application-layer concern out of scope.

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

Feldera's `IterativeCircuit` (`crates/dbsp/src/operator/recursive.rs`) is
**local to one circuit on one worker**. RockStream lifts this into the
distributed setting by allowing `Exchange` operators inside a recursive scope
so the inner-time iteration can re-partition data each round. The iteration
frontier (`Timestamp::iteration`) participates in the same antichain protocol
as the outer source epoch. Cross-shard convergence is detected when the
iteration component of the cluster frontier stops advancing, not via a
synchronous barrier.

---

## 3. System Topology

```
                       ┌────────────────────────────┐
                       │Control Plane (1/3/5 nodes) │
                       │   (single writer in dev;    │
                       │    Raft-elected writer in   │
                       │    production; DbReader fan-│
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

1. **Control plane** (1 node in Tier 1/2; 3 or 5 nodes in Tier 3).
  The durable control state is a dedicated **control SlateDB** holding the
  catalog, cluster membership, shard-placement map, audit log, and checkpoint
  index. In Tier 3, control nodes form a small Raft group that elects exactly
  one control writer lease for that SlateDB. Followers serve reads via
  `DbReader`, replay the control WAL, and can take over the writer lease after
  Raft leadership changes. Raft protects *control-plane leadership and leases*;
  SlateDB remains the storage engine for the catalog. This removes any hidden
  dependency on a global data-plane transaction manager.

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
state. Throughput scales with shard count for partitionable workloads; the real
limits are hot keys, shuffle fan-out, object-store request rates, compaction
debt, and external source/sink throughput.

### What a "Worker" Owns

A worker is a process (typically one per host or container). It:
- Runs the writer for each of its assigned shards.
- Hosts operator instances whose state lives on those shards.
- Maintains a network port for shuffle send/receive.
- Reports frontiers and metrics to the control plane.

### Cluster Bootstrap and Worker Discovery

The control-plane address is passed as `--control=<url>` (or `ROCKSTREAM_CONTROL_URL`
in the environment). Workers join by calling `control.register(worker_id, addr,
capacity)` on startup; the control plane adds them to `topology/worker/` and begins
assigning shards. No pre-configured member list is required beyond the control URL.

For Tier 3 Raft bootstrap, the first control node starts with
`rockstream start --role=control --bootstrap --storage=s3://...`. Subsequent
control nodes start without `--bootstrap` and join the Raft group via the same
control URL. Once a quorum of control nodes is formed, the Raft leader opens the
control SlateDB for writing and the cluster is ready to accept workers.

Network security: all inter-node gRPC (worker↔control, worker↔worker, gateway↔worker)
and the public SQL port must use **mutual TLS (mTLS)**. Certificate rotation is handled
out-of-band; the control plane enforces that every worker presents a valid certificate
before being admitted into `topology/worker/`.

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

### 4.0 Source Statistics and Cost-Model Inputs

Step [5] (distribution pass) and `EXPLAIN INCREMENTAL ESTIMATE` both require
cardinality estimates. The planner obtains them in priority order:

1. **Connector-reported statistics**: connectors expose `discover_stats()`
   returning `{row_count, avg_row_bytes, key_cardinality, update_rate_per_s}`
   after `discover_schema()`. Kafka connectors count committed offsets; Postgres
   CDC connectors read `pg_class.reltuples`; S3/Parquet connectors read footer
   metadata.
2. **Cached catalog statistics**: the control plane stores the last-known stats
   in `control: catalog/table/{id}/stats`, refreshed on each connector attach
   and on `ANALYZE TABLE <name>`.
3. **Heuristic fallback**: when neither is available, the planner uses
   configurable defaults (`default_row_count = 1_000_000`,
   `default_update_rate = 1000/s`) and marks the estimate as
   `confidence=low` in `EXPLAIN INCREMENTAL ESTIMATE` output.

Estimate accuracy is tracked over time: after 60 s of operation the real metrics
feed back into the catalog stats, and `EXPLAIN INCREMENTAL` shows both the
original estimate and the observed values side-by-side.

### Why DataFusion (not Calcite)

- Rust-native: integrates directly with the rest of the codebase, no JVM.
- Mature SQL frontend with full ANSI coverage.
- Pluggable logical plan; we extend it with DBSP-specific physical nodes.
- Substrait support for cross-language tooling.
- Active community; used by InfluxDB IOx, Comet, Ballista, etc.

We use Feldera's `sql-to-dbsp` as the reference for SQL-to-DBSP semantics and
pg_trickle as the reference for concrete SQL edge cases. The RockStream runtime
does not execute generated SQL and does not copy pg_trickle's CTEs into the hot path.
It compiles SQL into native DBSP-style operators and validates their behavior
against batch DataFusion, PostgreSQL, and pg_trickle test oracles.

### 4.1 The Differentiation Pass & PlanIR

The incrementalization step (4) and operator runtime are specified in detail in
[IVM.md](IVM.md). In summary:

- The logical plan is lowered into **PlanIR** — an explicit enum with one
  variant per operator (Scan, Filter, Project, InnerJoin, Aggregate, Distinct,
  Window, TopK, TimeWindow, Recursive, Exchange, …). PlanIR is modelled on
  pg_trickle's `OpTree`.
- A **`DiffCtx`** walks PlanIR and emits a runtime operator graph (`OpNode`s).
  The operator semantics are DBSP-native. pg_trickle's `diff_*` functions are
  used to identify edge cases and build regression tests: the EC-01 join fix,
  Q07 double-counting correction, Q21 SemiJoin correction, FULL JOIN NULL
  handling in SUM, recursive DRed fallback, and similar cases.
- The runtime is a **long-lived circuit of typed operators** (Feldera's model)
  rather than per-epoch SQL re-execution (pg_trickle's model). Each operator
  is a long-lived async task that consumes `RecordBatch` deltas and maintains
  one or more **arrangements** (indexed Z-sets) on its assigned SlateDB shard.
- We do not use generated Rust artifacts as the v1 deployment model. Instead,
  workers interpret a fixed physical plan; the vectorized `RecordBatch` inner
  loop is fast enough via DataFusion's expression executor. Code generation can
  be added later for hot paths without changing semantics.

See [IVM.md §4–7](IVM.md#4-the-rockstream-ivm-architecture) for the full
operator catalogue, runtime trait, and per-operator rules.

### 4.2 Schema Evolution and Plan Replacement

Every source, view, and pipeline carries an explicit schema version. Connectors
publish schemas into `control: schema/` before they emit data, and every
`RecordBatch` delta carries the schema version used to decode it.

Schema changes are classified before they reach the runtime:

| Change | Handling |
|---|---|
| Add nullable column or add column with default | Compatible. Existing arrangements keep their old row encoding; readers project the new column as NULL/default until rewritten by fresh deltas. |
| Widen numeric type or compatible string/binary widening | Compatible if DataFusion can cast losslessly; recorded as a schema-version edge. |
| Rename, drop, narrow, or change join/group/window key type | Breaking. Requires `CREATE PIPELINE ... REPLACE` or `ALTER PIPELINE ... REPLACE VIEW`, producing a blue/green plan clone (§10.5). |
| Connector reports unexpected incompatible schema | Pipeline transitions to `BLOCKED(RS-1002)` and stops consuming new offsets until the operator approves a migration. |

Online replacement uses a checkpoint/clone path: create the new plan at a
published frontier, backfill only state whose encoding changed, run old and new
plans in parallel until the new plan reaches the old frontier, then flip query
routing atomically in the catalog. This is the default mechanism for `ALTER
VIEW`, join-key changes, and breaking source-schema updates.

---

## 5. Per-Shard SlateDB Storage Layout

Each shard has its own SlateDB. Within a shard, we use a layout designed
specifically for the operator catalog and the shuffle subsystem.

When a shard is created, RockStream configures a SlateDB segment extractor that
uses the namespace + arrangement prefix. This lets SlateDB's segment-aware LSM
layout isolate operator/shuffle/view state inside one shard without requiring a
separate SlateDB database per operator. The segment extractor is immutable after
database creation, so the prefix scheme is part of the storage format contract.

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

A dedicated control SlateDB (one writer, >=2 readers in Tier 3) holds:

```
0x01    catalog/             Tables, views, pipelines, schemas
0x02    plan/                Compiled physical plans, operator-instance assignments
0x03    topology/            Worker registry, shard placement, lease state
0x04    frontier/            Aggregated per-operator frontier (driven by workers)
0x05    checkpoints/         Cluster-wide checkpoint references
0x06    connector/           External-source offsets, sink commit state
0x07    audit/               Durable event log for every control-plane action
0x08    state_accounting/    Per-pipeline state bytes, shard count, quota usage
0x09    schema/              Versioned source/view schemas and compatibility data
```

Workers read this database (via `DbReader` pinned to fresh checkpoints) on
startup and subscribe to its CDC feed (`WalReader`) for plan changes and
topology updates. Writes to the control DB go through the control-plane leader;
in Tier 3, that leader must hold the current Raft-issued writer lease before it
can open the control SlateDB for writing.

### 5.3 SlateDB API Constraints Used by This Design

The design assumes the following SlateDB features because they exist in the
current implementation: single-writer fencing, `WriteBatch`, `DbReader`, MVCC
snapshots, transactions, checkpoints/clones, merge operators, TTL, compaction
filters, WAL reader, and segment extractors.

The design deliberately does **not** assume range deletion or database
split/merge APIs. Cleanup and rebalancing therefore use one of three patterns:

- **Scan-and-delete** for bounded key ranges and correctness-sensitive cleanup.
- **Frontier-aware compaction filters** for retention where dropping old entries
  cannot make older values visible again.
- **Checkpoint/clone/projection** for large shard movement or blue/green plan
  replacement.

Compaction filters are never treated as a correctness shortcut. SlateDB warns
that filters can affect snapshot consistency and that dropping entries can
resurrect older versions. RockStream uses explicit deletes for zero-crossing
state transitions and reserves compaction filters for retention after the
frontier proves no reader can observe the removed versions.

### 5.4 SlateDB Operational Budgets

These SlateDB realities are first-class budget items in this design rather than
things to discover at runtime:

- **WAL listing is expensive at high retention.** `WalReader` documents that
  listing thousands of WAL files is costly. Every worker keeps a per-shard
  WAL listing cache, invalidated only on WAL rotation, and tails via
  `WalReader::get(latest_id + 1)` rather than repeated `list()`.
- **Manifest writes are not free.** Every flush/compaction/GC updates the
  manifest, which is an object-store write. Epoch sizes have a configurable
  minimum (`min_epoch_ms`, `min_epoch_bytes`) so manifest churn stays bounded
  even when source rate spikes. `manifest_poll_interval` on readers is tuned
  to match the cluster's frontier-staleness budget.
- **Merge operators must be associative.** SlateDB does not verify
  associativity. RockStream registers only operators whose associativity is
  proved by construction (integer add for weights, `(sum, count)` tuples for
  algebraic aggregates). MIN/MAX/Top-K/window/recursive state is maintained
  as explicit sorted arrangements, not merge operands.
- **Compaction-filter snapshot consistency** is preserved by gating any `Drop`
  decision on the per-shard checkpoint frontier; a filter never drops data
  that an active `DbReader` snapshot could observe.
- **`DbReader` is the cross-worker read path.** Joins and lookups that read
  state owned by another shard use `DbReader` pinned to a published
  checkpoint, never an undefined "live" read.

### 5.5 Storage Format Versioning and Rolling Upgrades

Every shard carries a one-byte format version at a fixed key
(`shard_meta/0x06 0xFV`). The current format is **version 1**.

Compatibility rules:
- A binary that supports format versions `[min, max]` will refuse to open a
  shard whose stored version is outside that range, printing `RS-5001`.
- New binaries must support at least the previous format version to enable
  rolling upgrades (one worker restarted at a time).
- Breaking format changes require a bump to the version and a migration tool
  (`rockstream migrate --from=N --to=M --storage=s3://...`) that writes the new
  format offline before the new binary is deployed.

Rolling upgrade procedure: (1) deploy new binary to one worker; (2) verify it
acquires its shards and processes epochs; (3) roll forward. The control plane
rejects shard-lease acquisition from a binary whose supported version range does
not overlap the shards' stored version, preventing silent data corruption.

### 5.6 Storage Profiles and Autotuner Defaults

The same binary supports two storage profiles with different default budgets:

| Profile | Used by | Default tuning |
|---|---|---|
| `local_fs` | Tier 1 (`rockstream start --storage=./data`) | Low-latency epochs, aggressive flush cadence, small local cache. Intended to feel like an embedded database on a laptop. |
| `object_store` | Tier 2/3 (`s3://`, `gs://`, `az://`) | Larger `min_epoch_ms`, request-rate budget enforcement, WAL listing cache, coalesced shuffle objects. Intended to minimize PUT/LIST/manifest churn. |

The control plane detects the profile from the storage URL and seeds the
auto-tuner with the correct latency/cost assumptions. Operators can still
override SLOs and quotas, but they should not have to know whether a dev laptop
or a 1,000-shard S3 cluster is underneath them.

---

## 6. Operator Catalog & State Encodings

Every operator instance has an `op_id` (16-byte ULID assigned by the compiler).
State keys begin with the op_id so different operators on the same shard never
collide. The state behind each operator is an **arrangement** — an indexed,
sorted Z-set whose key is the lookup column(s) for the operator. Arrangements
are specified in [IVM.md §9](IVM.md#9-arrangements-state-on-slatedb).

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

Operators never read merge-backed keys through raw SlateDB `get()`. They read
through `ShardDb::get_merged()` / `ShardDb::scan_merged()`, which resolve the
base value plus all visible merge operands at the epoch snapshot being used.
If a future SlateDB API cannot provide read-path merge resolution for a storage
profile, RockStream falls back to a batched read-modify-write for that
arrangement kind and marks the profile as lower throughput in `EXPLAIN
INCREMENTAL ESTIMATE`. This makes merge laziness a performance choice, not a
correctness assumption.

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

`row_id` is a stable 128-bit identity assigned by the source connector, never a
fresh random value at replay time. For log sources it is derived from
`(source_id, partition, offset, row_ordinal)`; for CDC sources from the table
primary key plus source LSN; for keyless snapshots from
`(snapshot_id, file_path, row_group, row_ordinal)`. Idempotent replay therefore
rewrites the same arrangement key instead of duplicating rows.

```
0x01 0xJL op_id(16) join_key(var) row_id(16) → row_bytes  (left arrangement)
0x01 0xJR op_id(16) join_key(var) row_id(16) → row_bytes  (right arrangement)
```

For each incoming left delta `(row_L, +1)`:
- Scan `0x01 0xJR op_id(16) join_key(L)..` for matching right rows.
- Emit `(row_L ⋈ row_R, +1)` for each match.
- Insert `(row_L, +1)` into the left arrangement.

Retractions handled symmetrically with -1.

### 6.4.1 Outer Join Match Accounting

LEFT, RIGHT, and FULL OUTER JOIN need explicit match-count state so null-padded
rows flip correctly when the last match appears or disappears:

```
0x01 0xJL op_id(16) join_key(var) row_id(16) → row_bytes
0x01 0xJR op_id(16) join_key(var) row_id(16) → row_bytes
0x02 0xJM op_id(16) side(1) row_id(16)       → i64 match_count
```

For LEFT JOIN, a left insert with no right matches emits `(left,NULL,+1)` and
stores `match_count=0`. A right insert scans left rows for the key: for each
left row whose count was 0, it first retracts `(left,NULL,-1)`, increments the
count, and emits `(left,right,+1)`. A right delete emits `(left,right,-1)`,
decrements the left count, and if the count reaches 0 emits `(left,NULL,+1)`.
RIGHT JOIN is symmetric. FULL JOIN runs the same accounting on both sides so
both null-padded projections are retracted or restored exactly once. This is
the native operator state required by the EC-01 / FULL JOIN NULL cases in the
oracle suite.

### 6.5 Theta-Join / Cross-Join

Falls back to broadcast: one side is broadcast to all shards of the other side.
The compiler picks the smaller side. Broadcast happens via Exchange with target
list = `[all shards]`.

### 6.6 Distinct / Union (Set Semantics)

```
0x01 0xDS op_id(16) row_hash(16) → i64 weight
```

`MergeOperator` sums weights. Output emits delta when weight transitions
between zero and non-zero. When a key reaches zero, the operator emits an
explicit delete/tombstone for the arrangement entry when correctness requires
immediate invisibility. A compaction filter may later remove obsolete merge
operands after the frontier proves no snapshot can need them.

### 6.7 Window Functions (ROW_NUMBER, RANK, LAG, LEAD, sliding aggregates)

```
0x01 0xWN op_id(16) partition_key(var) order_key(var) row_id(16) → row_bytes
```

The order_key in the key gives natural ordering. For LAG/LEAD, scan the
neighboring entries. For sliding aggregates, maintain a segment tree per
partition; segment-tree nodes are stored under `op_index/`.

### 6.8 Recursion (`WITH RECURSIVE`, fixed points)

```
0x01 0xRC op_id(16) row_hash(16) iteration(4 BE) → i64 weight
```

State for the recursive variable is stored as a weighted set. Iteration is
driven by the operator scheduler: each iteration produces new deltas that
feed back as input deltas at the next iteration timestamp. The frontier
protocol naturally handles the inner-time dimension (the `iteration` component
of the timestamp vector).

Convergence detection: iteration stops when the recursive delta distincts to
empty and the inner frontier advances past the iteration timestamp. The compiler
also classifies recursive terms for monotonicity. INSERT-only monotone recursion
uses semi-naive evaluation; mixed insert/delete/update changes use DRed
(delete-and-rederive); non-monotone or unsupported recursive terms fall back to
full recomputation. See [IVM.md §11](IVM.md#11-recursion-with-recursive).

### 6.9 Time Windows (Tumbling, Hopping, Session)

```
0x01 0xTW op_id(16) window_id(16) key(var) → partial_state
```

`window_id` is computed from the event-time of the row. Window expiry uses
SlateDB **TTL** based on event-time-derived deadlines and a frontier-aware
compaction filter. The filter is only allowed to remove data older than both
the event-time expiry and the relevant input/output frontiers.

**Late-data policy.** A row is *late* for window `W` if its event-time falls
below the operator's current input frontier minus `allowed_lateness`. Per
pipeline or per time-window operator, the operator declares:

| Policy | Behaviour |
|---|---|
| `drop` | Late rows are silently discarded before reaching the window operator. |
| `update` | Late rows are applied to the window arrangement if it has not yet been garbage-collected; the window emits a corrected delta. |
| `route_to_sink` | Late rows are forwarded to a designated dead-letter sink connector with the original event-time preserved. |

The `allowed_lateness` budget is a duration attached to the time-window operator;
default is 0 (no late data accepted). It is surfaced in `EXPLAIN INCREMENTAL` and
counted as `connector_late_rows_total` in metrics.

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

The fast path is **worker-to-worker**, not shard-to-shard. A worker opens at
most one pooled bidirectional stream to each peer worker per traffic class and
multiplexes all shard/exchange batches over it. The framing header carries
`exchange_id`, `src_shard`, `target_shard`, `epoch`, and `seq`. This bounds
connection count at `O(workers^2)` instead of `O(shards^2)` and keeps large
clusters operable.

**Durable path (fallback / recovery / large batches)**: sender uploads the batch
as a coalesced object to
`s3://bucket/shuffle/{exchange_id}/{epoch}/{src_worker}/{target_worker}/{part}.arrow`.
One object may contain many `(src_shard,target_shard,seq)` frames plus an index
footer. The object key is written into the sender's `shuffle_outbox/`; the
receiver learns about it through direct notification or by tailing the sender's
outbox metadata. Receivers do **not** LIST the shuffle prefix on the hot path.

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
}
```

Ordering is product order: `t1 ≤ t2` iff every component of `t1` ≤ corresponding
component of `t2`. Two timestamps may be incomparable — hence we need
antichains. Additional nested scopes are represented by a stack of timestamp
components in the in-memory type (`Vec<ScopeTime>`), but the storage encoding
starts with the two components above because they cover the v1 hot path:
source epochs and recursive iterations. A new timestamp component is a storage
format change and must be added through the schema/format compatibility policy.

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

Frontier aggregation is **explicitly asynchronous**. It is not on the hot path
of any operator: operators decide what to do for the next epoch from their last
observed input frontier, even if it is a few aggregation rounds stale. The
aggregator's batching interval (`frontier_agg_interval`) is a tunable budget,
typically 50–500 ms. A frontier that is up to one budget interval stale is
still correct for garbage collection, window closing, and shuffle cleanup; it
only affects how quickly those reclamations happen.

### 8.5 Garbage Collection of Shuffle Buffers

When a receiver operator's input frontier on exchange `E` advances past epoch
`e`, the receiver writes:

```
control: frontier/exchange_e/consumed → e
```

Senders observe this and enqueue exact cleanup for all `shuffle_outbox/` entries
with `epoch ≤ e`. Receivers do the same for `shuffle_inbox/`. Because SlateDB
does not currently expose range deletion, cleanup is implemented as bounded
prefix scan + batched deletes, with a frontier-aware compaction-filter fallback
for very old retained data. This is exact in semantics; it is not implemented
by a single range-delete API call.

---

## 9. Atomic Epoch Commit Protocol

Each shard commits the mutations produced by all ready operator instances for an
epoch as one or more coalesced SlateDB `WriteBatch`es. Operator tasks return
`EpochOutput`; the shard-level epoch coordinator groups these outputs by shard
to reduce WAL/manifest/object-store write amplification. A batch includes:

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

This is the **only** durability event per epoch per shard group, not per
operator. SlateDB's WAL guarantees each `WriteBatch` is atomic. Recovery is
automatic: on restart, the shard reads its current frontier and processes inputs
from that frontier forward. Every write is idempotently keyed by epoch,
operator, port, and sequence.

### 9.1 Epoch Sizing

Epoch size is a tuning parameter, not a constant. The shard-level coordinator
enforces `min_epoch_ms` and `min_epoch_bytes` floors so a bursty source cannot
drive manifest/WAL writes faster than object storage can amortize them. It
also enforces a `max_epoch_ms` ceiling so a quiet source still publishes
frontier progress on a predictable cadence. All three are exposed per pipeline.

### 9.2 Crash Semantics

On worker death, the new worker opens each lost shard as the single writer
(SlateDB fences the previous writer via the manifest epoch), reads
`shard_meta/0x06 0xFR` to recover the last committed epoch frontier, and
re-runs source inputs from that frontier forward. Because every write inside a
shard `WriteBatch` is idempotently keyed by `(epoch, op_id, port, seq)`,
replay is a no-op.

Partial-shard failures (one shard commits epoch `e`, another does not) are
resolved by the frontier protocol: downstream operators that depend on both
shards simply do not advance their input frontier past `e` until both shards
publish it. There is no cross-shard 2PC.

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
5. After cutover, the donor shards mark the migrated key ranges as retired and
  reclaim them via bounded scan-and-delete or a frontier-aware compaction
  filter. This avoids depending on a missing SlateDB range-delete API.

### 10.3 Removing a Shard (Graceful)

Reverse of the above. The shard's keys are migrated to other shards via
checkpoint reads, then the SlateDB is decommissioned. SST GC will eventually
reclaim its object-store footprint.

### 10.4 Fault-Driven Reassignment

If a worker dies, its shards are re-leased to another worker. SlateDB's
single-writer enforcement (via the manifest fence epoch) prevents split-brain:
the old writer cannot commit after a new writer opens the same shard.

State transfer on fault-driven reassignment differs from proactive rebalancing:
there is no live donor to create a targeted checkpoint. Instead, the new worker
obtains state by opening each shard's SlateDB directly (no snapshot needed —
the shard's last durably committed epoch is already in the WAL). The new writer
replays any WAL entries beyond the last manifest checkpoint, then resumes
processing from the recovered epoch frontier. Recovery latency is bounded by
the last shard checkpoint age (`checkpoint_age_seconds`), not by live data
transfer. Proactive rebalancing (§10.2) uses checkpoint-based migration;
fault-driven reassignment uses WAL replay. Both paths surface the pipeline as
`RECOVERING` until the frontier catches up.

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
| **Within an epoch on one shard group** | Coalesced `WriteBatch` commit is atomic. |
| **Across operators in the same cluster** | Frontier protocol + idempotent operator state keyed by source epoch. |
| **External sources & sinks** | Two-phase commit on connector state; sink writes are keyed by `(source_epoch, output_position)`. |

### 11.2 Cluster Checkpoints

Every `T` seconds (or every `N` epochs), the control plane runs a
**barrier-based** checkpoint inspired by Flink Chandy-Lamport:

1. Inject a checkpoint barrier into every source operator with a fresh
   `checkpoint_id`.
2. Barriers flow through the DAG, aligned at multi-input operators (the operator
   waits until the barrier arrives on all inputs).
3. Each shard tracks which local operators have observed the barrier. When all
   operators on that shard have durably committed through the barrier, the
   shard creates **one** SlateDB `Checkpoint` and records
   `(checkpoint_id, shard_checkpoint_id)` in the control plane. Checkpoints are
   per shard, not per operator, which bounds manifest-write bursts.
4. Barrier alignment buffers are bounded by the same credit system as shuffle:
   if a fast input reaches its alignment credit limit while waiting for a slow
   input's barrier, the operator stops granting upstream credits and
   backpressure propagates to sources. A `checkpoint_alignment_timeout` turns
   excessive waiting into `RECOVERING` or `BLOCKED`, never unbounded memory.
5. When all shards have reported, the control plane commits the cluster
   checkpoint atomically: writes `control: checkpoints/{checkpoint_id}` with the
   full map of per-shard checkpoints.
6. Old cluster checkpoints (beyond the retention horizon) are released, allowing
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

Gateways are stateless and horizontally scalable. For each query they pin to a
**published vector frontier** — the antichain of per-shard checkpoints
associated with the most recent committed cluster frontier. All `DbReader`
handles for one query open at that vector, so multi-shard reads in the same
query see a causally consistent snapshot even though no global LSN exists. The
vector frontier's age is exposed in query metadata so clients can decide
whether to retry against a fresher one.

### 12.3 Subscribe / Streaming Queries

Clients can subscribe to a view's change stream. Implemented by tailing the
shard's `WalReader` filtered to `view_output/` for the requested view-id prefix.
Subscription connections are authenticated via the same token/certificate checked
by the gateway (§12.5); the gateway proxies the subscription rather than exposing
raw shard access to clients.

### 12.4 Freshness Tokens and Read-Your-Writes

Every source commit, sink commit, and query response can carry a **freshness
token**:

```
FreshnessToken { source_id, source_epoch, cluster_frontier_hash }
```

For normal low-latency reads, the gateway pins to the freshest published vector
frontier and returns the token it used. For read-your-writes, clients pass
`wait_for=<FreshnessToken>`; the gateway waits until the published vector
frontier dominates the requested source epoch or until a caller-supplied
timeout expires. The query result then explicitly says whether the token was
satisfied. This gives application developers a simple contract without
exposing antichains in the default path.

### 12.5 Authentication and Authorization

All external interfaces (SQL port, gRPC subscribe, REST/HTTP) enforce the same
auth layer:

- **Authentication**: bearer tokens (JWTs signed by a configurable OIDC
  issuer) or mutual TLS client certificates. Tier 1 (`--auth=off`) skips auth
  for local development.
- **Authorization**: per-resource RBAC stored in the control-plane catalog
  (`catalog/acl/`). Principals are identified by the JWT `sub` or certificate
  CN. Default roles:

| Role | Can do |
|---|---|
| `viewer` | `SELECT` from granted views; subscribe to granted views. |
| `pipeline_owner` | Deploy, alter, pause, resume, drop pipelines they own; all viewer rights on owned pipelines. |
| `admin` | Everything, including granting roles and viewing all audit-log entries. |

- **Multi-tenancy isolation**: pipelines are namespaced by `tenant_id`. A
  principal with `pipeline_owner` on tenant A cannot see or affect tenant B's
  pipelines. Quota enforcement (§14.13) is per-pipeline, so tenants cannot
  starve each other by default.
- **Audit trail**: every query, subscription, deploy, and alter carries the
  authenticated principal as the `actor` in the audit log (§14.11).

`rockstream` CLI uses the same OIDC token flow (`rockstream login`) or a
service-account key file. Unauthenticated requests are rejected at the gateway
before they reach any shard or control-plane write path.

### 12.6 Postgres Wire Protocol Compatibility

The query gateway speaks the **PostgreSQL wire protocol** (via the `pgwire`
crate). Applications can connect with `psql`, standard JDBC/ODBC drivers, and
any ORM that supports Postgres — without code changes for read-heavy and
streaming workloads.

**Isolation levels supported:**

| Postgres level | RockStream implementation |
|---|---|
| `READ COMMITTED` | Each statement pins to the latest published vector frontier at statement start. |
| `REPEATABLE READ` | `BEGIN` pins the session to a specific vector frontier; all statements in the transaction see that snapshot. |
| `SERIALIZABLE` | **Not supported** (requires cross-shard conflict detection; see §1.1). Returns `RS-2003 isolation.serializable_not_supported`. |

**Postgres catalog compatibility** required for ORMs:

- `pg_catalog.pg_tables`, `pg_views`, `pg_class`, `pg_attribute`,
  `pg_namespace`, `pg_type` — populated from the control-plane catalog.
- `information_schema.tables`, `information_schema.columns` — generated views.
- Postgres native **type OIDs** sent in row description messages so drivers can
  decode column types without metadata round-trips.
- `SET search_path`, `SHOW server_version`, `SHOW transaction_isolation` —
  stub responses sufficient for ORM connection probes.

**Postgres wire protocol does NOT imply a Postgres drop-in.** DDL (`CREATE
TABLE`, `ALTER TABLE`) is handled via `CREATE PIPELINE` / `CREATE VIEW`
semantics. Write DML goes through the internal source connector (§13.5).
Extensions, `COPY`, `LISTEN`/`NOTIFY`, and advisory locks are out of scope.

**Positioning**: with the Postgres wire layer plus the internal source connector
(§13.5), RockStream operates as a *streaming SQL platform with Postgres-compatible
read access* — the same tier as Materialize and RisingWave, not Neon. Clients
write rows directly; the IVM engine keeps views fresh; `psql` queries views.

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

### 13.3 Connector Contract

Connector types are pluggable, but the contract is fixed. Built-in connectors
implement it as Rust traits; external connectors use the same protocol over
gRPC so they can run in a separate connector tier.

Source connectors must provide:

```
discover_schema() -> SchemaVersion
start_snapshot(frontier) -> SnapshotStream
poll_delta(after_offset, max_bytes) -> Vec<RecordBatchDelta>
commit_offset(epoch, offset) -> IdempotentResult
pause(reason) / resume()
```

Sink connectors must provide:

```
prepare(epoch, rows) -> pending_handle
commit(epoch, pending_handle, checkpoint_id) -> IdempotentResult
abort(epoch, pending_handle) -> IdempotentResult
```

Every emitted row includes the stable `row_id` rules from §6.4 and the schema
version from §4.2. Connector failures use the `RS-1xxx` error range; schema
drift that cannot be applied online becomes `BLOCKED(RS-1002)`.

### 13.4 Connector Catalog and Isolation

The control plane catalogs available connector types and routes connector
instances to workers. Connector processes are independent of operator
processes; they can be co-located for low latency or run as a separate
"connector tier" for isolation.

### 13.5 Internal (Direct-Write) Source Connector

Clients do not need an external Kafka or Postgres to feed a pipeline. The
**internal source connector** accepts DML (`INSERT`, `UPDATE`, `DELETE`) issued
directly over the Postgres wire protocol (§12.6) and converts them to Z-set
deltas on a dedicated **base-table shard**.

```
Client (psql / JDBC)  ──INSERT──►  Gateway  ──delta──►  Base-table shard
                                                               │
                                                    Internal source connector
                                                               │
                                                        Pipeline IVM engine
```

**Write semantics:**

- Each `INSERT`/`UPDATE`/`DELETE` is appended to a per-connection write buffer
  (analogous to a Postgres transaction buffer).
- On `COMMIT`, the buffer is flushed as a single atomic Z-set delta to the
  base-table shard via `WriteBatch`. The delta receives the shard's next
  `source_epoch`, identical to any other source connector.
- On `ROLLBACK`, the buffer is discarded without touching the shard.
- On `BEGIN`, the session pins to the current published vector frontier for
  `REPEATABLE READ` reads within the same transaction.

**Isolation guarantees:**

| Isolation | Within a session | Across sessions |
|---|---|---|
| `READ COMMITTED` | Reads see every committed delta before statement start. | ✅ |
| `REPEATABLE READ` | Reads see the snapshot at `BEGIN`. Writes are session-local until `COMMIT`. | ✅ |
| `SERIALIZABLE` | Not supported (§1.1, §12.6). | ✗ |

**Self-contained operation**: with the internal source connector, a pipeline
needs no external broker or database. Deploy RockStream, issue SQL `INSERT`
statements, query IVM views — no Kafka, no Postgres, no infrastructure beyond
object storage.

---

## 14. Operations: Deploy, Monitor, Diagnose

This section is a contract, not aspiration. Every primitive below has a
corresponding milestone in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md);
the design is not considered shipped until they exist and stay working.

### 14.1 The Operator's Mental Model

The operator interacts with **two nouns** and **one verb**:

- **Pipelines**: a named compiled plan (a set of views fed by named
  connectors). The unit of deployment, quota, priority, and SLO.
- **Views**: SQL views maintained by a pipeline. The unit of query.
- **Deploy**: submit/replace a pipeline. Everything else (sharding, shuffle,
  recovery, compaction, GC, rebalancing) is implicit.

The operator never names a shard, an operator instance, an arrangement, a
frontier, a checkpoint, or a WAL segment in normal work. Those terms appear
only inside drill-downs (`EXPLAIN INCREMENTAL`, support bundle, audit log)
and only when something is wrong.

### 14.2 Deployment Ladder

One binary (`rockstream`), one config schema, three tiers. Every tier uses
the same storage layout (§5) and the same SQL interface.

```
Tier 1 — Single process (dev / eval)
  rockstream start --storage=./data
  In-process control plane, one worker, local FS, a handful of shards.
  Zero config needed. Survives crashes via local SlateDB.

Tier 2 — Single host (small production)
  rockstream start --role=all --storage=s3://bucket/...
  Control plane and workers in one process; shared object storage.
  Survives worker restarts; horizontally scales within one host.

Tier 3 — Multi-host cluster (full production)
  On each control node:  rockstream start --role=control --storage=s3://...
  On each worker node:    rockstream start --role=worker  --storage=s3://...
  Elastic. Workers and control nodes can be added or removed online without
  config rewrites; the control plane discovers and admits them.
```

Moving from Tier 1 to Tier 3 is purely additive: the same data files
produced by Tier 1 against MinIO open in Tier 3 against S3. There is no
migration step because there is no node-local state to migrate.

### 14.3 SLO-Driven Configuration

The operator declares **what** they want; the control plane decides **how**:

```sql
CREATE PIPELINE sales_pipeline
WITH (
    freshness_target_ms = 1000,        -- views must be ≤ 1 s stale
    state_budget_gb     = 200,         -- pipeline may use ≤ 200 GB state
    object_store_rps    = 5000,        -- soft cap on PUT+GET per second
    priority            = normal       -- low | normal | high
)
AS
    CREATE SOURCE orders FROM kafka (...);

    CREATE VIEW sales_by_product AS
        SELECT product_id, SUM(quantity) AS qty
        FROM   orders
        GROUP BY product_id;

    CREATE VIEW sales_by_region AS
        SELECT region, SUM(quantity) AS qty
        FROM   orders
        GROUP BY region;
```

A pipeline may contain many sources and many views. The compiler builds one
shared operator DAG so common subplans are maintained once and fanned out to
multiple view sinks. `ALTER PIPELINE ... ADD VIEW` and `ALTER PIPELINE ...
REPLACE VIEW` use the schema/plan replacement path from §4.2.

The control plane auto-tunes the four mechanism knobs (§14.6) to satisfy the
SLO inside the quota. Operators do not normally set those knobs; they set
intent. If the SLO cannot be met inside the quota, the pipeline transitions
to a named degraded state (§14.10) instead of silently missing the target.

### 14.4 The One Signal: SLO Compliance

For every pipeline the control plane reports a single rolling indicator:

```
pipeline_slo_compliance{pipeline="sales_pipeline"}  =  0.0 .. 1.0
```

Value `1.0` means the freshness target has been met for the full window
(default 5 min). Anything below is the fraction of time it was met. A single
Grafana panel showing this number per pipeline is enough to answer "is the
platform healthy?" without operator training.

When SLO compliance dips, the corresponding `pipeline_degraded_reason` label
reports a named reason from §14.10. Drill-down metrics break the reason down
by operator and shard.

### 14.5 Self-Tuning by Default

Three control loops run continuously in the control plane. All three are on
by default and can be disabled per pipeline (`autotune.* = off`) for audited
manual control.

| Loop | Adjusts | Trigger | Bounds |
|---|---|---|---|
| **Adaptive parallelism** | `operator.*.parallelism` | Operator `epoch_ms` p95 trends above SLO budget for > 30 s | `min_parallelism` ≤ N ≤ `max_parallelism` (per pipeline) |
| **Adaptive epoch sizing** | `min_epoch_ms`, `max_epoch_ms` | Object-store write rate trends above quota, or SLO compliance < target | Floor: 10 ms; ceiling: 5 s |
| **Adaptive source throttle** | Per-connector `max_poll_bytes_per_epoch` | `frontier_lag_ms` trends above `freshness_target_ms * 1.5` for > 20 s, indicating ingestion is outpacing processing | Minimum 1 row/epoch; maximum = connector's native batch ceiling |

Every adjustment is recorded in the audit log (§14.11) with the metric
reading that triggered it. Operators see *what the system decided and why*,
not opaque magic.

### 14.6 Manual Override Knobs

For the cases auto-tuning cannot solve, the same four knobs from v3.2 remain
available as per-pipeline or per-operator overrides:

| Knob | Auto default | When to override |
|---|---|---|
| `min_epoch_ms` | adaptive (10 ms–250 ms) | You have a known cost ceiling object storage cannot exceed. |
| `max_epoch_ms` | = `freshness_target_ms / 2` | You want freshness tighter than what the SLO loop derives. |
| `frontier_agg_interval` | 100 ms | Very large clusters (≥ 1000 shards) may relax to 500 ms. |
| `operator.*.parallelism` | adaptive | `EXPLAIN INCREMENTAL` shows a specific operator stuck ⚠ and you want to pin it. |

Manual overrides are sticky and visible in `SHOW PIPELINE` output so the
next operator does not have to guess why a value was set.

### 14.7 The CLI Surface

Everything is one binary, one CLI:

```
rockstream start           --role=all|control|worker|gateway
rockstream pipeline {list, show, deploy, replace, pause, resume, drop}
rockstream view     {list, show, query, subscribe}
rockstream explain  <view> [--estimate]
rockstream cluster  {status, workers, quotas}
rockstream support  bundle [--pipeline=<name>]   # see §14.12
rockstream audit    {tail, query}                # see §14.11
```

No separate `rockstream-control`, `rockstream-worker`, `rockstream-gateway`
binaries; no separate config files for each role. A node decides which roles
it plays from its `--role` flag and what it can see from its `--storage` URL.
A single uniform binary makes packaging, image building, and version
upgrades trivial.

### 14.8 Diagnosing a Slow or Stuck Pipeline

Run `rockstream explain <view>` (equivalently `EXPLAIN INCREMENTAL <view>`)
to get a human-readable summary of the view's operator graph annotated with
the latest per-operator statistics:

```
VIEW  sales_by_product  [SLO: 1000 ms]  [lag: 450 ms ✅]  state: 12 GB / 200 GB
 ├─ AGG  SUM(quantity) GROUP BY product_id  [avg_epoch: 3 ms]  [shards: 8]
 │   └─ EXCHANGE  hash(product_id)  [depth: 0 batches]  [throughput: 1.2 M rows/s]
 │       └─ JOIN  orders ⋈ products ON product_id  [avg_epoch: 180 ms ⚠]  [shards: 32 → 64 (adapting)]
 │           ├─ EXCHANGE  hash(product_id)  [depth: 14 batches ⚠]  [throughput: 800 k rows/s]
 │           │   └─ SCAN  orders  [connector_lag: 0 ms]
 │           └─ SCAN  products  [connector_lag: 0 ms]
```

The `⚠` flags draw attention to operators whose `avg_epoch_ms` or
`shuffle_outbox_depth` exceed thresholds. The `→ 64 (adapting)` annotation
shows the adaptive-parallelism loop is already responding. Operators almost
never need to touch these manually; the value of the tree is *understanding
what the system is doing*, not driving it.

### 14.9 Cost Preview Before Deploy

```
rockstream explain <view> --estimate
```

or

```sql
EXPLAIN INCREMENTAL ESTIMATE <CREATE VIEW …>;
```

produces a predicted operator tree with:

- estimated state size per operator (from source-table statistics);
- estimated steady-state object-store request rate;
- estimated minimum frontier lag at the requested SLO;
- whether the pipeline fits within its declared `state_budget_gb` and
  `object_store_rps` quotas, and if not, by how much.

This is the single biggest operator surprise eliminated: nobody has to
deploy a view to discover it needs 4 TB of arrangement state.

### 14.10 Named Degraded States

When a pipeline cannot meet its SLO inside its quotas, it transitions to a
**named** degraded state. The control plane never fails silently and never
drops data without an explicit, surfaced reason. States:

| State | Meaning | Operator action |
|---|---|---|
| `HEALTHY` | SLO met, quota margin available. | None. |
| `BACKFILLING` | Pipeline is loading historical source data. SLO compliance is not counted yet; `backfill_progress` is shown separately. | Wait or raise bootstrap parallelism/quota. |
| `RECOVERING` | Worker or shard recovery is replaying from a checkpoint. SLO compliance is temporarily excluded from alerting until `recovery_deadline`. | Watch recovery progress; investigate only if deadline expires. |
| `STRESSED` | SLO met, quota ≥ 80% utilised. | Plan capacity addition. |
| `OVER_BUDGET_RELAXED` | SLO relaxed by the system because state budget is full. Freshness is degraded but data is correct. | Raise `state_budget_gb` or revise view to reduce state. |
| `RPS_THROTTLED` | SLO relaxed because object-store quota is the bottleneck. | Raise `object_store_rps` or revise SLO. |
| `PAUSED` | Pipeline explicitly paused, or paused by admission control to free capacity for higher-priority work. | Resume when ready. |
| `BLOCKED` | A non-recoverable error (e.g. connector authentication, schema mismatch). | Inspect `pipeline_blocked_reason`; fix; resume. |

Every state transition is in the audit log (§14.11) with the metric or
event that caused it. SLO compliance §14.4 dips together with the state
transition so the dashboard tells the same story.

### 14.11 Audit Log

Every control-plane action is appended to a durable, queryable audit log in
the control SlateDB:

```
control: audit/{ulid} → {
  timestamp, actor ("system" | user_id), action, target (pipeline/view),
  before, after, reason, related_metric
}
```

Actions captured: pipeline deploy/replace/pause/resume/drop, autotuner
parallelism change, autotuner epoch-size change, admission-control pause,
shard add/remove/rebalance, worker join/leave, checkpoint commit, degraded-
state transition.

`rockstream audit tail` follows the log; `rockstream audit query` supports
filters by pipeline, time range, action type. The log is the single source
of truth for "what changed in the cluster yesterday at 03:00 UTC?".

### 14.12 Support Bundle

One command collects everything needed to debug an issue without ad-hoc
requests for logs, metrics, plans, and configs:

```
rockstream support bundle --pipeline=sales_pipeline --since=1h --out=bundle.tar.gz
```

Includes: pipeline definition, last N compiled plans, last N audit-log
entries scoped to the pipeline, the live `EXPLAIN INCREMENTAL` output, the
relevant Prometheus metric series for the time window, recent worker logs,
recent checkpoint references, anonymised sample of recent connector
offsets, and the cluster topology snapshot. Sensitive values (credentials,
user data) are redacted by default; `--include-secrets=false` is the
default and cannot be overridden by config (only by an explicit CLI flag).

### 14.13 Quotas and Multi-Tenancy

Every pipeline declares its resource envelope at creation. The control
plane enforces these as hard caps:

| Quota | Enforced by |
|---|---|
| `state_budget_gb` | Sum of `op_state_bytes` across the pipeline; over-limit transitions to `OVER_BUDGET_RELAXED`. |
| `object_store_rps` | Token-bucket admission on the shard commit path. |
| `max_parallelism` | Upper bound for the adaptive-parallelism loop. |
| `max_shards` | Upper bound on shards owned by this pipeline. |
| `priority` | Used by admission control (§14.16) to choose which pipelines to pause first under contention. |

Quotas are declared in `CREATE PIPELINE` and can be altered with
`ALTER PIPELINE ... SET (...)`. They are visible in `SHOW PIPELINE` and in
the audit log when changed.

### 14.14 Error Code Taxonomy

Every error returned to a user, written to a log, or recorded as a
`pipeline_blocked_reason` carries a stable `RS-XXXX` code with a published
doc URL. Examples (illustrative):

```
RS-1001  connector.authentication_failed
RS-1002  connector.schema_drift
RS-2001  view.unsupported_sql_construct
RS-2002  view.state_budget_exceeded
RS-3001  shard.fence_lost
RS-3002  shard.recovery_replay_failed
RS-4001  control.quota_violation
RS-4002  control.autotune_bounds_exhausted
```

`rockstream` exits non-zero on any RS-coded error and prints a one-line
remediation pointer. The codebase has a single error-code registry; CI
fails if a new code is introduced without a doc entry. "Internal error"
without a code is itself a bug.

### 14.15 Metrics Reference

Every shard, operator instance, and pipeline reports:
- `pipeline_slo_compliance` — the primary indicator (§14.4).
- `pipeline_degraded_reason` — label when below 1.0 (§14.10).
- `frontier_lag_ms` — raw lag, per pipeline.
- `backfill_progress` — for snapshot-mode connectors: `offsets_consumed / snapshot_end_offset` (both reported by the connector's `discover_stats()`). Undefined (omitted) for live-only connectors with no snapshot boundary.
- `recovery_progress` — fraction of shards whose recovered epoch frontier ≥ the cluster checkpoint epoch.
- `rows_in_per_sec`, `rows_out_per_sec` — throughput.
- `epoch_ms` — per operator, processing time per epoch.
- `op_state_bytes`, `op_state_rows` — arrangement size.
- `shuffle_outbox_depth` — pending batches on each exchange sender.
- `connector_lag_ms` — age of the oldest unread event in the source.
- `compaction_backlog_bytes` — SST bytes awaiting compaction.
- `checkpoint_age_seconds`, `checkpoint_duration_seconds`, `checkpoint_lag_ms` — recovery planning and checkpoint health.
- `object_store_rps` — PUT+GET+LIST+DELETE per second per shard.
- `autotune_decisions_total` — counter labeled by `(loop, direction)`.

Exported via Prometheus / OpenTelemetry. A starter Grafana dashboard ships
in `deploy/dashboards/rockstream-overview.json` and contains exactly one
panel above the fold per pipeline: SLO compliance over time.

### 14.16 Backpressure and Admission Control

Backpressure is cooperative credit flow: receivers grant credits to senders;
senders block on credit exhaustion; this propagates upstream as growing
`frontier_lag_ms` long before any data loss is possible. No operator blocks
on a sibling's progress; only on its own credits and its own input
frontier. This is the structural reason RockStream does not adopt Feldera's
`DynamicScheduler` ownership model.

Admission control sits in front of every `CREATE PIPELINE` and every
autotuner expansion. It refuses requests that would push cluster
utilisation past configured thresholds, and it pauses lower-priority
pipelines when higher-priority ones request capacity that is otherwise
unavailable. Both decisions are recorded in the audit log with the
relevant metric readings.

### 14.17 Failure Injection (`rockstream chaos`)

A built-in fault-injection subcommand makes the recovery story testable in
the same environment as production. Inject worker kills, object-store
latency, shard fence loss, or connector stalls and watch SLO compliance,
degraded-state transitions, and the audit log respond. Recovery is not a
story told in docs; it is a button anyone can press.

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
| **Postgres wire protocol** | No | Yes | Yes | No | **Yes (§12.6)** |
| **Direct DML writes** | No | No (CDC only) | No (CDC only) | No | **Yes (§13.5)** |
| **SERIALIZABLE isolation** | No | Emulated | Emulated | N/A | **No (§1.1)** |
| **Open source** | Yes | Yes | Yes | No | Yes |

The unique positioning: **end-to-end object-storage native** (no NVMe required,
no local-state assumptions) **+ full SQL via DBSP** (correctness guarantees) **+
adaptive per-operator parallelism**.

---

## 16. Optimality Assessment (v3.4)

The v3.4 review asks whether the design is coherent, easy to operate, and
optimal enough to build. Each answer is a structural commitment encoded in this
document; the open risks list what remains to validate in implementation.

### 16.1 Is the storage substrate used correctly?

**Yes, with explicit budgets.** SlateDB's single-writer fence, `WriteBatch`,
`DbReader`, checkpoints, segment extractors, TTL, compaction filters and WAL
reader are all real and used as documented. Range deletion is absent and is
therefore not assumed. Cleanup is scan-and-delete plus frontier-gated
compaction filters (§5.3, §8.5). Manifest and WAL costs are explicit budgets
(§5.4, §9.1).

### 16.2 Does the runtime model fit a sharded object-store backend?

**Yes, after diverging from Feldera in three places.** RockStream borrows
Feldera's circuit-of-typed-operators design but (a) schedules operators
asynchronously per shard rather than via Feldera's synchronous
`DynamicScheduler`, (b) treats arrangements as SlateDB-backed indexed Z-sets
rather than in-memory Spines, and (c) makes `Exchange` first-class so
cross-shard ownership is never an `OwnershipConflict` error.

### 16.3 Are pg_trickle's correctness rules honored?

**Yes, as oracle-driven test obligations.** EC-01 join split, Q07
double-counting correction, Q21 SemiJoin context, FULL JOIN NULL handling in
SUM, `has_key_changed` metadata, distinct-as-multiplicity, EXCEPT/INTERSECT
per-branch counts, DRed recursion, recomputation fallback, diamond
consistency, and cadence inheritance all appear as planner metadata or test
vectors in IVM.md. The runtime does not execute pg_trickle's SQL; it must
match its behavior.

### 16.4 Does the time model scale?

**Yes, because it is causal.** Per-shard frontiers compose into a cluster
vector frontier; there is no global LSN to contend on. Aggregation is async
with a documented staleness budget (§8.4). Query reads pin to a published
vector frontier (§12.2). Recursion participates in the same antichain via the
inner `iteration` component.

### 16.5 Is the system understandable and operable?

**Yes, with an explicit operating contract.** Operators deploy pipelines and
query views; shards, antichains, arrangements, checkpoints, and WAL segments
are internal unless a drill-down is requested. SLO compliance is the primary
signal (§14.4). `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, the
audit log, named degraded states, support bundles, and freshness tokens make
the system explain itself before and during failure.

### 16.6 Where is the design still at risk?

The following items are the explicit validation backlog and feed the
implementation plan's Phase 3.5 and Phase 4 acceptance criteria:

- **Hot-key skew** in joins and aggregates. Sub-key partitioning and adaptive
  re-sharding (§10.5) must keep worst-shard load within a documented factor
  of median.
- **Object-store request budget** under sustained load (PUT/GET/LIST/DELETE
  per second per shard) including WAL, manifest, SST, shuffle, and
  checkpoints.
- **Frontier-aggregator throughput** with thousands of shards × hundreds of
  operators. The aggregator must be CPU- and memory-bounded, never blocking.
- **Distributed recursion cost**. Each inner iteration is a full shuffle
  round; convergence detection across shards must not require a synchronous
  global barrier.
- **Bootstrap and recovery time** for state sizes ≥ 1 TB. Recovery uses
  `DbReader` against per-shard checkpoints; base-table ingest uses snapshot
  mode (IVM.md §12).
- **Compaction-filter safety proofs** for distinct/union retention and
  windowed expiry. Every filter has a written argument that no `Drop`
  decision could resurrect a version observable by an active reader.
- **Control-plane HA**. A single SlateDB writer with hot readers is good
  enough for Tier 1/2; production uses a Raft-elected writer lease over the
  control SlateDB (§3). The remaining risk is implementation complexity, not an
  architectural gap.
- **Schema evolution and blue/green plan replacement**. Compatible changes are
  straightforward; breaking changes require clone/backfill/flip and must prove
  they preserve exactly-once source offsets.
- **Exchange object count and connection fan-out**. The design now bounds both
  with worker-level multiplexing and coalesced durable shuffle objects (§7.2),
  but Phase 4 must validate the request-rate math at thousands of shards.
- **Barrier alignment under skew**. Checkpoint buffering is bounded by credits
  (§11.2), but Phase 6 must prove it does not starve quiet inputs or trigger
  false recovery under bursty sources.
- **Auto-tuner stability**. Hysteresis and auditability are specified (§14.5),
  but workloads with step-function traffic still need soak tests to ensure the
  tuner does not oscillate.

The design is considered structurally sound modulo these validations.

---

## Appendix: Key Encoding Reference

```
Per-shard SlateDB:
  op_state/agg:        0x01 0xAG op_id(16) group_key(var)
  op_state/minmax:     0x01 0xMM op_id(16) group_key(var) value(var) row_hash(8)
  op_state/join_L:     0x01 0xJL op_id(16) join_key(var) row_id(16)
  op_state/join_R:     0x01 0xJR op_id(16) join_key(var) row_id(16)
  op_index/join_match: 0x02 0xJM op_id(16) side(1) row_id(16)
  op_state/distinct:   0x01 0xDS op_id(16) row_hash(16)
  op_state/window:     0x01 0xWN op_id(16) part_key(var) order_key(var) row_id(16)
  op_state/timewin:    0x01 0xTW op_id(16) window_id(16) key(var)
  op_state/topk:       0x01 0xTK op_id(16) part_key(var) value_desc(var) row_id(16)
  op_state/recursion:  0x01 0xRC op_id(16) row_hash(16) iteration(4 BE)

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

  audit/event:          0x07 ulid(16)
  state_accounting:    0x08 pipeline_id(16) metric_id(2)
  schema/source:        0x09 0x01 source_id(16) version(8 BE)
  schema/view:          0x09 0x02 view_id(16) version(8 BE)
```

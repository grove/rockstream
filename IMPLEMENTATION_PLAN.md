# RockStream Implementation Plan

A phased roadmap from empty repository to a production-grade,
horizontally-scalable IVM system. Each phase delivers a working, testable
system with progressively more capability.

> **Read first**:
> - [DESIGN.md](DESIGN.md) — system architecture (storage, shards, exchange,
>   fault tolerance, scaling).
> - [IVM.md](IVM.md) — the incremental-view-maintenance engine itself
>   (PlanIR, differentiation pass, per-operator rules, circuit runtime,
>   arrangements). Phases 1–3 below operationalize IVM.md's
>   `IVM-1` through `IVM-13` milestones.

---

## Phase Overview

| Phase | Title | Outcome | Indicative Duration |
|---|---|---|---|
| 0 | Repository & Tooling | Buildable, tested, CI-green skeleton | 1–2 weeks |
| 1 | Single-Shard IVM Core (IVM-1 … IVM-3) | Single-process engine: filter/project/map + algebraic aggregates + MIN/MAX | 5–7 weeks |
| 2 | SQL Frontend & Joins (IVM-4 … IVM-6) | DataFusion → PlanIR → circuit; inner/outer/semi/anti joins; set ops | 6–8 weeks |
| 3 | Advanced Operators (IVM-7 … IVM-12) | Windows, time windows, recursion, bootstrap, view-on-view, lateral | 8–10 weeks |
| 3.5 | IVM Correctness Soak (IVM-13) | TPC-H 22/22, Nexmark, fuzz, parity vs. pg_trickle & DataFusion batch | 3 weeks |
| 4 | Multi-Shard & Exchange | Distributed execution, shuffle subsystem | 6–8 weeks |
| 5 | Frontier Protocol | Distributed progress tracking | 4 weeks |
| 6 | Fault Tolerance | Cluster checkpoints, recovery, exactly-once | 6 weeks |
| 7 | Elasticity | Online add/remove shards, rebalancing | 4 weeks |
| 8 | Connectors & Sinks | Kafka, Postgres CDC, S3, HTTP | 4 weeks |
| 9 | Query Gateway | SQL-over-views, subscriptions | 3 weeks |
| 10 | Observability & Hardening | Metrics, tracing, chaos testing, docs | 4 weeks |
| 11 | Production Launch | Beta → GA | 4 weeks |

Durations are indicative effort, not calendar time, and assume a small dedicated team.

---

## Phase 0 — Repository & Tooling

**Goal**: A workspace that builds, tests, and ships.

**Deliverables**

- Cargo workspace with the following crates:
  - `rockstream-types` — shared types (timestamp, frontier, Z-set row, schema).
  - `rockstream-storage` — wrappers around SlateDB, key encoders/decoders,
    merge operator registry.
  - `rockstream-plan` — `PlanNode` enum (the PlanIR from IVM.md §5) and the
    physical `OpNode` graph.
  - `rockstream-diff` — the `DiffCtx` differentiation pass (IVM.md §6–7).
  - `rockstream-ops` — `Operator` trait + per-operator implementations
    (IVM.md §8.1).
  - `rockstream-sql` — SQL frontend on DataFusion (Phase 2).
  - `rockstream-runtime` — worker process, circuit executor, scheduler, exchange.
  - `rockstream-control` — control-plane service.
  - `rockstream-gateway` — query gateway service.
  - `rockstream-connectors` — connector implementations.
  - `rockstream-cli` — operator CLI.
  - `rockstream-oracle` — batch reference engine + property-test harness
    asserting `incremental(query, deltas) == batch(query, accumulated)`
    (the DBSP soundness theorem, IVM.md §14.1). Used by every operator phase.
- CI: GitHub Actions running `cargo fmt --check`, `cargo clippy -D warnings`,
  `cargo test`, `cargo deny`, codecov.
- Logging via `tracing` with OTEL exporter feature flag.
- Property-testing harness via `proptest`.
- Pinned MSRV; reproducible builds.
- License headers, CONTRIBUTING, CODE_OF_CONDUCT.
- Dev container (Dockerfile + devcontainer.json) with SlateDB, MinIO,
  Postgres, Kafka pre-installed.

**Exit criteria**

- `cargo test --workspace` passes.
- `make e2e` brings up a local cluster (MinIO + 1 worker + 1 control) and tears
  it down.
- The oracle harness can drive a no-op pipeline and confirm equivalence.

---

## Phase 1 — Single-Shard IVM Core

**Goal**: A single-process engine that incrementally maintains views built
from filter, projection, algebraic aggregates, and non-invertible aggregates.
This phase delivers the foundation of the IVM engine. SQL frontend is
hard-coded plans only; the SQL parser comes in Phase 2.

### Milestone IVM-1 — Filter / Project / Map skeleton (IVM.md §13 IVM-1)

- Implement the `PlanNode` enum from IVM.md §5: variants for `Source`,
  `Filter`, `Project`, `Map`, `ViewSink`, `Exchange` (stub).
- Implement the `DiffCtx` and `DiffCtx::diff` dispatch from IVM.md §6, with
  the trivial linear-operator rules for filter/project/map.
- Implement the `Operator` trait and `EpochOutput` struct from IVM.md §8.1.
- Implement `OperatorTask` event loop (IVM.md §8.2): one tokio task per
  operator instance, single atomic SlateDB `WriteBatch` per epoch covering
  state + outputs + frontier.
- Per-shard SlateDB namespaces from DESIGN.md §5.1 (op_state, view_output,
  shard_meta) wired through `ShardDb`.
- Source connector that feeds a `Vec<RecordBatch>` as delta batches with
  `_weight: i64` column convention.
- Property test: `SELECT a, b * 2 AS c FROM t WHERE c > 10` against random
  insert/delete sequences, asserting parity with the oracle.

### Milestone IVM-2 — Algebraic aggregates (IVM.md §13 IVM-2, §7.6)

- Add `Aggregate` PlanNode + `0xAG` arrangement (DESIGN.md §6.2).
- Implement `AggregateMergeOp` (associative `(sum, count)` merge) and register
  with SlateDB's `MergeOperator`.
- Implement `diff_aggregate` for SUM / COUNT / AVG / COUNT(*):
  - Group input delta by group_key.
  - `db.merge(key, (Δsum, Δcount))` into the `0xAG` arrangement.
  - Read previous and current aggregate; emit `(old, -1) ⊎ (new, +1)` deltas
    via the cached last-emitted value in `op_index/0xAG`.
- Property test: `SELECT k, SUM(v), COUNT(*), AVG(v) FROM t GROUP BY k`
  against random sequences (insert/update/delete + group churn), asserting
  parity with the oracle.

### Milestone IVM-3 — Non-invertible aggregates: MIN / MAX (IVM.md §13 IVM-3, §7.6)

- Add `0xMM` indexed-multiset arrangement (DESIGN.md §6.3) +
  `op_index/0xMM` cached extremum.
- Implement `diff_minmax`:
  - Insert: SlateDB merge on the multiset entry; if value is the new extremum,
    update cache and emit delta.
  - Delete: if the deleted value was the extremum, prefix-scan the sorted
    multiset to find the new extremum.
- Add MEDIAN / PERCENTILE as a follow-up using the same multiset + rank lookup.
- Property test: groups churning across MIN/MAX transitions.

**Exit criteria for Phase 1**

- 1M-row/s throughput for filter on a laptop (single-threaded).
- 200k-row/s for `GROUP BY SUM`; 100k-row/s for `GROUP BY MIN`.
- Crash mid-epoch (`kill -9` injected mid-`WriteBatch`); on restart, the
  shard reads its persisted frontier and reprocesses the failed epoch —
  output bit-identical to an uninterrupted run.
- Oracle property test runs green for ≥ 100k randomized scenarios per
  operator combination.

---

## Phase 2 — SQL Frontend & Joins

**Goal**: Real SQL goes in; full join + set-op support comes out. By end of
phase, RockStream can incrementally maintain views from arbitrary multi-way
join queries written as plain SQL.

### SQL Frontend deliverables (always-on for the rest of the project)

- `rockstream-sql`:
  - DataFusion-based parser, binder, logical optimizer.
  - Custom DataFusion `Extension` nodes for incremental operators
    (`IncAggregate`, `IncJoin`, `IncDistinct`, `IncWindow`).
  - Lowering pass: `LogicalPlan` → `PlanNode` (IVM.md §5).
  - Distribution pass: annotate each `PlanNode` with `partition_key`, insert
    `Exchange` nodes wherever partitioning differs. (Exchanges are no-ops in
    single-shard mode; preparation for Phase 4.)
  - Cost-based operator-parallelism selector (initial: configurable;
    later: learned from stats).
- Plan persistence: encode physical plans as Substrait + RockStream extensions;
  store in control plane.
- SQL coverage delivered incrementally inside the milestones below, in this
  order: filter → project → group-by aggregates → inner join → outer joins
  → semi/anti → set ops → subqueries (correlated decorrelated by optimizer)
  → CASE/CAST/complex expressions. Window functions and `WITH RECURSIVE` are
  Phase 3.

### Milestone IVM-4 — Inner equi-join (IVM.md §13 IVM-4, §7.3)

- Add `InnerJoin` PlanNode + dual arrangements (`0xJL`, `0xJR` from
  DESIGN.md §6.4).
- Port the bilinear-expansion algorithm with corrections **literally** from
  [`pg-trickle1/src/dvm/operators/join.rs`](../pg-trickle1/src/dvm/operators/join.rs):
  - Part 1 — `ΔL ' R` split into `ΔL_I ' R₁` and `ΔL_D ' R₀` (EC-01 fix).
  - Part 2 — `L₀ ' ΔR` with appropriate pre-change snapshot construction.
  - Part 3 — correction term `(L₁ − L₀) ' ΔR` for join children (Q07 fix).
- Pre-change snapshot semantics: arrangements are updated at end-of-epoch
  commit, so during processing they reflect epoch `e-1`.
- Distribution pass inserts `Exchange` whenever the join key differs from the
  child's partition key (no-op in single shard; verified by tests).
- Run TPC-H Q1, Q3, Q5 (5-way join), Q6 against the batch oracle for parity.
- Property test: random 3-way join over random insert/update/delete sequences.

### Milestone IVM-5 — Outer / Semi / Anti joins (IVM.md §13 IVM-5, §7.4–7.5)

- Add `LeftJoin`, `RightJoin`, `FullJoin`, `SemiJoin`, `AntiJoin` variants.
- Port pg_trickle's implementations
  ([`outer_join.rs`](../pg-trickle1/src/dvm/operators/outer_join.rs),
  [`full_join.rs`](../pg-trickle1/src/dvm/operators/full_join.rs),
  [`semi_join.rs`](../pg-trickle1/src/dvm/operators/semi_join.rs),
  [`anti_join.rs`](../pg-trickle1/src/dvm/operators/anti_join.rs)) with
  side-specific NULL-padding logic and the Q21 SemiJoin correction.
- One extra arrangement per side tracking currently-unmatched rows so
  transitions can emit retractions.
- Run TPC-H Q11, Q21 (the notorious SemiJoin corner cases) against the oracle.

### Milestone IVM-6 — Distinct / Union / Intersect / Except (IVM.md §13 IVM-6, §7.7–7.8)

- `0xDS` weight-based arrangement (DESIGN.md §6.6) with
  `DistinctWeightMerge` (`i64` addition).
- Output delta on zero-crossing transitions (0 → +n emits +1;
  +n → 0 emits −1).
- SlateDB compaction filter drops keys with weight 0.
- Implement Intersect / Except with set + bag semantics; port pg_trickle's
  `intersect.rs` / `except.rs`.
- Property tests on set semantics with random sequences.

**Exit criteria for Phase 2**

- Plain-SQL view DDL works end-to-end: a user can submit
  `CREATE VIEW v AS SELECT ... FROM t1 JOIN t2 ON ... GROUP BY ...` and the
  engine compiles, deploys, and maintains it incrementally.
- TPC-H Q1, Q3, Q5, Q6, Q11, Q21 all pass parity vs. DataFusion batch.
- All compiled plans round-trip through Substrait without loss.
- Property-test harness extends to every operator combination implemented
  so far.

---

## Phase 3 — Advanced Operators

**Goal**: Cover the remaining operators required to handle the full SQL
standard for analytical workloads.

### Milestone IVM-7 — Window functions (IVM.md §13 IVM-7, §7.9)

- Add `Window` PlanNode + `0xWN` ordered arrangement (DESIGN.md §6.7).
- Strategy from pg_trickle: **partition-based recomputation** — when any row
  in a partition changes, recompute the whole partition.
- Vectorized rewrite: per affected partition, read all rows from the
  arrangement and re-evaluate the window function batch-wise; diff against
  previously-emitted output cached as part of the arrangement.
- Implement ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, sliding SUM/AVG.
- Optimization (deferred): segment-tree variant for sliding aggregates
  (DESIGN.md §6.7), stored under `op_index/0x02 0xST`.

### Milestone IVM-8 — Time windows (IVM.md §13 IVM-8)

- TUMBLE, HOP, SESSION windows.
- `0xTW` arrangement (DESIGN.md §6.9) keyed by `window_id`.
- Event-time TTL on arrangement entries plus a compaction filter that drops
  state past the input frontier's watermark.
- Late-data handling policy: configurable (`drop` / `update` / `route_to_sink`).

### Milestone IVM-9 — Top-K (continues Phase 2's set-op family)

- `0xTK` value-descending sort (DESIGN.md §6.10).
- Maintain only `K + ε` entries; on delete of a top-K entry, scan one past `K`
  to refill. Emit deltas that swap displaced entries.
- Detection: pg_trickle's `detect_topk_pattern` heuristic identifies
  `... ORDER BY x LIMIT K` over a partition and rewrites it to TopK.

### Milestone IVM-10 — Recursion (IVM.md §13 IVM-9, §11)

- Add `Recursive` and `RecursiveSelfRef` PlanNodes.
- `0xRC` recursive-variable arrangement (DESIGN.md §6.8) keyed by
  `row_hash + iteration`.
- Implement the nested-time scheduler loop:
  - Outer time = `source_epoch`; inner time = `iteration` (resets per epoch).
  - At each iteration, evaluate the step plan against the arrangement at
    `iteration - 1`, distinct-collapse the result, emit deltas.
  - Convergence: inner frontier advances past `iteration` with no new
    deltas → loop exits, output frontier on the operator advances to
    `{source_epoch + 1, 0}`.
- This is Feldera's `IterativeCircuit` model rebuilt for our async runtime.
- Test: transitive closure on a 1M-edge graph; recursive employee hierarchy;
  graph reachability with cycles.

### Milestone IVM-11 — Bootstrap & snapshot mode (IVM.md §13 IVM-10, §12)

- Source connectors implement **snapshot mode**: emit each base-table row
  exactly once at weight +1 in either one giant epoch or a sequence of
  streamed bootstrap epochs. The circuit processes them identically to
  any other delta.
- Streaming bootstrap: chunk a snapshot across many epochs; output frontier
  advances past `bootstrap_complete` only when every chunk has been ingested.
- Reconciliation mode: when a CDC connector loses its position, re-snapshot
  affected sources; arrangements absorb the symmetric difference (existing
  rows produce −1, new rows produce +1).
- Test: view over a 100M-row base table; verify initial output equals batch
  query result; verify mid-stream connector restart produces no divergence.

### Milestone IVM-12 — View-on-view DAG (IVM.md §13 IVM-11)

- Add `ViewRef` PlanNode that subscribes to an upstream view's CDC stream
  (the upstream view's `view_output/` namespace via SlateDB `WalReader`).
- Port pg_trickle's `dag.rs` model: per-stream-table cadence inheritance,
  diamond-consistency groups (`atomic` mode where all members of a diamond
  refresh together at the same epoch boundary, enforced by the frontier
  protocol).
- Cycle detection during plan compilation (Kahn's algorithm).
- Test: 5-level chain of views; each one is delta-driven by its parent.
  Verify cadence propagation matches pg_trickle reference behaviour.

### Milestone IVM-13 — Lateral / set-returning functions (IVM.md §13 IVM-12)

- `LateralFunction` and `LateralSubquery` PlanNodes.
- Strategy: row-scoped recomputation. For each changed outer-delta row,
  evaluate the lateral expression (a DataFusion physical plan) and emit
  expanded rows with the appropriate weight; previous expansion is retracted.
- Required for JSON-heavy workloads, `unnest()`, `jsonb_array_elements`,
  `generate_series`.

**Cross-cutting Phase 3 deliverables**

- Operator authoring guide (`docs/operators.md`) with template + checklist:
  arrangement encoding, diff rule, retraction semantics, snapshot/replay
  test, fuzz harness, microbenchmark.
- DBSP-correctness property tests for every operator + combination
  (IVM.md §14.1):

  ```
  ∀ initial S, ∀ deltas (Δ₁ ... Δₙ):
    incremental(f, S, [Δ₁ … Δₙ]) == batch(f, S ⊎ Δ₁ ⊎ … ⊎ Δₙ)
  ```

- Microbenchmarks for each operator (`criterion`).
- UDF / UDAF support hooks via DataFusion (scalar UDFs in Phase 2 already;
  UDAFs require a custom associative-combiner interface to plug into
  `MergeOperator`).

**Exit criteria for Phase 3**

- Full TPC-H runs incrementally on a single shard with parity vs. DataFusion
  batch and parity vs. pg_trickle (where applicable).
- Recursive transitive-closure example converges and produces correct deltas
  on a 1M-edge graph.
- A 5-level view-on-view DAG with diamond consistency converges to a stable
  state under continuous input.

---

## Phase 3.5 — IVM Correctness Soak

**Goal**: Prove the IVM engine is production-grade *before* layering on
distribution and fault tolerance. (IVM.md §13 IVM-13.)

**Deliverables**

- **TPC-H 22/22**: port pg_trickle's TPC-H test suite (queries Q1–Q22 at
  SF=0.01) and run all 22 incrementally on RockStream; bit-identical results
  vs. DataFusion batch.
- **Nexmark soak**: continuous Nexmark workload, 24 hours, verify zero
  divergence vs. reference.
- **Random query fuzzer**: a SQL generator producing arbitrary queries over
  a synthetic schema; runs each query both incrementally on RockStream and
  as batch on DataFusion; flags any divergence.
- **Side-by-side oracle vs. pg_trickle**: where queries are supported on
  both, run the same input through both engines and assert output equivalence.
  Acts as a second, independent correctness oracle.
- **Deterministic simulation testing**: borrow SlateDB's `slatedb-dst`
  pattern; a single-threaded, seeded-RNG harness drives source connectors
  deterministically and verifies bit-identical output across reruns.
- **Performance regression suite**: criterion benchmarks tracked over time;
  CI fails on > 10% regression.

**Exit criteria**

- 22 / 22 TPC-H queries: identical results vs. batch.
- ≥ 10× measured speedup vs. batch at 1% change rate (matches pg_trickle's
  TPC-H number).
- Random fuzzer runs ≥ 1 hour without finding divergence on any operator
  combination implemented in Phases 1–3.
- DST harness passes 100k seeds with bit-identical output across reruns.

After Phase 3.5 the IVM engine is feature-complete and correct for
single-shard. Phases 4–11 make it distributed, durable, elastic, and
production-ready.

---

## Phase 4 — Multi-Shard & Exchange

**Goal**: Move from single-process to distributed execution.

**Deliverables**

- **Shard manager** (`rockstream-runtime::shard`):
  - A worker owns N shards; each shard has its own `Arc<Db>`.
  - Shard lease acquisition via control-plane SSI transactions.
  - SlateDB fence-epoch enforcement verified in integration tests
    (two writers can't commit to the same shard).
- **Exchange subsystem** (`rockstream-runtime::exchange`):
  - gRPC service for direct shuffle (`proto/shuffle.proto`).
  - Object-store fallback writer & reader.
  - Hybrid dispatcher: chooses path per-batch based on receiver health and
    batch size.
  - `shuffle_outbox/` and `shuffle_inbox/` encoders integrated into the
    epoch commit batch.
  - Credit-based backpressure.
- **Rendezvous hashing** library with virtual nodes; property tests for
  re-balance minimality.
- **Distribution-aware execution**:
  - Operator instances are addressable by `(op_id, instance_idx)`.
  - The scheduler on each worker runs only the `OperatorTask`s (IVM.md §8.2)
    whose `instance_idx` is assigned to its shards.
  - Exchange operators serialize Arrow batches keyed by destination shard
    and stage them in `shuffle_outbox/` as part of the per-epoch atomic
    commit (DESIGN.md §9).
  - Cross-shard arrangement reads are forbidden in the hot path: the
    compiler's distribution pass guarantees that every stateful operator's
    inputs share its `partition_key`, inserting `Exchange` whenever they
    don't (IVM.md §5, §9.4).
  - Re-run the full Phase 1–3 oracle + TPC-H suite against the distributed
    cluster; results must be bit-identical to the single-shard runs.

**Exit criteria**

- 16-shard cluster (single host, 16 processes) runs TPC-H with linear
  throughput vs. single shard for parallelizable queries.
- Killing one worker process causes its shards to be re-leased to another
  worker; processing continues without data loss (verified by output equality
  vs. uninterrupted run).

---

## Phase 5 — Frontier Protocol

**Goal**: Correct progress tracking across multi-input operators.

**Deliverables**

- `rockstream-types::Frontier`: full antichain implementation with
  product-order timestamps. Property tests for meet/join/advance.
- **Per-shard frontier reporter**: bundled in every epoch commit
  (`shard_meta/0x06 0xFR`).
- **Control-plane frontier aggregator**: subscribes to all shards' `WalReader`
  feeds, computes per-operator cluster frontier, publishes to
  `frontier/op_id` in the control DB.
- **Operator frontier consumers**: each operator reads its input frontier from
  the control plane (cached, push-updated via gRPC subscription), and uses it
  to:
  - Trigger window closing.
  - Detect recursion convergence.
  - Release shuffle inbox entries.
- **Exchange GC**: senders observe `frontier/exchange_e/consumed` and range-
  delete their outbox.

**Exit criteria**

- A query with a join over two sources at different ingestion rates produces
  correct output (no premature emission, no infinite buffering).
- Recursive query converges deterministically; frontier advances past
  iteration timestamps after convergence.
- Shuffle storage usage is bounded under sustained throughput.

---

## Phase 6 — Fault Tolerance & Exactly-Once

**Goal**: Survive any single-node failure; deliver exactly-once end-to-end.

**Deliverables**

- **Cluster checkpoint coordinator** (control-plane component):
  - Barrier injection at sources.
  - Barrier alignment at multi-input operators.
  - Per-shard `Checkpoint` creation tied to barrier passage.
  - Atomic cluster-checkpoint commit in `checkpoints/cluster`.
  - Old-checkpoint GC.
- **Recovery driver**: from a cluster checkpoint, brings up every shard via
  `DbReader` pinned to its per-shard checkpoint, then re-elects writers.
- **Exactly-once sink protocol**:
  - Sink interface trait with `pre_commit(epoch, rows)` and
    `commit(epoch, checkpoint_id)`.
  - Kafka sink: transactional producer.
  - S3 / object-store sink: `_pending/` → atomic rename.
  - Postgres sink: app-managed transaction with offset table.
- **Connector offset integration**:
  - Source connectors record offsets in the epoch commit batch.
  - On recovery, replay from recorded offsets.
- **Chaos test suite**:
  - Random process kills, network partitions, disk-full, object-store throttle.
  - Verify output equivalence against a non-faulty reference.

**Exit criteria**

- 24-hour chaos run on a 32-shard cluster with continuous Kafka input and
  Kafka output: zero data loss, zero duplicates, output matches reference.
- Recovery from full cluster outage in < 60 s for state size < 1 TB.

---

## Phase 7 — Elasticity

**Goal**: Add and remove shards without downtime.

**Deliverables**

- **Online shard split**:
  - Range-based partitioning per exchange (initially identical to rendezvous
    hashing buckets).
  - Donor shard creates a `Checkpoint`; new shard ingests the affected key
    range via `DbReader`.
  - Cutover at an epoch boundary; shard map version bump.
  - Donor shard range-deletes migrated keys.
- **Online shard merge**: reverse of split.
- **Worker scale-out**: new worker process joins, control plane assigns
  un-leased shards or rebalances from over-loaded workers.
- **Skew detection**: per-shard load metrics trigger automatic re-sharding for
  hot operators.
- **`Clone` for blue/green**: control plane creates a clone of an entire
  pipeline at a checkpoint, runs the new version in parallel, atomic flip
  routes connectors when ready.

**Exit criteria**

- Scale from 8 → 64 shards during sustained TPC-H Q5 traffic; output
  uninterrupted, frontier lag returns to baseline within 30 s post-scale.
- Hot-key benchmark: introduce a 100x skewed key; auto-rebalance brings
  worst-shard load within 1.5x median within 60 s.

---

## Phase 8 — Connectors & Sinks

**Goal**: Connect to the real world.

**Deliverables**

- **Sources**:
  - Kafka (consumer-group based; offsets recorded in control plane).
  - Postgres logical replication (decoded via `pgoutput`).
  - HTTP push (webhook endpoint).
  - S3 / object-store table format ingest (Parquet + manifest).
  - SlateDB CDC source (one pipeline feeds another).
- **Sinks**:
  - Kafka (transactional).
  - Postgres upsert.
  - S3 / Iceberg / Delta Lake.
  - HTTP webhook (idempotency-key driven).
  - SlateDB CDC sink.
- **Connector lifecycle**: deploy, pause, resume, delete; failure isolation.
- **Connector marketplace structure**: SDK + example crates; documented
  contract.

**Exit criteria**

- End-to-end: Postgres CDC → RockStream IVM → Kafka, sustained at 100k rows/s
  for 24 hours with exactly-once.

---

## Phase 9 — Query Gateway

**Goal**: Serve materialized views to applications.

**Deliverables**

- **Gateway service** (stateless, horizontally scalable):
  - PostgreSQL wire protocol compatibility (using `pgwire`).
  - Routes lookups & range scans to the correct shards via `DbReader`.
  - Ad-hoc SQL over materialized views (DataFusion on a snapshot).
  - Connection pooling, query timeouts, rate limiting.
- **Subscribe API**: gRPC streaming endpoint that tails view changes (via
  `WalReader` on the relevant shards).
- **Snapshot consistency**: gateways pin to a recent cluster checkpoint so
  multiple gateway hops see consistent data.
- **Authentication / authorization**: pluggable auth (initially: bearer
  tokens); per-view RBAC.

**Exit criteria**

- `psql` connects, runs `SELECT * FROM my_view LIMIT 10`, returns < 10 ms.
- Subscribe stream survives gateway restart with no data loss.

---

## Phase 10 — Observability & Hardening

**Goal**: Production-readiness.

**Deliverables**

- **Metrics** (Prometheus): per-operator throughput, latency, state size,
  shuffle bytes, frontier lag, checkpoint duration, compaction backlog.
- **Tracing** (OpenTelemetry): per-epoch spans, per-batch spans through
  exchanges, end-to-end source-to-sink trace.
- **Logging**: structured JSON, configurable levels, log aggregation friendly.
- **Admin CLI** (`rockstream` binary):
  - `pipeline create/start/pause/delete`
  - `cluster status`, `cluster scale`
  - `shard list/migrate`
  - `checkpoint list/restore`
- **Web console** (optional, post-MVP): pipeline graph viewer, frontier lag
  charts, live throughput.
- **Chaos testing automation**: Jepsen-style test harness.
- **Performance baselines**: Nexmark, TPC-H continuous, recursive graph
  workloads with documented numbers.
- **Documentation**:
  - Operator's guide.
  - SQL reference (delta from ANSI SQL).
  - Connector development guide.
  - Deployment playbooks (k8s, ECS, bare-metal).
- **Security**:
  - TLS everywhere (worker↔control, worker↔worker, gateway↔client).
  - At-rest encryption via object-store features.
  - Audit log for catalog/control-plane operations.

**Exit criteria**

- 99.99% availability over a 30-day soak test on a 64-shard cluster.
- Documented disaster-recovery procedure executed successfully.
- Independent security review passes.

---

## Phase 11 — Production Launch

**Goal**: GA release.

**Deliverables**

- Versioning policy (SemVer), release engineering pipeline.
- Storage format compatibility guarantees (forward + one back).
- Migration tooling for upgrades.
- Hosted-service deployment package (Helm chart, Terraform modules).
- Public benchmarks vs. Feldera, RisingWave, Materialize on Nexmark / TPC-H.
- Launch blog post + reference architecture diagrams.

**Exit criteria**

- v1.0.0 tagged; binaries + container images published.
- First external production customer running with paid support contract (or
  internal stakeholder accepting handoff).

---

## Cross-Cutting Concerns

These run in parallel with every phase.

### Testing Strategy

| Layer | Approach |
|---|---|
| Unit | Per-module; `cargo test`. |
| Property | DBSP correctness theorem: `incremental == batch` for random inputs. |
| Integration | Multi-shard cluster spun up via `testcontainers`. |
| Soak | 24/72-hour runs with realistic input rates. |
| Chaos | Random faults injected via `failpoints` and OS-level kill. |
| Benchmark | `criterion` microbenchmarks; Nexmark + TPC-H macros. |
| Determinism | DST-style test (SlateDB has `slatedb-dst`); deterministic simulation. |

### Performance Targets

| Workload | Single-shard | 64-shard cluster |
|---|---|---|
| Filter+project throughput | 5M rows/s | 250M rows/s |
| GROUP BY SUM throughput | 1M rows/s | 50M rows/s |
| Equi-join throughput | 500k rows/s | 25M rows/s |
| End-to-end frontier lag (Kafka→view) | < 100 ms | < 200 ms |
| Recovery time (1 TB state) | n/a | < 60 s |

### Risk Register

| Risk | Mitigation |
|---|---|
| SlateDB single-writer is too restrictive | Already mitigated by sharding; further mitigation via per-shard write parallelism using SlateDB's batched writer. |
| Frontier protocol implementation bugs | Heavy property testing; reference implementation in pure logic for comparison. |
| Object-store cost dominates | Aggressive local SST cache; coalesce small writes; tier cold state. |
| SQL incrementalization gaps | Start from Feldera's compiler reference; build a comprehensive SQL test corpus. |
| Operator skew | Adaptive re-sharding in Phase 7; sub-key partitioning for extreme skew. |
| Hardware/network partitions | Chaos testing; documented degraded-mode behavior. |
| Schema evolution | Versioned plan storage; online plan replacement via `Clone`. |

### Team Structure (Suggested)

- **Storage** (2 engineers): SlateDB integration, sharding, exchange, checkpoints.
- **Compiler** (2 engineers): SQL → physical plan, optimizer, incrementalization.
- **Runtime** (2 engineers): scheduler, frontier protocol, operator implementations.
- **Connectors / Gateway** (1–2 engineers): I/O, exactly-once integrations.
- **SRE / Observability** (1 engineer): metrics, tracing, deployment, chaos.

Total: 8–9 engineers for ~12-month path to GA.

---

## Open Questions (To Be Resolved Early)

1. **Compiler reuse vs. ground-up** — **resolved**: ground-up Rust on
   DataFusion, with per-operator rules ported from pg_trickle (IVM.md §3).
   Feldera's Java sql-to-dbsp is a reference for SQL coverage only.
2. **Execution model: codegen vs. interpretation** — **resolved**:
   interpretation of a long-lived operator graph (IVM.md §8.3). Code generation
   may be added later as an optimization for hot queries; not required for v1.
3. **Exchange transport**: pure gRPC vs. QUIC vs. raw TCP framing. Start gRPC
   for ergonomics; benchmark and revisit.
4. **State format on SlateDB**: Arrow IPC framing per arrangement value
   (current plan, IVM.md §9.1) vs. Apache Arrow Row format for point-access
   arrangements. Benchmark in Phase 3 / Phase 3.5.
5. **Control plane HA**: lean on SlateDB's single-writer (with hot standby
   readers) and a leader election above it (etcd? Raft over SlateDB?), or use
   external Raft like the standard Kubernetes pattern? Start with a single
   writer + cold standby; harden in Phase 10.
6. **Arrangement compaction frontier**: Materialize aggressively compacts
   arrangements past the consumer frontier. We get the equivalent from
   SlateDB compaction + the weight-zero-drop compaction filter — but it's
   worth measuring whether active arrangement consolidation is needed for
   long-running queries. Resolve in Phase 3.5 soak.

These are explicitly to be revisited and answered with prototypes during
Phases 1–4.

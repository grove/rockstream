# RockStream Implementation Plan

A phased roadmap from empty repository to a production-grade,
horizontally-scalable IVM system. Each phase delivers a working, testable
system with progressively more capability.

> **Operability is a phase deliverable, not Phase 10.** Per DESIGN.md P16
> and §14, every phase below has explicit operability deliverables
> ("→ Operability" callouts). The single binary, the error-code registry,
> the audit log, the support bundle, `EXPLAIN INCREMENTAL [ESTIMATE]`,
> quotas, and the auto-tuner all land incrementally inside the phases that
> create the surface they cover — not in a single "hardening" sprint at
> the end.

> **Read first**:
> - [DESIGN.md](DESIGN.md) — system architecture (storage, shards, exchange,
>   fault tolerance, scaling).
> - [IVM.md](IVM.md) — the incremental-view-maintenance engine itself
>   (PlanIR, DBSP-native differentiation pass, operator runtime,
>   arrangements, and pg_trickle-derived correctness oracles). Phases 1–3 below operationalize IVM.md's
>   `IVM-1` through `IVM-13` milestones.

---

## Phase Overview

This document owns the detailed deliverables, exit criteria, and operability
callouts for each phase. For version sequencing, public milestones, decision
gates, and the build philosophy, see [ROADMAP.md](ROADMAP.md).

The phase numbers here map to ROADMAP.md roadmap versions as follows:

| Phase | ROADMAP versions | Focus |
|---|---|---|
| 0 | v0.1–v0.4 | Repository, simulation, storage, no-op pipeline |
| 1 | v0.5–v0.10 | Single-shard IVM core (IVM-1 … IVM-3) |
| 2 | v0.11–v0.18 | SQL frontend, joins, set ops (IVM-4 … IVM-6) |
| 3 | v0.19–v0.26 | Advanced operators: windows, recursion, view-on-view (IVM-7 … IVM-12) |
| 3.5 | v0.27 | IVM correctness soak (IVM-13) |
| 4 | v0.28–v0.30 | Multi-shard execution and exchange subsystem |
| 5 | v0.31–v0.32 | Frontier protocol and progress tracking |
| 6 | v0.33–v0.36 | Fault tolerance, exactly-once, chaos |
| 7 | v0.37–v0.39 | Elasticity: split, merge, drain, clone |
| 8 | v0.40–v0.42 | Postgres query gateway, introspection, freshness, subscribe |
| 9 | v0.43–v0.45 | Connectors and sinks (Tier 1 + Tier 2 contract) |
| 10 | v0.46–v0.49 | Auth, observability, upgrades, security |
| 11 | v0.50–v0.51 | Long soak and production beta handoff |
| 12 | v0.52–v0.54 | Cold-tier sink, Iceberg REST catalog, snapshot GC |

Durations are indicative effort, not calendar time, and assume a small
dedicated team. The ROADMAP.md version table is the single source of truth
for sequencing; this table exists only to orient readers between documents.

---

## Phase 0 — Repository & Tooling

**Goal**: A workspace that builds, tests, and ships.

**Deliverables**

- Cargo workspace with the following crates:
  - `rockstream-types` — shared types (timestamp, frontier, Z-set row, schema).
  - `rockstream-storage` — wrappers around SlateDB, key encoders/decoders,
    merge operator registry, segment extractor configuration, checkpoint
    helpers, scan-and-delete cleanup utilities. Key encoders must include
    `namespace_id` in all catalog key paths from day one (DESIGN.md §5.2).
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
  - `rockstream-sim` — deterministic simulation harness (DESIGN.md §17):
    `Runtime` trait abstracting `now`, `spawn`, `sleep`, `object_store`,
    `network`; `TokioRuntime` (production) and `SimRuntime` (in-memory,
    seeded RNG) implementations; `buggify!()` macro (no-op in release, hot
    in simulation builds). Threaded through every other crate from Phase 1
    onward; no I/O surface in the codebase may bypass it. Includes the
    v3.10 TigerBeetle-style safety contract: paired assertions at
    durable/network boundaries, an explicit simulator fault model, liveness
    checks tied to the recovery SLOs, and fixed upper bounds for queues,
    buffers, and scan windows.
- CI: GitHub Actions running `cargo fmt --check`, `cargo clippy -D warnings`,
  `cargo test`, `cargo deny`, codecov.
- Logging via `tracing` with OTEL exporter feature flag.
- Property-testing harness via `proptest`.
- Storage API validation tests proving the design uses only supported SlateDB
  features: single-writer fencing, `WriteBatch`, `DbReader`, checkpoints,
  `MergeOperator`, TTL, compaction filters, WAL reader, and segment extractor.
  No code path may depend on range deletion.
- Pinned MSRV; reproducible builds.
- License headers, CONTRIBUTING, CODE_OF_CONDUCT.
- Dev container (Dockerfile + devcontainer.json) with SlateDB, MinIO,
  Postgres, Kafka pre-installed.

**Exit criteria**

- `cargo test --workspace` passes.
- `make e2e` brings up a local cluster (MinIO + 1 worker + 1 control) and tears
  it down.
- The oracle harness can drive a no-op pipeline and confirm equivalence.
- **SlateDB determinism gate**: a dedicated test drives a write-heavy `ShardDb`
  workload (mixed `put`, `merge`, `WriteBatch` commit, `DbReader` snapshot read,
  WAL tail) twice under `SimRuntime` at the same seed. The resulting SlateDB
  key–value state and WAL sequence must be bit-identical between the two runs.
  If any internal SlateDB background task (compaction, manifest flush, WAL
  rotation) uses wall-clock time or an uncontrolled RNG, the test will diverge
  and the failure must be resolved — either by finding a `SimRuntime`-compatible
  configuration of SlateDB, or by documenting the non-deterministic surfaces and
  constraining them out of the hot simulation paths — before Phase 1 begins.
  This test is the proof that the FoundationDB simulation property holds *through*
  SlateDB, not merely around it.

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
  operator instance returning `EpochOutput` fragments to a shard-level epoch
  commit coordinator.
- Implement shard-level group commit: coalesce all ready operator fragments for
  a shard into one or more atomic SlateDB `WriteBatch` commits covering state,
  output, shuffle staging, connector offsets, and frontiers.
- Per-shard SlateDB namespaces from DESIGN.md §5.1 (op_state, view_output,
  shard_meta) wired through `ShardDb`.
- `ShardDb` must expose: `put/merge/delete` fragment builders, checkpoint
  creation, `DbReader` snapshot reads, WAL reader smoke tests, segment extractor
  setup, and bounded prefix scan + batched delete cleanup.
- **WAL listing cache** in `ShardDb`: list WAL files once on attach, then tail
  via `WalReader::get(latest_id + 1)` and invalidate only on rotation. Listing
  must not appear on the hot path (validated by smoke test).
- **Async, ownership-free scheduler**: the per-worker scheduler runs operators
  as tokio tasks driven by data arrival and frontier updates, with credit-based
  backpressure. No `OwnershipConflict`-style rejection of multi-consumer
  streams.
- **Embedded runtime profile**: `rockstream start --role=all --storage=./data`
  wires control, worker, frontier, and gateway services in-process. The
  single-shard hot path must not issue gRPC calls or create shuffle objects;
  `EXPLAIN INCREMENTAL` reports any elided exchange boundaries.
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
- Group-commit benchmark: shard-level batching must reduce durability events
  by at least 5x compared with one commit per operator at the same epoch rate.
- Embedded freshness benchmark records p50/p95 commit-to-query visibility for
  1, 10, and 1000 rows/epoch; the run must show zero gRPC shuffle calls and
  zero durable shuffle objects.
- Oracle property test runs green for ≥ 100k randomized scenarios per
  operator combination.

**→ Operability deliverables (Phase 1)**

- **Single binary surface.** `rockstream` is one binary from day one;
  `rockstream start --storage=./data` is a zero-config developer command.
  Role flags exist but only `--role=all` is wired this phase.
- **Error-code registry.** Crate `rockstream-errors` defines every error as
  an `RS-XXXX` code with a doc-URL stub; CI fails the build if a returned
  `Error` or logged `error!` has no code. Doc URLs may 404 until Phase 10 but
  must exist in the registry.
- **Support-bundle skeleton.** `rockstream support bundle` collects plan,
  recent logs, and basic shard stats into a tarball. Redaction on by default.
- **Audit-log skeleton.** Every control action goes through a single
  `record_event(...)` helper that writes to `control: audit/{ulid}` and to
  structured logs. Only a handful of events exist yet; the surface is wired.
- **`SimRuntime` adoption from day one.** Every operator, scheduler, and
  storage call site is parameterised on the `Runtime` trait from
  `rockstream-sim` (DESIGN.md §17.1). Production binaries use
  `TokioRuntime`; every unit and property test uses `SimRuntime` with a
  seeded RNG so failures are deterministically reproducible. Retrofitting
  this later is the single most expensive mistake the project can make.
- **`buggify!()` discipline.** Race-prone code paths (partial `WriteBatch`
  failures, fenced-writer commit attempts, manifest publish delays) carry
  `buggify!()` annotations with a comment naming the race. CI fails any PR
  touching coordination code that omits an annotation reviewed by a second
  engineer.

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
- **Schema-version catalog**: source/view schemas are stored in
  `control: schema/`; compatible changes (nullable/default columns, lossless
  widening) are accepted online, while breaking changes produce
  `BLOCKED(RS-1002)` until a blue/green replacement plan is approved.
- SQL coverage delivered incrementally inside the milestones below, in this
  order: filter → project → group-by aggregates → inner join → outer joins
  → semi/anti → set ops → subqueries (correlated decorrelated by optimizer)
  → CASE/CAST/complex expressions. Window functions and `WITH RECURSIVE` are
  Phase 3.

### Milestone IVM-4 — Inner equi-join (IVM.md §13 IVM-4, §7.3)

- Add `InnerJoin` PlanNode + dual arrangements (`0xJL`, `0xJR` from
  DESIGN.md §6.4).
- Implement stable source-derived `row_id` handling. Replay must rewrite the
  same join/window/top-k arrangement key; keyless snapshots use
  `(snapshot_id, file_path, row_group, row_ordinal)`, never random replay IDs.
- Implement a DBSP-native two-arrangement join and validate it against the
  corrected bilinear-expansion behavior in
  [`pg-trickle1/src/dvm/operators/join.rs`](../pg-trickle1/src/dvm/operators/join.rs):
  - Part 1 — `ΔL ' R` split into `ΔL_I ' R₁` and `ΔL_D ' R₀` (EC-01 fix).
  - Part 2 — `L₀ ' ΔR` with appropriate pre-change snapshot construction.
  - Part 3 — correction term `(L₁ − L₀) ' ΔR` for join children (Q07 fix).
- Pre-change snapshot semantics: arrangements are updated at end-of-epoch
  commit, so during processing they reflect epoch `e-1`.
- Planner metadata: `JoinSemantics` records inside-semi/anti context,
  join-child depth, pre-change snapshot mode, key-change tracking, and which
  node owns correction output.
- Distribution pass inserts `Exchange` whenever the join key differs from the
  child's partition key (no-op in single shard; verified by tests).
- Run TPC-H Q1, Q3, Q5 (5-way join), Q6 against the batch oracle for parity.
- Property test: random 3-way join over random insert/update/delete sequences.

### Milestone IVM-5 — Outer / Semi / Anti joins (IVM.md §13 IVM-5, §7.4–7.5)

- Add `LeftJoin`, `RightJoin`, `FullJoin`, `SemiJoin`, `AntiJoin` variants.
- Implement DBSP-native operators validated against pg_trickle's implementations
  ([`outer_join.rs`](../pg-trickle1/src/dvm/operators/outer_join.rs),
  [`full_join.rs`](../pg-trickle1/src/dvm/operators/full_join.rs),
  [`semi_join.rs`](../pg-trickle1/src/dvm/operators/semi_join.rs),
  [`anti_join.rs`](../pg-trickle1/src/dvm/operators/anti_join.rs)) with
  side-specific NULL-padding logic and the Q21 SemiJoin correction.
- One extra arrangement per side tracking currently-unmatched rows so
  transitions can emit retractions.
- **Planner optimizations from pg_trickle** (implemented as `JoinSemantics`
  metadata, not as SQL CTE rewriting): SemiJoin `R_old` materialization
  (Q21 fix), DI-6 equi-join key filter pushdown on the SemiJoin right side,
  `merge_safe_dedup` flag for scan-filter-project chains, and FULL JOIN
  aggregate rescan when an upstream FULL JOIN can produce matched\u2194unmatched
  transitions under a SUM/AVG.
- Run TPC-H Q11, Q21 (the notorious SemiJoin corner cases) against the oracle.

### Milestone IVM-6 — Distinct / Union / Intersect / Except (IVM.md §13 IVM-6, §7.7–7.8)

- `0xDS` weight-based arrangement (DESIGN.md §6.6) with
  `DistinctWeightMerge` (`i64` addition).
- Output delta on zero-crossing transitions (0 → +n emits +1;
  +n → 0 emits −1).
- Zero-crossing entries are explicitly deleted/tombstoned when immediate
  invisibility is required. A compaction filter may remove obsolete merge
  operands only after a snapshot-safety audit.
- Implement Intersect / Except with set + bag semantics; validate against
  pg_trickle's `intersect.rs` / `except.rs`.
- Property tests on set semantics with random sequences.

**Exit criteria for Phase 2**

- Plain-SQL view DDL works end-to-end: a user can submit
  `CREATE VIEW v AS SELECT ... FROM t1 JOIN t2 ON ... GROUP BY ...` and the
  engine compiles, deploys, and maintains it incrementally.
- TPC-H Q1, Q3, Q5, Q6, Q11, Q21 all pass parity vs. DataFusion batch.
- All compiled plans round-trip through Substrait without loss.
- Property-test harness extends to every operator combination implemented
  so far.

**→ Operability deliverables (Phase 2)**

- **`EXPLAIN INCREMENTAL`** prints the annotated operator tree from
  DESIGN.md §14.8 against live statistics for any installed view.
- **`EXPLAIN INCREMENTAL ESTIMATE`** runs the planner and cost model
  *without* deploying; reports predicted state size, per-operator
  `epoch_ms`, object-store request rate, and minimum achievable frontier
  lag. Estimation accuracy is tracked over time on the TPC-H suite.
  Estimates are labelled `confidence=low` when only heuristic fallback stats
  are available (DESIGN.md §4.0).
- **Source statistics pipeline**: `discover_stats()` wired for Kafka (commit
  offsets) and Postgres CDC (`pg_class.reltuples`) connectors; stats cached in
  `catalog/table/{id}/stats`; live metrics feed back after 60 s of operation;
  `ANALYZE TABLE` command refreshes on demand (DESIGN.md §4.0).
- **`CREATE PIPELINE … WITH (…)`** SQL grammar parses `freshness_target_ms`,
  `state_budget_gb`, `object_store_rps`, `priority`, `max_parallelism`,
  `max_shards`. Values are stored in catalog; enforcement lands in Phase 3
  (state budget) and Phase 4 (parallelism/shard caps).
- **Multi-view pipeline DDL.** `CREATE PIPELINE ... AS (...)` accepts multiple
  `CREATE SOURCE` and `CREATE VIEW` statements, compiles them into one shared
  DAG, and stores dependency metadata for `ALTER PIPELINE ... ADD/REPLACE VIEW`.

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
- Event-time TTL on arrangement entries plus a frontier-aware compaction filter
  that removes state only after event-time expiry and input/output frontiers
  prove safety.
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
- Compiler strategy selection:
  - Semi-naive for monotone insert-only recursion.
  - DRed for monotone mixed insert/delete/update recursion.
  - Full recomputation fallback for non-monotone terms, unsupported multiple
    self-references, or recursive/output column mismatches.
- Implement the nested-time scheduler loop:
  - Outer time = `source_epoch`; inner time = `iteration` (resets per epoch).
  - At each iteration, evaluate the step plan against the arrangement at
    `iteration - 1`, distinct-collapse the result, emit deltas.
  - Convergence: inner frontier advances past `iteration` with no new
    deltas → loop exits, output frontier on the operator advances to
    `{source_epoch + 1, 0}`.
- Safety controls: max iteration count, frontier-stall detection, and explicit
  error reporting when fallback recomputation exceeds configured cost limits.
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
- Model pg_trickle's `dag.rs` semantics: per-stream-table cadence inheritance,
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

**→ Operability deliverables (Phase 3)**

- **Per-pipeline state-budget enforcement.** The runtime accounts
  `op_state_bytes` per pipeline; reaching `state_budget_gb` transitions the
  pipeline to `OVER_BUDGET_RELAXED` (DESIGN.md §14.10), surfaces a named
  `RS-2002` reason, and records the transition in the audit log. No silent
  growth past the budget.
- **Object-store RPS quota.** Token-bucket admission on the per-shard
  commit path enforces `object_store_rps`; over-limit transitions to
  `RPS_THROTTLED`.
- **Degraded-state surface.** `pipeline_slo_compliance` and
  `pipeline_degraded_reason` metrics ship; `SHOW PIPELINE` reports the
  current state. End-to-end test: a deliberately-too-tight
  `freshness_target_ms` produces a visible degraded reason within one
  observation window.

---

## Phase 3.5 — IVM Correctness Soak

**Goal**: Prove the IVM engine is production-grade *before* layering on
distribution and fault tolerance. (IVM.md §13 IVM-13.)

**Deliverables**

- **TPC-H 22/22**: adapt pg_trickle's TPC-H test suite (queries Q1–Q22 at
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
  Paired-assertion checks are mandatory for every durable arrangement write,
  frontier advance, epoch replay, and sink idempotency key.
- **Storage correctness audit**: verify every cleanup path works without SlateDB
  range deletion; prove each compaction filter is snapshot-safe; run a WAL
  retention/listing-cost test with long-lived readers.
- **Merge-read correctness test**: for every merge-backed arrangement, prove
  `ShardDb::get_merged()` / `scan_merged()` observes all visible merge operands
  at the epoch snapshot. If the storage profile cannot support this, the test
  must force the read-modify-write fallback and update cost estimates.
- **Commit-cost benchmark**: compare shard-level group commit against
  per-operator commits at 10, 100, and 1000 operators per shard.
- **Object-store request budget**: measure GET/LIST/PUT/DELETE rates for
  arrangements, shuffle, checkpoint, WAL reader, and compaction under soak.
- **Manifest churn budget**: measure manifest writes per minute under steady
  state and bursty load; confirm `min_epoch_ms` / `min_epoch_bytes` floors
  hold the write rate within budget without starving frontier progress past
  `max_epoch_ms`.
- **WAL listing-cost test**: keep a `DbReader` open against a writer at 1-hour
  WAL retention; assert that no operator hot path issues `list()` and that
  cached tail reads stay below an explicit per-shard request/s budget.
- **Per-shard adaptive cost model**: validate that a hot shard switching to
  recomputation while sibling shards stay on DIFFERENTIAL produces correct
  outputs and does not stall the cluster frontier.
- **Performance regression suite**: criterion benchmarks tracked over time;
  CI fails on > 10% regression.

**Exit criteria**

- 22 / 22 TPC-H queries: identical results vs. batch.
- ≥ 10× measured speedup vs. batch at 1% change rate (matches pg_trickle's
  TPC-H number).
- No correctness-critical cleanup depends on range deletion; compaction filters
  have documented safety proofs and failing tests for unsafe resurrection cases.
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
  - Exchange path classifier from DESIGN.md §7.5: `elided`, `loopback`,
    `direct`, and `durable`.
  - Worker-to-worker connection pooling/multiplexing: one stream per peer
    worker per traffic class, with shard/exchange IDs in the frame header.
  - Same-worker loopback path using bounded in-process channels while keeping
    durable outbox/inbox metadata for replay.
  - Pre-shuffle combiner for compiler-certified associative payloads, including
    Z-set weight cancellation and partial SUM/COUNT/AVG aggregation.
  - Hierarchical exchange domains controlled by `exchange_domain_size` for
    large worker counts.
  - Object-store fallback writer & reader.
  - Coalesced durable shuffle objects: one object may contain many shard-to-
    shard frames plus an index footer. Receivers never LIST the shuffle prefix
    on the hot path; they consume outbox metadata / notifications.
  - Hybrid dispatcher: chooses path per-batch based on receiver health and
    batch size.
  - `shuffle_outbox/` and `shuffle_inbox/` encoders integrated into the
    shard-level epoch commit batch.
  - Credit-based backpressure.
- **Rendezvous hashing** library with virtual nodes; property tests for
  re-balance minimality.
- **Distribution-aware execution**:
  - Operator instances are addressable by `(op_id, instance_idx)`.
  - Placement is locality-aware: compatible adjacent operators should be
    co-located when the cost model predicts serialization/network cost exceeds
    the benefit of wider parallelism.
  - The scheduler on each worker runs only the `OperatorTask`s (IVM.md §8.2)
    whose `instance_idx` is assigned to its shards.
  - Exchange operators serialize Arrow batches keyed by destination shard
    and stage them in `shuffle_outbox/` as part of the per-shard atomic
    commit (DESIGN.md §9).
  - Cross-shard arrangement reads are forbidden in the hot path: the
    compiler's distribution pass guarantees that every stateful operator's
    inputs share its `partition_key`, inserting `Exchange` whenever they
    don't (IVM.md §5, §9.4).
  - Re-run the full Phase 1–3 oracle + TPC-H suite against the distributed
    cluster; results must be bit-identical to the single-shard runs.
- **Distributed recursion**: extend the recursion runtime (IVM.md §11.1) so
  `Exchange` operators can appear inside a recursive scope. The inner-iteration
  frontier participates in the standard antichain aggregation. Validate with
  a sharded transitive-closure / reachability benchmark on a 10M-edge graph;
  enforce max-iteration cap, inner-frontier stall timeout, and per-shard
  recompute fallback.

**Exit criteria**

- 16-shard cluster (single host, 16 processes) runs TPC-H with near-linear
  throughput vs. single shard for partitionable queries, with documented skew
  and shuffle limits.
- Same-worker loopback path produces bit-identical output to direct gRPC and
  shows zero worker-to-worker network calls for co-located exchanges.
- Pre-shuffle combiner benchmark documents bytes avoided for partitioned
  aggregate workloads and proves no row-preserving operator is combined unless
  explicitly certified.
- Killing one worker process causes its shards to be re-leased to another
  worker; processing continues without data loss (verified by output equality
  vs. uninterrupted run).
- Connection count is bounded by worker count, not shard count; a 1,000-shard
  exchange stress test with hierarchical domains must stay within configured
  connection and durable shuffle-object budgets.

**→ Operability deliverables (Phase 4)**

- **Real role flags.** `--role=control|worker|gateway|all` selects which
  services the node runs; the multi-host Tier-3 setup in DESIGN.md §14.2
  works against this phase's binary unchanged.
- **Auto-pause for unrecoverable shards.** A shard that loses its fence or
  fails recovery transitions the owning pipeline to `BLOCKED(RS-3001)` or
  `BLOCKED(RS-3002)` with full event-log trail; never crashes the cluster.
- **Audit-log entries for shard moves.** Every shard add / remove /
  rebalance is recorded with the trigger (operator request, lease loss,
  rebalancer decision).

---

## Phase 5 — Frontier Protocol

**Goal**: Correct progress tracking across multi-input operators.

**Deliverables**

- `rockstream-types::Frontier`: full antichain implementation with
  product-order timestamps. Property tests for meet/join/advance.
- **Per-shard frontier reporter**: bundled in every epoch commit
  (`shard_meta/0x06 0xFR`).
- **Control-plane frontier aggregator**: consumes worker-level frontier
  summaries from DESIGN.md §8.6, computes per-operator cluster frontier,
  publishes to `frontier/op_id` in the control DB, and can rebuild summaries
  from per-shard `shard_meta/0x06 0xFR` after worker loss.
- **Operator frontier consumers**: each operator reads its input frontier from
  the control plane (cached, push-updated via gRPC subscription), and uses it
  to:
  - Trigger window closing.
  - Detect recursion convergence.
  - Release shuffle inbox entries.
- **Exchange GC**: senders observe `frontier/exchange_e/consumed` and reclaim
  outbox/inbox entries with bounded prefix scan + batched deletes; long-retained
  entries may be removed by frontier-aware compaction filters after audit.

**Exit criteria**

- A query with a join over two sources at different ingestion rates produces
  correct output (no premature emission, no infinite buffering).
- Recursive query converges deterministically; frontier advances past
  iteration timestamps after convergence.
- Shuffle storage usage is bounded under sustained throughput.
- Frontier aggregation stress test covers thousands of shards × hundreds of
  operators without the control plane subscribing to each shard feed directly.

**→ Operability deliverables (Phase 5)**

- **SLO-driven planner.** The control plane derives `min_epoch_ms`,
  `max_epoch_ms`, and initial per-operator parallelism from each pipeline's
  declared `freshness_target_ms` and quotas (DESIGN.md §14.3). Manual knobs
  remain as overrides; the audit log records both the derived value and any
  override.
- **Adaptive parallelism loop.** Implements the control loop from
  DESIGN.md §14.5 (hysteresis bands; bounded by `max_parallelism`); every
  scale decision is audit-logged with the metric reading that triggered it.
- **Adaptive epoch sizing.** Same pattern, bounded 10 ms–5 s.
- **Auto-tuner property test.** A random workload sequence must reach a
  stable parallelism within bounded time (no oscillation).

---

## Phase 6 — Fault Tolerance & Exactly-Once

**Goal**: Survive any single-node failure; deliver exactly-once end-to-end.

**Deliverables**

- **Cluster checkpoint coordinator** (control-plane component):
  - Barrier injection at sources.
  - Barrier alignment at multi-input operators.
  - Bounded barrier alignment buffers tied to shuffle credits; exhausted
    credits propagate backpressure instead of growing memory.
  - One per-shard `Checkpoint` creation after all local operators have durably
    committed through the barrier (not one checkpoint per operator).
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
- **Worker self-fencing** (DESIGN.md §11.6): workers that cannot reach the
  control plane for `self_fence_after` seconds terminate themselves. Simulated
  in `SimRuntime` by injecting a control-plane partition; the partitioned
  worker must fence before the new owner acquires its leases.
- **Object store brownout handling** (DESIGN.md §11.7): workers buffer up to
  `local_buffer_max_epochs` epochs when the object store is unreachable, then
  apply backpressure to sources. Recovery is transparent when the store
  returns. Test: inject a 60-second object-store blackout; verify no data
  loss, no duplicates, and frontier resumes within the SLO after recovery.
- **Thundering herd mitigation** (DESIGN.md §11.8): staggered worker startup
  using `worker_id mod jitter_buckets` delay; control plane rate-limits lease
  grants at `max_lease_grants_per_second`. Test: restart all 32 workers
  simultaneously; verify no false failure detections and object-store request
  spike stays within 2× normal.
- **Simulation-test coverage** (under `SimRuntime` with `BUGGIFY` enabled,
  DESIGN.md §17.3):
  - Epoch commit interleavings across N shards — every partial-failure
    permutation leaves the cluster frontier monotonic and exactly-once
    intact.
  - Frontier protocol — arbitrary report reorderings converge to the same
    cluster vector frontier as serial delivery.
  - Checkpoint barrier alignment under credit exhaustion — never deadlocks;
    surfaces `RECOVERING` if it cannot complete.
  - 2PC sink crash points — pre-commit / between / commit all recover
    idempotently.
  - Network partition self-fencing — partitioned worker terminates before
    the new owner commits to a previously-leased shard.
  - Object store brownout — 50-epoch blackout produces zero loss/duplicates.
- **Recovery-time SLO instrumentation**: emit `failure_detection_seconds`,
  `shard_recovery_seconds`, `pipeline_freshness_recovery_seconds` histograms
  (DESIGN.md §11.5). Pipelines that miss the 60 s freshness-recovery budget
  surface `RECOVERING_SLOW`.

**Exit criteria**

- 24-hour chaos run on a 32-shard cluster with continuous Kafka input and
  Kafka output: zero data loss, zero duplicates, output matches reference.
- Recovery from full cluster outage in < 60 s for state size < 1 TB.
- **Recovery-time invariants (DESIGN.md §11.5) hold at
  `target_shard_state_bytes` (default 20 GB)**: failure detection ≤ 5 s
  (p99), single-shard reassignment ≤ 30 s (p99), pipeline freshness
  recovery ≤ 60 s (p99). Measured under the chaos suite, not synthetic
  micro-benchmarks.
- **Simulation seeds**: ≥ 100k seeded `SimRuntime` runs across the
  coordination suite pass cleanly; any failing seed is checked in as a
  regression test.
- **Continuous simulation soak infrastructure starts here**: a scheduled CI
  job runs new seeded `SimRuntime` executions against `main` around the
  clock from v0.36 onward. Failing seeds are minimized using the standard
  seed-bisection tool, stored as regression seeds in the repository, and
  block release until either fixed or explicitly accepted with a documented
  limitation. The soak starts small (hundreds of seeds/night) and scales
  to millions of seeds/night by v0.50. The CI job is the evidence that the
  FoundationDB simulation discipline is active, not aspirational.
- Routine worker restart surfaces `RECOVERING` with `recovery_progress` and
  suppresses false SLO alerts until `recovery_deadline`; missed deadlines alert.

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
  - Donor shard retires migrated keys and reclaims them via bounded
    scan-and-delete or a frontier-aware compaction filter after cutover.
- **Proactive shard splitter** (DESIGN.md §10.6): each shard reports its
  total state footprint on every epoch; the control plane schedules a split
  when footprint crosses `1.5 × target_shard_state_bytes`
  (default `target = 20 GB`). Splits are rate-limited to one per minute per
  shard and respect the auto-tuner budget. The `target_shard_state_bytes`
  knob is settable per storage profile.
- **Online shard merge**: reverse of split. Cold-shard merge driven by
  `min_shard_state_bytes` floor (default 4 GB) to prevent fragmentation.
- **Worker scale-out**: new worker process joins, control plane assigns
  un-leased shards or rebalances from over-loaded workers.
- **Worker drain protocol** (DESIGN.md §10.7):
  - `DRAINING` state in `topology/worker/`: no new shard assignments.
  - Each shard on the draining worker migrates via the checkpoint-copy path.
  - `DECOMMISSIONED` state once all shards have cut over.
  - `rockstream cluster workers drain <worker-id>` CLI command.
  - Audit event for every state transition.
- **Worker capacity model** (DESIGN.md §10.8): workers report
  `capacity_headroom` (remaining shard slots based on observed memory, I/O,
  CPU utilisation). The placement algorithm refuses to assign shards to a
  worker at zero headroom.
- **Cluster autoscaling signals** (DESIGN.md §10.8):
  `cluster_worker_pressure`, `demanded_shard_count`, `placed_shard_count`
  exported as Prometheus metrics; k8s HPA / KEDA drives scale-out/in.
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
- **Proactive split test**: drive a single shard's state footprint to 30 GB;
  the control plane initiates a split before the shard exceeds operational
  thresholds, with no operator alert and no observable freshness-SLO impact.
- **Worker drain test**: drain a worker hosting 4 shards; all shards complete
  migration within 120 s; no epoch is lost; worker reaches `DECOMMISSIONED`.
- **Capacity headroom test**: place shards until a worker reaches zero
  headroom; subsequent placement attempts route to other workers.

---

## Phase 9 — Connectors & Sinks

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
- **Connector contract**: built-in Rust traits and external gRPC protocol share
  the same `discover_schema`, `start_snapshot`, `poll_delta`, `commit_offset`,
  `prepare`, `commit`, `abort`, and `should_flush` surface from DESIGN.md
  §13.3. The contract includes the v3.8 additions (opaque `OffsetToken`,
  `watermark: Option<EventTimeWatermark>`, `credits_available()`) plus the
  two v3.9 additions: `start_snapshot` and `poll_delta` accept an optional
  `PartitionFilter` (planner-derived column predicates) so Iceberg/Delta/Hudi
  connectors skip non-matching partition directories at the source rather than
  in the operator layer; and sink connectors expose `should_flush(bytes, epochs)
  -> bool` so file-format sinks buffer across epochs and write properly-sized
  Parquet files — pending rows are staged as `connector/{id}/pending_buffer` in
  the shard SlateDB and participate in every epoch checkpoint for exactly-once
  recovery.
- **Dead-letter sink routing**: per-record decode errors become `RS-1003`
  events and are routed to a configurable DLQ sink. Implemented as a
  connector-tier concern; the IVM core never sees malformed records.
- **Per-connector source-epoch vector** (DESIGN.md §8.1.1): each connector
  maintains a strictly increasing `source_epoch` and persists
  `control: connector/{id}/epoch_map/{source_epoch} → { partition →
  committed_offset }` atomically with the epoch commit. Exactly-once
  recovery looks up the highest committed `source_epoch` and resumes from
  the recorded partition offsets.
- **View output retention** (DESIGN.md §5.7): support
  `CREATE VIEW WITH (retention = '7d')` (and `MATERIALIZED VIEW` default
  forever); enforce via SlateDB TTL + compaction filter that keeps the
  current value per primary key regardless of age. Retention bytes counted
  against the pipeline's `state_budget_gb` quota and shown in
  `EXPLAIN INCREMENTAL ESTIMATE`.
- **Schema evolution integration**: connectors publish schema versions before
  data; incompatible drift returns `RS-1002` and blocks consumption before any
  offset advances.
- **Connector marketplace structure**: SDK + example crates; documented
  contract.

**Exit criteria**

- End-to-end: Postgres CDC → RockStream IVM → Kafka, sustained at 100k rows/s
  for 24 hours with exactly-once.

---

## Phase 8 — Query Gateway & Postgres Compatibility

**Goal**: Serve materialized views to applications over the Postgres wire
protocol. Make RockStream self-contained (no external broker required).

**Design constraint (DESIGN.md §12.7.3)**: The gateway's `ViewReader` trait
must be cold-tier-aware from this phase. Define `ViewReadStrategy` (with both
`HotOnly` and `TwoTier` variants) and the `ViewReader` trait in
`rockstream-gateway` now. Only `HotOnly` is implemented here; the `TwoTier`
path is a future deliverable. This keeps the cold tier addable later without
a gateway rewrite.

**Deliverables**

### v0.40 — Postgres Read Gateway (core)

- **`ViewReader` / `ViewReadStrategy` abstraction** (DESIGN.md §12.7.3):
  - Define `ViewReadStrategy` enum (`HotOnly` | `TwoTier { snapshot_manifest, hot_tail_from_epoch }`) in `rockstream-gateway`.
  - Define the `ViewReader` trait with a `read_strategy()` method the planner calls before routing a query.
  - Implement `HotOnly` fully. `TwoTier` variant is present but returns `RS-4001 cold_tier.not_enabled` if selected.
- **HTTP server routing reservation** (DESIGN.md §13.7.2): `--role=gateway`
  starts an HTTP server on port `8181` alongside pgwire on `5432`. Register
  the `/iceberg/v1/` route prefix now (returns `501 Not Implemented`). This
  ensures the gateway HTTP surface is catalog-aware before the Iceberg REST
  catalog implementation lands, avoiding a routing rewrite later.
- **pgwire gateway** (stateless, horizontally scalable):
  - Postgres wire protocol (`pgwire` crate): startup, query, extended-query,
    copy-out, terminate message flows.
  - Routes lookups \& range scans to the correct shards via `DbReader`.
  - Ad-hoc SQL over materialized views (DataFusion on a snapshot).
  - Connection pooling, query timeouts, rate limiting.
- **Postgres catalog stubs** required by ORMs:
  - `pg_catalog.pg_tables`, `pg_views`, `pg_class`, `pg_attribute`,
    `pg_namespace`, `pg_type` — generated from the control-plane catalog.
  - `information_schema.tables`, `information_schema.columns`.
  - `SHOW server_version`, `SHOW transaction_isolation`, `SET search_path`
    stub responses.
- **Postgres type OID mapping**: every column in every view carries a
  Postgres-native OID in the row-description message so JDBC/ODBC drivers
  decode without metadata round-trips.
- **Session isolation levels** (DESIGN.md §12.6):
  - `READ COMMITTED`: each statement pins to latest published vector frontier.
  - `REPEATABLE READ`: `BEGIN` captures a vector frontier; all statements in
    the transaction see that snapshot; `COMMIT`/`ROLLBACK` releases it.
  - `SERIALIZABLE`: rejected with `RS-2003 isolation.serializable_not_supported`.

**Exit criteria for v0.40**

- `psql` connects, runs `SELECT * FROM my_view LIMIT 10`, returns < 10 ms.
- SQLAlchemy ORM reflects view schema without errors.
- `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`.

---

### v0.41 — Gateway Introspection and Read Performance

- **Cross-shard partial aggregation pushdown** (DESIGN.md §12.3.1): for queries
  of the form `SELECT agg, key FROM mv GROUP BY key`, the gateway pushes partial
  aggregation to shards and merges O(groups) rows rather than O(view rows).
- **`rockstream` system schema** (DESIGN.md §12.6.1): virtual tables
  (`rockstream.epochs`, `rockstream.pipelines`, `rockstream.views`,
  `rockstream.shards`, `rockstream.connectors`, `rockstream.audit_log`,
  `rockstream.schema_history`) projecting control-plane state through the
  standard SQL interface. No additional storage required.
- **Arrangement segment cache** (DESIGN.md §5.4): per-worker LRU cache
  keyed by `(shard_id, segment_id)`, bounded by `segment_cache_bytes`
  (default 512 MB). Populated on `DbReader` segment fetches for join lookups
  and gateway reads; invalidated on compaction via manifest-poll. Reported
  as `segment_cache_hit_ratio` and `segment_cache_bytes_used` metrics.

**Exit criteria for v0.41**

- `SELECT COUNT(*), region FROM mv GROUP BY region` pushes partial agg to
  shards; gateway receives O(groups) rows, not O(view rows).
- `SELECT * FROM rockstream.epochs WHERE pipeline_id = 'orders'` returns
  committed epoch history without additional storage writes.
- Segment cache hit ratio > 80% for a hot-join benchmark with a working set
  that fits within `segment_cache_bytes`.

---

### v0.42 — Freshness, Subscribe, Isolation, and Historical Queries

- **Subscribe API**: gRPC streaming endpoint that tails view changes (via
  `WalReader` on the relevant shards). Gateway proxies subscriptions; raw
  shard access is never exposed to clients.
- **Freshness tokens**: query responses return the vector frontier used;
  clients can pass `wait_for=<token>` for read-your-writes semantics with a
  timeout and explicit satisfied/not-satisfied response.
- **Historical queries** (DESIGN.md §12.4.1):
  - `AS OF EPOCH <n>` resolves to the nearest committed cluster checkpoint
    whose frontier dominates epoch `n` on all relevant shards.
  - `AS OF TIMESTAMP <t>` resolves to the checkpoint whose commit wall-clock
    time is the greatest value ≤ `t`.
  - Bounded by view retention (§5.7); queries beyond retention return
    `RS-2005 history.epoch_before_retention`.
  - Configurable `checkpoint_retention_count` (default 128) and
    `checkpoint_retention_duration` (default min(view retention, 7d)) control
    how far back historical queries can reach.
- **Internal (direct-write) source connector** (DESIGN.md §13.5):
  - `INSERT`/`UPDATE`/`DELETE` DML over the Postgres wire protocol appended to
    a per-connection write buffer.
  - `COMMIT` flushes as an atomic Z-set delta via `WriteBatch` to a dedicated
    base-table shard, receiving the shard's next `source_epoch`.
  - `ROLLBACK` discards the buffer without shard writes.

**Exit criteria for v0.42 (Phase 8 complete)**

- `psql` runs `INSERT INTO t VALUES (...); COMMIT` and view reflects it within
  `freshness_target_ms`.
- Read-your-writes demo passes.
- Subscribe stream survives gateway restart with no data loss.
- `SELECT * FROM orders_mv AS OF EPOCH <past>` returns the correct historical
  snapshot; queries beyond retention return `RS-2005`.
- `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`.

---

**Additional Phase 8 deliverables (cross-cutting)**

- **Authentication / authorization**: OIDC / bearer-token auth at the gateway;
  per-view RBAC with `viewer` / `pipeline_owner` / `admin` roles stored in the
  control-plane catalog (DESIGN.md §12.5). `rockstream login` CLI flow for
  human principals; service-account key files for automated clients.
- **Cluster bootstrap ceremony**: `--bootstrap` flag for first control node;
  subsequent control nodes join the Raft group via `--control=<url>`; documented
  join/leave procedure for Raft voters (DESIGN.md §3 Cluster Bootstrap).
- **Storage format version gate**: binary reads `shard_meta/0x06 0xFV` on
  shard open; refuses if version out of supported range (DESIGN.md §5.5,
  error `RS-5001`). `rockstream migrate` tool skeleton.

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
  - `cluster workers {list, drain, status}`
  - `shard list/migrate`
  - `checkpoint list/restore`
  - `debug arrangement <view> <op_id> <key>` (DESIGN.md §14.7.1)
- **Web console** (optional, post-MVP): pipeline graph viewer, frontier lag
  charts, live throughput.
- **Chaos testing automation**: Jepsen-style test harness.
- **`rockstream chaos`**: in-tree fault-injection subcommand (DESIGN.md
  §14.17). Worker kills, object-store latency, shard fence loss, connector
  stalls; recovery is observable through `pipeline_slo_compliance` and the
  audit log.
- **Simulation-test CI gate** (DESIGN.md §17): every commit runs N seeded
  `SimRuntime` executions across the coordination suite (epoch commit,
  frontier, checkpoint, 2PC sink, reassignment, schema evolution) with
  `BUGGIFY` enabled. Pre-release runs scale N to millions of seeds; failing
  seeds are checked in as regression tests and replayed on every subsequent
  build. The gate checks both safety (oracle divergence, invariant assertion,
  invalid recovery state) and liveness (a recoverable fault must either commit
  a new epoch inside the 5 s / 30 s / 60 s recovery budgets or surface a named
  degraded state).
- **Continuous simulation soak** (DESIGN.md §17.6): a scheduled job runs new
  deterministic seeds against `main` around the clock. Failures are minimized,
  stored as regression seeds, and block release until either fixed or explicitly
  accepted with a documented limitation.
- **Frontier aggregator deployment** (DESIGN.md §3.2): document and ship
  the `rockstream start --role=frontier` deployment topology for Tier 3.
  Frontier-role processes are stateless and horizontally scalable; the
  Raft control group remains 3–5 nodes regardless of cluster shard count.
- **Full error-code documentation**: every `RS-XXXX` in the registry has a
  published doc page with cause, detection signal, and remediation. CI gate
  enforces.
- **Auto-tuner hardening**: long-running stability tests across diverse
  workload mixes; tune hysteresis defaults; document override patterns.
- **Support-bundle completeness**: redaction integration test asserts no
  credential pattern leaves the bundle by default; bundle includes audit-log
  entries, plan history, metric snapshots, frontier history, recent worker
  logs.
- **Performance baselines**: Nexmark, TPC-H continuous, recursive graph
  workloads with documented numbers.
- **Documentation**:
  - Operator's guide.
  - SQL reference (delta from ANSI SQL).
  - Connector development guide.
  - Deployment playbooks (k8s, ECS, bare-metal).
- **Security**:
  - mTLS everywhere (worker↔control, worker↔worker, gateway↔client);
    certificate rotation documented (DESIGN.md §3 Cluster Bootstrap).
  - At-rest encryption via object-store features.
  - **Secrets management** (DESIGN.md §14.18): `CREATE SECRET` DDL,
    envelope encryption with configurable KEK source (env, AWS KMS, GCP KMS,
    Vault), worker-side secret-token resolution, rotation without pipeline
    restart, audit trail for all secret lifecycle events.
  - Auth integration tests: unauthenticated requests rejected; cross-tenant
    pipeline access denied; audit log `actor` field populated on every event.
  - Rolling-upgrade integration test: deploy N→N+1 with one worker at a time;
    assert no epoch loss and format-version gate fires on incompatible binary.

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

## Phase 12 — Cold-Tier Sink & Iceberg REST Catalog

**Goal**: Make RockStream's pre-computed views consumable by any tool in the
data lake ecosystem (DuckDB, Trino, Spark, dbt) without those tools
needing to talk to RockStream's gateway. Implement the two-tier storage model
designed in DESIGN.md §12.7 and the cold-tier sink/catalog designed in §13.6–§13.7.

**Prerequisite decision gate**: by Production Beta (v0.51), the team evaluates
whether cold-tier storage is worth implementing or whether pushing Iceberg
snapshots to external catalogs via sinks (§13.6.5 path only) is sufficient.
If cold tier is confirmed, Phase 12 proceeds.

**Decision criteria — "no" case.** Phase 12 does NOT proceed if all of the
following hold:

1. **No pilot customer requires full-scan analytics on view output.** If every
   known use case reads views by primary key or narrow predicate (the hot path
   the LSM is good at), the cold tier adds cost without value.
2. **External-catalog-push via §13.6.5 satisfies tool discoverability.** If
   pilot customers can point DuckDB/Spark/Trino at a sink-written Iceberg path
   and the freshness lag of periodic sink flushes is acceptable, the
   gateway-served two-tier merge is unnecessary.
3. **No measurable user demand for `AS OF` full-collection scans.** If
   historical queries are limited to point lookups or narrow key ranges, the
   LSM reader at a past checkpoint is sufficient; Parquet cold snapshots
   provide no speedup.

If any one condition is false, Phase 12 proceeds. The decision is recorded in
the audit log with the evidence considered.

---

### v0.52 — Cold-Tier Parquet/Iceberg Sink

- **Iceberg cold-tier sink writer** (DESIGN.md §13.6):
  - `CREATE SINK ... TO ICEBERG '<path>' WITH (...)` DDL processing.
  - `should_flush()` gated by `snapshot_interval_epochs` / `snapshot_interval_ms`.
  - Pending-buffer staging in shard SlateDB (`connector/{id}/pending_buffer`).
  - Parquet data file writer: one file per shard partition, column stats
    (min/max/null count), configurable `parquet_row_group_bytes`.
  - Iceberg manifest file writer: per-data-file entries with Parquet stats.
  - Manifest list (snapshot) file writer.
  - Atomic `metadata.json` pointer commit (optimistic CAS on version-hint).
  - Connector offset update: `last_snapshot_epoch`.
  - Crash-recovery: idempotent replay from pending_buffer produces identical
    Parquet files (keyed by `{shard_id}-{epoch}`).
- **`ViewReader` `TwoTier` implementation** (DESIGN.md §12.7.3):
  - Gateway resolves `TwoTier { snapshot_manifest, hot_tail_from_epoch }`.
  - Cold read: DataFusion Parquet scan over the snapshot's data files.
  - Hot tail read: `DbReader` scan for epochs > `snapshot_epoch`.
  - Merge: union with deduplication by `row_id`.
  - Planner threshold: `cold_tier_scan_threshold` (default 10M rows).
- **Delta Lake variant** stub: `CREATE SINK ... TO DELTA` with
  `_delta_log/` JSON transaction entries. Feature-flagged behind
  `--experimental-delta-sink`.
- **Cold snapshot GC** (DESIGN.md §13.6.6):
  - `cold_snapshot_retention_count` per-sink (default: 32 snapshots).
  - `cold_snapshot_retention_duration` per-sink (default: 7 days).
  - GC runs after each successful snapshot commit: delete Parquet data
    files and manifest files not referenced by any retained snapshot.
  - Iceberg metadata rollback: `metadata.json` always points at the
    latest snapshot; old manifests that reference only expired data files
    are removed.
  - GC is idempotent: re-running after a crash does not delete live data.
  - Metrics: `cold_snapshot_count`, `cold_snapshot_bytes`,
    `cold_gc_last_run_epoch`, `cold_gc_bytes_reclaimed`.

**Exit criteria for v0.52**

- DuckDB `iceberg_scan('s3://...')` reads a valid Iceberg v2 table written
  by RockStream.
- Full-scan query over a 100M-row cold-tier view completes 10x faster than
  the same query against hot LSM.
- Crash mid-flush (kill during step 2) produces no orphan data files after
  recovery flush.
- Cold snapshot GC keeps ≤ `cold_snapshot_retention_count` snapshots;
  expired Parquet files are deleted within one GC cycle.
- `EXPLAIN INCREMENTAL` shows `TwoTier` read strategy when cold tier is
  active.

---

### v0.53 — Catalog Registration & Iceberg REST Catalog Server

- **Catalog registration backends** (DESIGN.md §13.6.5):
  - `catalog = 'filesystem'` — no-op (already functional from v0.52).
  - `catalog = 'glue'` — AWS Glue Data Catalog API integration.
  - `catalog = 'rest'` — Iceberg REST Catalog spec client for Polaris,
    Unity Catalog, Gravitino, Nessie.
  - `catalog = 'hive'` — Hive Metastore Thrift client.
  - `catalog = 'ducklake'` — DuckLake metadata database sync.
  - Step 6 of snapshot lifecycle: idempotent catalog API call.
  - `CATALOG_WARN` state on failure; retried next flush; IVM never blocked.
  - Credential management via named secrets (`catalog/secrets/`).
- **Native Iceberg REST Catalog server** (DESIGN.md §13.7):
  - Gateway HTTP server on port 8181 serves `/iceberg/v1/`.
  - `GET /v1/config`, `GET /v1/namespaces`, `GET /v1/namespaces/{ns}/tables`,
    `GET /v1/namespaces/{ns}/tables/{tbl}`,
    `GET /v1/namespaces/{ns}/tables/{tbl}/snapshots`.
  - Backed by control-plane `DbReader` + cold snapshot manifests.
  - Auth: same bearer/mTLS as SQL gateway.
  - `rockstream-catalog` module inside `rockstream-gateway`.

**Exit criteria for v0.53**

- Spark configured with `catalog.type=rest, catalog.uri=http://rockstream:8181/iceberg/v1`
  discovers and reads a RockStream view by name.
- DuckDB with `iceberg_rest` secret discovers tables without explicit S3 path.
- Glue catalog shows table within 30s of snapshot commit.
- `CATALOG_WARN` state surfaces cleanly when external catalog is unreachable;
  subsequent flush + successful API call resolves the state.
- Auth: unauthenticated catalog request is rejected.

---

### v0.54 — Cold-Tier Correctness Soak & Cost Accounting

- **Delta Lake full support**: remove `--experimental-delta-sink` flag;
  Delta `_delta_log/` format with add/remove actions; readable by DuckDB
  `delta_scan`.
- **Cold + hot merge correctness soak**: 7-day randomized workload
  (inserts, updates, deletes) comparing `TwoTier` merged read vs.
  accumulated hot-only state. Any divergence is a P0 bug.
- **Cost accounting**: cold-tier storage bytes reported in
  `EXPLAIN INCREMENTAL ESTIMATE`, counted against pipeline
  `state_budget_gb` quota, visible in `rockstream.views` system table.
- **Snapshot interval auto-tuning**: the auto-tuner adjusts
  `snapshot_interval_epochs` based on observed write rate and target
  cold snapshot file size (avoid small files, avoid excessively large
  buffering in shard pending_buffer).
- **Documentation**: cold-tier operator guide, DuckDB/Trino
  integration examples, catalog configuration reference.

**Exit criteria for v0.54 (Phase 12 complete)**

- 7-day soak shows zero merge divergence.
- Delta `_delta_log/` readable by DuckDB `delta_scan`.
- `EXPLAIN INCREMENTAL ESTIMATE` reports cold-tier bytes within 20% of
  actual.
- Cold-snapshot bytes count against `state_budget_gb`; exceeding quota
  pauses the sink with `RS-4010 cold_tier.quota_exceeded`.
- Snapshot interval auto-tuning produces ≥ 128 MB Parquet files under a
  10k rows/s continuous workload.

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
| Per-operator commits overwhelm object storage | Shard-level group commit; commit-cost benchmark in Phase 3.5; adaptive epoch sizing with `min_epoch_ms` / `min_epoch_bytes` floors. |
| SlateDB has no range-delete API | Design cleanup as scan-and-delete, compaction-filter retention, or checkpoint/clone/projection; make range-delete absence an integration test. |
| Compaction filters break snapshot safety | Treat filters as retention only; explicit deletes for correctness; safety proofs and stale-reader tests before enabling filters. |
| MergeOperator used for non-associative state | Restrict merge operators to associative accumulators; implement MIN/MAX/Top-K/window/recursive retractions with explicit arrangements. |
| Frontier protocol implementation bugs | Heavy property testing; reference implementation in pure logic for comparison. |
| Object-store cost dominates | Aggressive local SST cache; coalesce small writes; tier cold state; WAL listing cache. |
| WAL listing becomes a hot-path cost | Per-shard WAL listing cache, tail via `WalReader::get(latest_id+1)`; Phase 3.5 listing-cost test. |
| Manifest churn under bursty load | `min_epoch_ms` / `min_epoch_bytes` floors; manifest-write budget tracked in Phase 3.5. |
| Frontier aggregator becomes a bottleneck | Async aggregation with bounded staleness budget; Phase 5 throughput test at thousands of shards × hundreds of operators. |
| SQL incrementalization gaps | Use Feldera's compiler as semantic reference; use pg_trickle as oracle for edge cases; build a comprehensive SQL test corpus. |
| pg_trickle semantics diverge from native runtime | Side-by-side oracle tests; store planner metadata explicitly; favor DBSP derivations where pg_trickle is PostgreSQL-specific. |
| Distributed IMMEDIATE mode fights scale | Keep IMMEDIATE restricted to simple/single-shard cases; default to deferred low-latency epochs. |
| Feldera-style synchronous ownership scheduling rejects valid topologies | Use async, ownership-free per-worker scheduler; multi-consumer streams are normal; `DbReader` is the multi-reader path. |
| Distributed recursion stalls or diverges | Per-iteration inner frontier, max-iteration cap, inner-frontier stall timeout, per-shard recompute fallback. |
| Operator skew | Adaptive re-sharding in Phase 7; sub-key partitioning for extreme skew. |
| Hardware/network partitions | Chaos testing; documented degraded-mode behavior. |
| Schema evolution | Versioned schema catalog; compatible online changes; incompatible drift becomes `BLOCKED(RS-1002)` until blue/green replacement via `Clone`. |
| Shuffle connection/object explosion | Worker-level stream multiplexing; coalesced durable shuffle objects; Phase 4 budget test at 1,000 shards. |
| Checkpoint barrier alignment buffers grow without bound | Alignment buffers are credit-bounded and propagate backpressure; Phase 6 chaos test injects slow inputs during checkpointing. |
| Merge-backed arrangements read stale values | All merge-backed reads go through `ShardDb::get_merged()` / `scan_merged()`; Phase 3.5 test forces fallback if the storage profile cannot resolve operands on read. |
| **Auto-tuner oscillation** | Hysteresis bands on every adaptive loop (scale up after K consecutive over-budget windows, scale down only after 4× K under-budget windows); upper/lower bounds per pipeline; every decision recorded in the audit log so oscillation is visible. Property test: random workload sequence must reach a stable parallelism within bounded time. |
| **SLO unmet for structural reasons (skew, source slow, downstream sink slow) goes unnoticed** | `pipeline_degraded_reason` is always populated when `pipeline_slo_compliance < 1.0`; ships in Phase 10 alongside the dashboard. Default alerting rule fires on any pipeline with `degraded_reason ≠ HEALTHY` for > 5 min. |
| **Quota enforcement adds hot-path overhead** | Token-bucket admission and state accounting are per-shard, lock-free; benchmark in Phase 3.5 must show < 2% throughput cost. |
| **Error-code registry rots** | CI gate: any new `tracing::error!` / returned `Error` without a registered `RS-XXXX` fails the build. Doc URL existence is checked. |
| **Support bundle leaks secrets** | Default redaction is on and not config-overridable; only an explicit CLI flag (`--include-secrets`) can disable it; integration test asserts no credential pattern leaves the bundle by default. |

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
  DataFusion, with DBSP-native operators validated against pg_trickle edge
  cases (IVM.md §3). Feldera's sql-to-dbsp is a reference for SQL semantics.
2. **Execution model: codegen vs. interpretation** — **resolved**:
   interpretation of a long-lived operator graph (IVM.md §8.3). Code generation
   may be added later as an optimization for hot queries; not required for v1.
3. **Exchange transport**: pure gRPC vs. QUIC vs. raw TCP framing. Start gRPC
   for ergonomics; benchmark and revisit.
4. **State format on SlateDB**: Arrow IPC framing per arrangement value
   (current plan, IVM.md §9.1) vs. Apache Arrow Row format for point-access
   arrangements. Benchmark in Phase 3 / Phase 3.5.
5. **Control plane HA** — **resolved**: Tier 3 uses a 3- or 5-node Raft group
  to elect exactly one control SlateDB writer lease. Followers serve catalog
  reads via `DbReader` and replay the control WAL. Phase 10 hardens the lease
  handoff and split-brain tests.
6. **Arrangement compaction frontier**: Materialize aggressively compacts
   arrangements past the consumer frontier. SlateDB compaction filters may help,
   but only after snapshot-safety proof; active arrangement consolidation may
   still be needed for long-running queries. Resolve in Phase 3.5 soak.
7. **Control DB implementation detail** — **resolved**: control data lives in
  the control SlateDB; Raft owns only leadership, membership, and writer-lease
  fencing. No data-plane state enters the Raft log.
8. **Frontier-aggregator staleness budget**: the aggregator is async with a
   `frontier_agg_interval` tunable (DESIGN.md §8.4). Pick a default value and
   confirm it satisfies window-close, shuffle-GC, and query-freshness SLOs at
   target scale during Phase 5.
9. **Vector-frontier query semantics** — **resolved**: query gateways pin to a
  published cluster vector frontier (DESIGN.md §12.2) and return freshness
  tokens (DESIGN.md §12.4). Clients that need read-your-writes pass
  `wait_for=<token>` with a timeout; Phase 9 validates the API ergonomics.
10. **Distributed recursion shape**: IVM.md §11.1 allows `Exchange` inside the
    recursive scope. Validate in Phase 4 with a sharded transitive-closure
    benchmark that convergence detection via the inner-iteration frontier
    scales without a synchronous global barrier.
11. **Raft membership change safety**: adding or removing a Raft voter is a
    joint-consensus operation and the most dangerous control-plane action
    available. The CLI must gate this behind an explicit confirmation, show
    current quorum health before proceeding, and record the change in the audit
    log. Resolve the exact joint-consensus or single-server protocol in Phase 10
    alongside the HA hardening milestone.

These are explicitly to be revisited and answered with prototypes during
Phases 1–4.

# RockStream Implementation Plan

A phased roadmap from empty repository to a production-grade, infinitely-scalable
IVM system. Each phase delivers a working, testable system with progressively
more capability.

> **Read first**: [DESIGN.md](DESIGN.md). This plan operationalizes that design.

---

## Phase Overview

| Phase | Title | Outcome | Indicative Duration |
|---|---|---|---|
| 0 | Repository & Tooling | Buildable, tested, CI-green skeleton | 1–2 weeks |
| 1 | Single-Shard Core | Single-process IVM engine running simple SQL | 4–6 weeks |
| 2 | SQL Frontend | DataFusion-based SQL → physical plan | 4 weeks |
| 3 | Full Operator Library | Joins, aggregations, windows, recursion | 8–10 weeks |
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
  - `rockstream-types` — shared types (timestamp, frontier, Z-set row).
  - `rockstream-storage` — wrappers around SlateDB, key encoders/decoders.
  - `rockstream-ops` — operator implementations.
  - `rockstream-plan` — physical plan types.
  - `rockstream-sql` — SQL frontend (Phase 2).
  - `rockstream-runtime` — worker process, scheduler, exchange.
  - `rockstream-control` — control-plane service.
  - `rockstream-gateway` — query gateway service.
  - `rockstream-connectors` — connector implementations.
  - `rockstream-cli` — operator CLI.
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

---

## Phase 1 — Single-Shard Core

**Goal**: A single-process engine that can incrementally maintain a hand-coded
operator graph for a fixed, simple query: `SELECT a, SUM(b) FROM t GROUP BY a`.

**Deliverables**

- `rockstream-storage`:
  - Key encoders for the per-shard namespaces in DESIGN.md §5.1.
  - Wrapper types: `ShardDb`, `ViewOutputWriter`, `OpStateAccessor`.
  - Merge operator registry; `SumCountMerge` reference implementation.
  - WriteBatch builder that produces shard-scoped, namespace-prefixed batches.
- `rockstream-types`:
  - `Timestamp { source_epoch, iteration, sub_epoch }`.
  - `Frontier` (antichain of timestamps) with meet/join operations and tests.
  - `Row` (Arrow-backed) and `Zset = Vec<(Row, i64)>`.
- `rockstream-ops` (Phase-1 subset):
  - `Source` (in-memory stub).
  - `Map`, `Filter`, `Project`.
  - `Aggregate` (SUM, COUNT) using the merge operator.
  - `ViewSink` writing to `view_output/`.
- `rockstream-runtime`:
  - Single-threaded scheduler that runs an operator graph end-to-end.
  - Epoch-based loop: pull deltas → run DAG → commit `WriteBatch`.
- Integration test: hand-built graph for `SELECT a, SUM(b) FROM t GROUP BY a`,
  drive 1M inserts + retractions, assert output equals a non-incremental
  reference.

**Exit criteria**

- 1M row throughput on a laptop (single-threaded).
- Crash mid-epoch (via `kill -9` injected mid-`WriteBatch`); on restart, output
  is identical to a clean run.

---

## Phase 2 — SQL Frontend

**Goal**: Compile arbitrary SQL queries to physical plans the runtime can execute.

**Deliverables**

- `rockstream-sql`:
  - DataFusion-based parser, binder, logical optimizer.
  - Custom DataFusion `Extension` nodes for incremental operators
    (`IncAggregate`, `IncJoin`, `IncDistinct`, `IncWindow`).
  - Incrementalization pass: walk a `LogicalPlan` and rewrite each node to its
    `Inc*` form. Borrow heavily from Feldera's `sql-to-dbsp` semantics.
  - Distribution pass: annotate each node with `partition_key`, insert
    `Exchange` nodes where partition keys differ. (Exchanges are no-ops in
    single-shard mode; preparation for Phase 4.)
  - Cost-based operator-parallelism selector (initial: configurable; later:
    learned from stats).
- Plan persistence: encode physical plans as Substrait + RockStream extensions;
  store in control plane.
- SQL coverage milestones (in order of implementation):
  1. SELECT … WHERE … (filter, project, scalar functions).
  2. GROUP BY with SUM/COUNT/AVG.
  3. Inner equi-join.
  4. LEFT/RIGHT/FULL OUTER JOIN, semi/anti.
  5. DISTINCT, UNION, INTERSECT, EXCEPT.
  6. Subqueries (correlated + uncorrelated; decorrelated by optimizer).
  7. CASE, CAST, complex expressions.
  8. Window functions (Phase 3).
  9. WITH RECURSIVE (Phase 3).
  10. JSON, arrays, structs (Phase 10).

**Exit criteria**

- TPC-H Q1, Q3, Q5, Q6 compile and run incrementally against a single shard.
- All compiled plans round-trip through Substrait without loss.

---

## Phase 3 — Full Operator Library

**Goal**: Implement every operator listed in DESIGN.md §6.

**Per-operator work** (each has: storage encoder, processing logic, retraction
handling, snapshot/replay test, fuzz harness)

| Operator | Storage | Notes |
|---|---|---|
| Equi-Join | `0xJL`, `0xJR` arrangements | Requires Exchange (no-op single-shard) |
| MIN/MAX | `0xMM` indexed multiset + cached extremum | Sort by value within group |
| MEDIAN/PERCENTILE | Same as MIN/MAX + rank lookup | Online ranking structure |
| Outer/semi/anti joins | Extend join logic | Null padding, anti-join semantics |
| Window functions | `0xWN` ordered store + `op_index/segtree` | Segment tree for sliding aggregates |
| Time windows | `0xTW` with event-time TTL | Tumbling, hopping, session |
| Top-K | `0xTK` value-desc sort | Boundary-crossing delta logic |
| Distinct/Union | `0xDS` with weight merge | Compaction filter drops zero-weight |
| Recursion (`WITH RECURSIVE`) | Standard state + iteration timestamp | Convergence detection via frontier |
| UDFs (scalar) | None | Pluggable via DataFusion |
| UDAFs (aggregate) | Custom merge operator interface | User provides associative combiner |

**Cross-cutting deliverables**

- Operator authoring guide (`docs/operators.md`) with template.
- Property tests: `incremental(query, input_stream) == batch(query, accumulated)`
  for every operator (the DBSP correctness theorem made executable).
- Microbenchmarks for each operator (`criterion`).

**Exit criteria**

- Full TPC-H runs incrementally on a single shard.
- Recursive transitive-closure example (reachability over a 1M-edge graph)
  converges and produces correct deltas.

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
  - The scheduler on each worker runs only the instances assigned to its shards.
  - Exchange operators serialize Arrow batches keyed by destination shard.

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

1. **Compiler reuse vs. ground-up**: integrate Feldera's `sql-to-dbsp` Java
   compiler via JNI/IPC, or reimplement in Rust on DataFusion? Prefer the
   latter for operational simplicity but accept slower SQL coverage ramp.
2. **Exchange transport**: pure gRPC vs. QUIC vs. raw TCP framing. Start gRPC
   for ergonomics; benchmark and revisit.
3. **State format on SlateDB**: row-encoded (current plan) vs. columnar
   (Arrow-encoded) blobs. Row for hot point-access state, columnar for
   bulk-scan arrangements? Benchmark.
4. **Control plane HA**: lean on SlateDB's single-writer (with hot standby
   readers) and a leader election above it (etcd? Raft over SlateDB?), or use
   external Raft like the standard Kubernetes pattern? Start with a single
   writer + cold standby; harden in Phase 10.
5. **Plan execution model**: long-lived operator threads (Flink-style) vs.
   per-epoch task scheduling (Spark-style)? Long-lived for low latency,
   per-epoch for elasticity. Probably hybrid: long-lived tasks with re-pinnable
   shard ownership.

These are explicitly to be revisited and answered with prototypes during
Phases 1–4.

# RockStream Roadmap

This document turns the design into an implementable path. It complements:

- [DESIGN.md](DESIGN.md): what RockStream is and why the architecture works.
- [IVM.md](IVM.md): how the incremental view maintenance engine works.
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md): detailed phase-by-phase engineering plan.

The roadmap is intentionally patient. There is no rush to 1.0. RockStream is a
distributed database-like system, and the fastest credible path is to build it in
small, evidence-producing versions. Each roadmap version below is sized at about
**10 person-weeks** of implementation effort. That can mean one person for ten
weeks, two people for five weeks, five people for two weeks, or any other mix.

The version number is not a promise of public release quality. It is a planning
unit. A version is done only when its proof is done.

---

## Roadmap Philosophy

1. **Evidence over dates.** A roadmap version ends when tests, benchmarks,
   simulations, and documentation prove the new capability works.
2. **Correctness before scale.** A fast incorrect incremental engine is not an
   asset. The single-shard IVM core must be boringly correct before distribution
   is allowed to make the problem harder.
3. **Simulation from the beginning.** The deterministic `SimRuntime` and
   `buggify!()` discipline are not hardening work. They are foundation work.
4. **Operability is never deferred.** Every capability arrives with diagnostics,
   error codes, audit events, and at least one clear operator-facing signal.
5. **Prefer thin vertical slices.** A version should leave a human able to do
   something real, or leave the project with stronger proof that a hard thing is
   safe.
6. **No accidental Postgres clone.** Postgres wire compatibility is an access
   layer, not the product goal. RockStream is for live SQL views and streaming
   analytics, not high-concurrency OLTP.
7. **Split before rushing.** If a version cannot fit into roughly 10
   person-weeks, split it. The roadmap is allowed to grow.

---

## Common Definition of Done

Every roadmap version must satisfy this baseline before it can be considered
complete:

- `cargo fmt --check`, `cargo clippy -D warnings`, and `cargo test --workspace`
  pass.
- New behavior has unit tests and either property tests, simulation tests, or an
  integration test, depending on risk.
- Any user-visible or operator-visible failure has an `RS-XXXX` error code.
- Any control-plane action writes an audit event.
- Any new performance claim has a benchmark or measurement note.
- Any new public surface is documented in the relevant doc or CLI help.
- Any new distributed coordination path has at least one seeded `SimRuntime`
  test before it is considered done.
- Main remains runnable through the single `rockstream` binary.

Long soaks are gates, not loopholes. If a version needs a 24-hour or 30-day run,
the engineering work still fits the version budget, but the version is not
accepted until the soak result is clean.

---

## Public Milestones

These names are for orientation. They are not calendar commitments.

| Milestone | Version | Meaning |
|---|---:|---|
| Developer Alpha | v0.10 | Local single-shard engine can maintain simple views and survive crash/replay. |
| SQL Alpha | v0.18 | Core SQL views, joins, set ops, and `EXPLAIN` work on one shard. |
| Single-Shard Beta | v0.27 | Advanced IVM is feature-complete enough for serious single-node testing. |
| Distributed Alpha | v0.36 | Multi-shard execution, frontier protocol, recovery, and exactly-once basics work. |
| Integration Beta | v0.45 | Postgres access, direct writes, and major external connectors work end to end. |
| Production Beta | v0.51 | Observability, auth, upgrades, security review, and long soaks are ready for a pilot. |
| Data Lake GA | v0.54 | Cold-tier Iceberg/Delta sinks, Iceberg REST catalog, external tool consumption proven. |
| 1.0 | after v0.54 | Tagged only after a real production handoff succeeds without design exceptions. |

---

## Version Roadmap

Each row is about 10 person-weeks. The "proof" column is the important part:
without that proof, the version is not done.

### Foundation

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.1 | Repository workbench | Cargo workspace, core crates, CI, dev container, `rockstream` binary stub, basic `tracing`, pinned MSRV, dependency policy. | Clean CI on an empty no-op binary; `rockstream --help` works; repository has no hidden local setup step. |
| v0.2 | Runtime abstraction and simulation seed | `rockstream-sim`, `Runtime` trait, `TokioRuntime`, `SimRuntime`, in-memory object store and network, seeded clock, first `buggify!()` macro, explicit fault-model registry, paired-assertion helper pattern. | A deterministic test replays the same seed byte-for-byte; changing the seed changes event order; production build compiles with `buggify!()` as no-op; every `buggify!()` site names a fault-model entry. |
| v0.3 | SlateDB storage contract | `rockstream-storage`, key encoders (including `namespace_id` in all catalog keys from day one), `ShardDb`, `WriteBatch` builders, `DbReader`, WAL reader smoke tests, merge operator registry, no range-delete dependency. | Storage API validation suite proves only supported SlateDB features are used; unsupported operations fail at compile/test time; catalog key encoders include namespace dimension. **SlateDB determinism test**: two `SimRuntime` runs at the same seed, driving SlateDB against the in-memory `ObjectStore` facade, must produce bit-identical key–value state and WAL sequences; any internal SlateDB async behavior that bypasses the seeded scheduler causes the test to fail. This is the gate that validates the FoundationDB simulation property holds *through* SlateDB, not merely around it. |
| v0.4 | No-op pipeline and local CLI | `rockstream start --storage=./data`, no-op source, no-op operator, no-op view sink, support-bundle skeleton, audit-log skeleton, error-code registry. | `make e2e` starts a local process, runs a no-op pipeline, emits audit events, writes a support bundle, and shuts down cleanly. |

### Single-Shard IVM Kernel

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.5 | Z-set and PlanIR kernel | Shared Z-set types, `PlanNode`, `OpNode`, `Operator`, `EpochOutput`, fixed in-memory source, shard-local scheduler loop. | Property test verifies simple Z-set insert/delete/update algebra; no storage yet beyond in-memory execution. |
| v0.6 | Filter, project, map | `DiffCtx`, filter/project/map differentiation, Arrow batch handling, `_weight` convention, DataFusion expression evaluation. | Random insert/delete sequences match DataFusion batch for filter/project/map queries. |
| v0.7 | Algebraic aggregates | `SUM`, `COUNT`, `AVG`, `COUNT(*)`, aggregate arrangement, associative merge operator, last-emitted cache. | >=100k randomized aggregate scenarios match the batch oracle; benchmark captures baseline throughput. |
| v0.8 | Non-invertible aggregates | `MIN`, `MAX`, indexed multiset state, cached extremum, delete path via prefix scan, merge-backed read correctness. | Random group churn tests match batch; stale merge operands cannot hide from `get_merged()` / `scan_merged()`. |
| v0.9 | Epoch commit and replay | Shard-level group commit, persisted frontier, crash mid-epoch, idempotent replay, WAL listing cache in the hot path, cooperative scheduling with yield points (`max_rows_per_quantum`, DESIGN.md §9.3). | Kill-injected mid-commit run restarts to bit-identical output; WAL hot path issues no object-store `list()`; a single expensive operator epoch cannot starve heartbeat sends (verified by scheduler yield ratio metric). |
| v0.10 | Developer Alpha loop | One-binary local workflow, embedded runtime profile, first `rockstream explain`, support bundle includes plan/log/shard stats, docs for local view development. | A developer can start RockStream, feed records, maintain a simple aggregate view, crash it, restart it, and inspect what happened; embedded fast-path benchmark reports p50/p95 freshness with zero gRPC shuffle calls. |

### Core SQL

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.11 | DataFusion SQL lowering | Parser, binder, logical optimizer integration, `LogicalPlan` to `PlanNode`, basic `CREATE VIEW`, scalar expressions, casts, CASE. | SQL and hard-coded PlanIR produce identical physical plans for the Phase 1 operators. |
| v0.12 | Catalog and plan persistence | Source/view schema catalog, Substrait + RockStream extension encoding, schema-version storage, compatible-change rules. | Plans round-trip through storage; compatible schema change succeeds; incompatible drift returns `RS-1002`. |
| v0.13 | Inner joins | Dual arrangements, stable row identity, pre-change snapshot semantics, join metadata, TPC-H Q1/Q3/Q5/Q6 subset. | Random three-way join property tests and selected TPC-H queries match DataFusion batch. |
| v0.14 | Outer, semi, anti joins | Left/right/full outer joins, semi/anti joins, unmatched-row state, pg_trickle Q21 and FULL JOIN aggregate edge cases. | Q11/Q21 and randomized NULL-heavy tests match batch and pg_trickle-derived expectations. |
| v0.15 | Distinct and set operations | Distinct, union, intersect, except, bag/set semantics, zero-crossing state, snapshot-safe cleanup rules. | Random set-operation sequences match batch; compaction filter is disabled until safety proof exists. |
| v0.16 | Pipeline DDL | `CREATE PIPELINE`, multiple sources/views in one pipeline, dependency metadata, basic pipeline lifecycle commands. | A multi-view pipeline can be created, started, paused, and deleted with audit events for every transition. |
| v0.17 | Explain and estimates | `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, source stats hooks, estimated state size, request rate, confidence labels. | Estimates are produced before deployment; observed vs. estimated error is tracked for representative workloads. |
| v0.18 | SQL Alpha soak | Core SQL correctness pass across filter/project/map, aggregates, joins, set ops, DDL, explain, catalog. | One-hour fuzzer finds no divergence; SQL Alpha demo runs from `psql`-like CLI or `rockstream sql`. |

### Advanced IVM

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.19 | Window functions | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, sliding SUM/AVG, partition recomputation. | Window-heavy randomized tests match batch; partition recomputation cost is measured and documented. |
| v0.20 | Time windows and late data | TUMBLE, HOP, SESSION, event-time TTL, late-data policies, frontier-aware retention hooks; event-time frontier driven by connector watermark channel from §13.3. | Late data test matrix proves `drop`, `update`, and `route_to_sink` behavior; TTL never removes visible state; a synthetic source that emits watermarks closes tumbling windows exactly once even under out-of-order input. |
| v0.21 | Top-K | Top-K detection, `K + epsilon` state, delete refill path, delta swaps, partitioned Top-K. | Random insert/update/delete Top-K tests match batch; delete from current top K refills correctly. |
| v0.22 | Recursion | Recursive PlanNodes, nested timestamp scheduler, semi-naive and DRed strategies, convergence detection, safety caps. | Transitive closure and hierarchy examples converge; cyclic graph tests produce correct deltas. |
| v0.23 | Bootstrap and snapshot mode | Snapshot sources, streamed bootstrap epochs, bootstrap frontier, reconciliation after connector position loss. | 100M-row equivalent synthetic snapshot matches batch; restart during bootstrap does not duplicate or skip rows. |
| v0.24 | View-on-view DAG | `ViewRef`, upstream view CDC, cadence inheritance, diamond-consistency groups, cycle detection. | Five-level DAG and diamond topology converge under continuous input; cycles are rejected at compile time. |
| v0.25 | Lateral, SRF, and UDF hooks | Lateral functions/subqueries, row-scoped recomputation, scalar UDF hooks, UDAF interface sketch. | JSON/unnest/generate_series style examples match batch; UDAF requirements are documented before implementation. |
| v0.26 | IVM correctness freeze | TPC-H 22/22 single-shard, Nexmark subset, random query fuzzer, pg_trickle side-by-side where supported. | 22/22 TPC-H queries match batch; fuzzer runs at least one hour without divergence. |
| v0.27 | Single-shard performance profile | Criterion suite, object-store request budget, manifest churn budget, merge-read fallback, state-budget enforcement. | >=10x speedup vs. batch at 1% change rate for representative queries, or documented gaps with follow-up issues. |

### Distributed Core

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.28 | Control plane and worker discovery | Control service, worker registration (with `capacity_headroom` reporting), topology catalog, bootstrap command, mTLS scaffolding, role flags. | Tier 1 and Tier 2 start flows work; workers join through `--control=<url>`; topology changes are audited; placement algorithm respects reported capacity. |
| v0.29 | Shard leasing and scheduling | Shard manager, per-shard SlateDB handles, lease acquisition, writer fencing, distributed operator placement. | Two-writer fence test proves only one writer can commit; killing a worker causes clean lease release/reassignment. |
| v0.30 | Direct exchange | gRPC shuffle service, exchange path classifier (`elided` / `loopback` / `direct` / `durable`), worker-level multiplexing, same-worker loopback, pre-shuffle combiners, Arrow serialization, credit backpressure. | 16-shard single-host cluster runs partitioned TPC-H subset with bounded connection count; loopback avoids network calls for co-located exchanges; combiner benchmark documents bytes avoided for aggregate shuffles. |
| v0.31 | Durable shuffle fallback | Object-store fallback path, coalesced shuffle objects, outbox/inbox metadata, receiver notifications, no LIST hot path. | Inject receiver failure and large batch; sender falls back durably and receiver catches up without duplicates. |
| v0.32 | Frontier protocol and aggregator | Antichain type, per-shard frontier reporter, worker-level frontier summaries, separable `--role=frontier`, cluster frontier publication, shuffle GC. | Multi-input join with uneven sources produces no premature output; aggregator stress test covers thousands of shards × hundreds of operators without direct per-shard subscriptions. |
| v0.33 | Distributed recursion and skew stress | Exchange inside recursive scopes, inner frontier convergence, skewed inputs, per-shard recompute fallback. | Sharded 10M-edge reachability benchmark converges; stalled inner frontier surfaces a named error. |
| v0.34 | Cluster checkpoints | Barrier injection, bounded alignment buffers, per-shard checkpoint creation, atomic cluster checkpoint commit, old checkpoint GC. | Checkpoint under slow input and credit exhaustion never grows unbounded and either succeeds or reports `RECOVERING`. |
| v0.35 | Recovery driver and SLO metrics | Recovery from cluster checkpoint, shard reassignment, failure detection, recovery histograms, `RECOVERING_SLOW`, worker self-fencing on control-plane partition (DESIGN.md §11.6), staggered restart / lease-grant rate limit to prevent thundering herd (DESIGN.md §11.8). | At target shard size: failure detection <=5s, shard reassignment <=30s, pipeline freshness recovery <=60s in chaos runs; 32-worker simultaneous restart triggers no false failure detections. |
| v0.36 | Exactly-once and chaos alpha | 2PC sink protocol, Kafka/S3/Postgres sink stubs, simulation suite for epoch/frontier/checkpoint/2PC interleavings, liveness checks tied to recovery SLOs, object store brownout handling (DESIGN.md §11.7), wire protocol version skew contract (DESIGN.md §5.5). | >=100k simulation seeds pass; every recoverable injected fault either commits a new epoch within the 5s/30s/60s budgets or surfaces a named degraded state; a 24-hour 32-shard chaos run has zero data loss and zero duplicates; 60-second object-store blackout test recovers cleanly. |

### Elasticity, Gateway, and Connectors

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.37 | Online split and merge | Online shard split, cold shard merge, checkpoint-copy-replay, shard-map version bump, cleanup after cutover. | Sustained workload continues through split/merge; output remains equal to uninterrupted reference. |
| v0.38 | Proactive scaling and rebalancing | `target_shard_state_bytes`, proactive splitter, worker scale-out, worker drain protocol (DRAINING → DECOMMISSIONED), `cluster_worker_pressure` metric for infrastructure autoscaling, skew detection, adaptive re-sharding, hot-key virtual buckets. | Drive one shard to 30GB; split starts before alert threshold and no freshness SLO is missed; a skewed-key benchmark stays within the documented worst-shard/median load factor; drain a 4-shard worker within 120s with no epoch loss; `cluster_worker_pressure` metric is exposed and HPA-consumable. |
| v0.39 | Clone and schema evolution | Pipeline clone, blue/green plan replacement, atomic flip, compatible/incompatible schema workflows. | Breaking schema change goes through clone/backfill/flip without source offset loss. |
| v0.40 | Postgres read gateway | pgwire startup/query/extended-query, row descriptions with Postgres OIDs, catalog stubs (`pg_catalog`, `information_schema`), snapshot reads, connection pooling, query timeouts, rate limiting. | `psql` and SQLAlchemy can read views; view schema reflects without ORM errors; `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`; gateway reads complete in < 10 ms p99 for a local cluster. |
| v0.41 | Gateway introspection and read performance | Cross-shard partial aggregation pushdown (DESIGN.md §12.3.1); `rockstream` system schema virtual tables (`rockstream.epochs`, `rockstream.pipelines`, `rockstream.shards`, `rockstream.audit_log`, etc., DESIGN.md §12.6.1); per-worker arrangement segment cache keyed by `(shard_id, segment_id)` (DESIGN.md §5.4). | `SELECT COUNT(*), region FROM mv GROUP BY region` pushes partial agg to shards; gateway receives O(groups) rows, not O(view rows); `SELECT * FROM rockstream.epochs` returns committed epoch history; segment cache hit ratio > 80% for hot-join workloads in benchmarks. |
| v0.42 | Freshness, subscribe, isolation, historical queries | `READ COMMITTED`, `REPEATABLE READ`, freshness tokens, `wait_for=<token>`, subscribe API, gateway restart behavior, `AS OF EPOCH <n>` / `AS OF TIMESTAMP <t>` historical queries (DESIGN.md §12.4.1), `checkpoint_retention_count` / `checkpoint_retention_duration` configuration. | Read-your-writes demo passes; subscribe stream survives gateway restart without gaps or duplicates; `SELECT * FROM orders_mv AS OF EPOCH <past>` returns the correct historical snapshot; queries beyond retention return `RS-2005`. |
| v0.43 | Internal direct-write connector | DML over pgwire, transaction buffer, COMMIT to base-table shard, ROLLBACK discard, generated source epochs. | `psql` can insert/update/delete rows and see a maintained view refresh within `freshness_target_ms`. |
| v0.44 | External source/sink set (Tier 1 contract) | Kafka source/sink, Postgres CDC source, S3/table-format source and sink, HTTP push/webhook; every source implements the §13.3 Tier 1 contract (opaque `OffsetToken`, `watermark: Option<EventTimeWatermark>`, `credits_available()`) and routes per-record decode errors to a DLQ sink as `RS-1003`; every sink implements `prepare`/`commit`/`abort` with a default `should_flush` that flushes every epoch. | Postgres CDC → RockStream IVM → Kafka sustains 100k rows/s for 24 hours exactly once; Kafka source closes a 1-minute tumbling window correctly under deliberate clock skew; under sustained downstream saturation, Kafka consumption rate tracks downstream credits with bounded inbox memory. |
| v0.45 | Connector lifecycle, SDK, and Tier 2 contract | Connector pause/resume/delete, external gRPC connector protocol, SDK, examples, isolation options; Tier 2 contract additions: `partition_filter: Option<PartitionFilter>` on source `start_snapshot`/`poll_delta` (opt-in; connectors that do not support it return `None` and fall back to operator-layer filtering), and `should_flush(bytes_buffered, epochs_buffered)` override for file-format sinks (Iceberg/Delta/Parquet) that need to buffer across epochs to avoid small files. | Third-party example connector passes Tier 1 contract tests; Iceberg sink implementing Tier 2 `should_flush` with a 10ms epoch produces ≤ 2 files/minute (≥ 256 MB each); a Tier 1 connector (e.g. Kafka) passes contract tests with the default flush-every-epoch `should_flush`; `partition_filter_support() -> bool` returns false on connectors that do not implement pushdown and operator-layer filtering is verified to produce identical output. |

### Production Beta

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.46 | Auth, RBAC, and transport security | OIDC/bearer auth, service accounts, per-view RBAC, mTLS everywhere, certificate rotation docs. | Auth integration tests reject unauthenticated and cross-tenant access; audit log has actor on every event. |
| v0.47 | Observability and admin surface | Prometheus metrics, OTEL traces, JSON logs, admin CLI, support bundle completeness, dashboard template, `rockstream debug arrangement` IVM debugger (DESIGN.md §14.7.1), tombstone density metric and proactive compaction trigger (DESIGN.md §5.4). | Operator can diagnose a slow pipeline from SLO compliance -> degraded reason -> explain -> support bundle; can inspect a specific arrangement key without stopping the pipeline. |
| v0.48 | Auto-tuner hardening | Adaptive parallelism, epoch sizing, source throttle, hysteresis, stability tests, override docs. | Random workload property tests converge without oscillation; every tuning action is audit logged. |
| v0.49 | Upgrades, migration, and security review | Storage format gate, rolling upgrade test, migration skeleton, disaster recovery drill, independent security review. | N -> N+1 rolling upgrade loses no epoch; incompatible format fails safely with `RS-5001`; security review issues are triaged. |
| v0.50 | Long production soak | 30-day 64-shard soak, 1,000-shard control/exchange stress, Nexmark/TPC-H continuous, chaos automation, continuous simulation soak on `main`, release-blocking defect burn-down. | 99.99% availability target met or miss is understood and fixed; no correctness divergence; large-cluster stress stays within exchange and frontier budgets; all historical failing simulator seeds replay in CI. |
| v0.51 | Production beta handoff | Helm/Terraform packaging, deployment playbooks, SQL reference, connector guide, operator guide, reference architecture. | First pilot workload runs with support agreement, documented runbook, rollback plan, and known limitations. |

### Cold Tier & Data Lake Integration

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.52 | Cold-tier Parquet/Iceberg sink | Iceberg v2 cold-tier sink writer (DESIGN.md §13.6): `CREATE SINK ... TO ICEBERG` with `snapshot_interval_epochs`/`snapshot_interval_ms`, Parquet data files with column stats, manifest files and manifest lists, atomic `metadata.json` commit, `should_flush`-gated buffering with pending rows staged in shard SlateDB, exactly-once via idempotent file keys. `ViewReader` `TwoTier` variant functional: gateway can merge cold snapshot + hot LSM tail. Cold snapshot GC (§13.6.6). | Cold-tier sink writes valid Iceberg v2 table readable by DuckDB `iceberg_scan`; full-scan query over a 100M-row view uses cold tier and completes 10x faster than hot-only LSM scan; snapshot GC keeps ≤ `cold_snapshot_retention_count` snapshots per view; crash mid-flush produces no orphan data files. |
| v0.53 | Catalog registration and Iceberg REST catalog server | Catalog registration backends (§13.6.5): `filesystem` (self-contained, already functional), `glue`, `rest`, `hive`, `ducklake`. Native Iceberg REST catalog server (§13.7) on gateway HTTP port 8181: `/iceberg/v1/` serves namespaces, tables, snapshots backed by control-plane metadata. Auth token/mTLS passed through. | Spark/Trino/DuckDB discover views by name via `catalog.uri=http://rockstream:8181/iceberg/v1`; Glue catalog shows table within 30s of snapshot commit; `CATALOG_WARN` state surfaces cleanly when external catalog is unreachable; catalog API failures never block IVM. |
| v0.54 | Cold-tier soak and Delta Lake support | Delta Lake cold-tier sink variant (`CREATE SINK ... TO DELTA`), cold-tier + hot tail merge correctness soak (randomized inserts/updates/deletes, compare cold+hot read vs. hot-only accumulated state), snapshot interval tuning, cost-accounting (cold-tier storage bytes in `EXPLAIN INCREMENTAL ESTIMATE` and quota system). | 7-day cold-tier soak with continuous writes shows no merge divergence; Delta `_delta_log/` is readable by DuckDB `delta_scan`; `EXPLAIN INCREMENTAL ESTIMATE` reports cold-tier storage cost within 20% of actual; cold-snapshot bytes count against pipeline `state_budget_gb`. |

---

## 1.0 Gate

RockStream should not tag 1.0 simply because v0.54 is complete. The 1.0 gate is
evidence-based:

- At least one real production or production-like workload has run long enough
  to exercise failure, recovery, upgrade, and backfill paths.
- No P0/P1 correctness bugs are open.
- Every supported SQL feature has an oracle test or batch-equivalence test.
- The deterministic simulator runs millions of seeds before release, and any
  historical failing seed is replayed in CI.
- Continuous simulation has run against `main` long enough to cover both safety
  and liveness failures across the explicit fault model.
- Recovery-time invariants hold at the target shard size.
- The upgrade path from the previous beta is tested and documented.
- The public docs state what RockStream is not: not an OLTP Postgres clone, not
  cross-shard `SERIALIZABLE`, not active-active multi-region writes.
- A new operator can debug the system using the docs, dashboard, CLI, audit log,
  and support bundle without reading source code first.

If any of these are not true, keep shipping v0.x releases. That is not failure;
that is the discipline this project needs.

---

## Decision Gates

These are explicit places to pause, learn, and possibly reshape the roadmap.

| Gate | After | Question |
|---|---:|---|
| Architecture sanity | v0.4 | Do SlateDB, the runtime abstraction, and local developer ergonomics still fit the design? |
| IVM kernel confidence | v0.10 | Is the core delta engine simple enough to debug, and does replay work cleanly? |
| SQL scope control | v0.18 | Are we still building the right SQL subset first, or have edge cases started to dominate? |
| Single-shard correctness | v0.27 | Is the IVM engine correct and fast enough to justify distribution work? |
| Distributed architecture | v0.36 | Does the shard/exchange/frontier/checkpoint model actually hold under simulation and chaos? |
| Product wedge | v0.45 | Is the `psql` + live views + connectors experience compelling enough for pilot users? |
| Production readiness | v0.51 | Is the system operable by someone who did not build it? |
| Data lake integration | v0.54 | Does the cold-tier + catalog story deliver real value, or is feeding external tools sufficient without the cold tier? |

At each gate, the default action is not to accelerate. The default action is to
remove uncertainty.

**Design freeze after v0.10.** Once the IVM Kernel Confidence gate is passed,
new sections may not be added to DESIGN.md or IVM.md unless they are required
to unblock a specific coded milestone. The documents become implementation
references, not the primary work product. Gaps discovered during coding are
tracked as GitHub issues and resolved with targeted corrections to the relevant
section — not as numbered design-revision passes. The test: after v0.10, every
DESIGN.md commit should be small and targeted ("correct §7.5: exchange
loopback threshold"), not broad ("v3.X adds Y, Z").

---

## Parallel Work Tracks

The version order is the critical path, but work can proceed in parallel once the
interfaces are stable.

| Track | Can start seriously after | Notes |
|---|---:|---|
| Storage and simulation | v0.1 | This is foundational and should lead the rest of the project. |
| SQL compiler | v0.5 | Can prototype against in-memory operators before storage is mature. |
| Operator implementations | v0.6 | Each operator must come with oracle/property tests. |
| Control plane | v0.12 | Catalog and plan persistence create the control-plane substrate. |
| Distributed runtime | v0.27 | Do not start serious distribution before the single-shard engine is frozen. |
| Gateway and pgwire | v0.18 | Can build against single-shard snapshots before distributed reads exist. |
| Connectors | v0.23 | Snapshot/bootstrap semantics should be stable first. |
| Observability/SRE | v0.4 | Should be threaded through all versions, not saved for the end. |
| Docs and examples | v0.4 | Every version should improve an example or runbook. |

---

## Things To Keep Out Until After 1.0

These may be good ideas later, but they dilute the first implementation:

- Active-active multi-region writes.
- Cross-shard `SERIALIZABLE` isolation.
- Full OLTP compatibility with Postgres.
- A large web console before the CLI and metrics are excellent.
- Per-query billing or chargeback.
- Arbitrary user-defined distributed transactions.
- Performance-only rewrites before correctness evidence is strong.
- New storage backends before SlateDB is fully proven.

---

## First Six Versions In More Detail

The first six versions decide whether the whole project has the right bones.
They deserve extra care.

### v0.1: Repository Workbench

The goal is not to write a lot of product code. The goal is to make every later
change cheap and safe. CI, formatting, dependency checks, tracing, and the crate
layout should be in place before the system becomes interesting.

The best possible outcome is boring: a new contributor can clone the repo, run
one command, and see green tests.

### v0.2: Runtime Abstraction and Simulation Seed

This is the FoundationDB lesson applied immediately. If `Runtime` is not in the
first real code paths, it will be painful to retrofit. The simulator can start
small: deterministic clock, deterministic task scheduling, in-memory object
store, in-memory network, seeded failure injection.

The important proof is reproducibility. A failing seed must fail the same way
every time.

### v0.3: SlateDB Storage Contract

This version draws a hard line around what RockStream may assume about SlateDB.
It should make unsupported assumptions impossible or loud. No hidden range
delete. No casual reliance on global sequence numbers. No hot-path listing.

The adapter should feel small, explicit, and test-heavy.

### v0.4: No-op Pipeline and Local CLI

This is the first slice of the operator experience. Even with a no-op pipeline,
the single-binary promise, audit log, error registry, and support bundle become
real. That matters because every later feature will naturally plug into these
surfaces instead of inventing its own diagnostics.

### v0.5: Z-set and PlanIR Kernel

This is where RockStream starts to become an IVM system. Keep it in-memory and
small. Prove the algebra before involving SQL, distribution, or external I/O.

### v0.6: Filter, Project, Map

The first useful operators should be intensely tested, not grand. A simple view
that stays correct through arbitrary inserts and deletes is the seed crystal for
the rest of the system.

---

## Roadmap Maintenance

This document should be updated whenever the project learns something material:

- A version was larger than 10 person-weeks and had to split.
- A risk moved from theoretical to proven.
- A proof became insufficient and needs a stronger gate.
- A public milestone changed meaning.
- A non-goal became tempting enough that it needs to be restated.

The roadmap should stay honest. Its job is not to make the project look fast.
Its job is to make the project buildable.
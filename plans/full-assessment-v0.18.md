# Post-Phase Assessment: v0.18 SQL Alpha

**Date**: 2026-05-30
**Assessor scope**: DESIGN.md (v3.28), IMPLEMENTATION_PLAN.md, ROADMAP.md, IVM.md
**Assessment trigger**: v0.18 SQL Alpha soak complete; Developer Preview decision gate

---

## 1. Executive Summary & Core Weaknesses

The v0.18 milestone marks the completion of Phase 2 (Core SQL). The system can
now lower SQL through DataFusion, compile filter/project/map/aggregate/join/set-op
plans, annotate them with merge-law metadata, and prove correctness via a
deterministic fuzzer. This is a credible single-shard SQL IVM engine on paper.
However, five critical architectural vulnerabilities remain:

1. **The gap between specification density and implementation mass is widening,
   not closing.** DESIGN.md is now 5,173 lines (grew ~70% since the v0.10
   assessment). IMPLEMENTATION_PLAN.md is 1,962 lines. The actual codebase is
   ~26,000 lines of Rust across all crates. The specification-to-implementation
   ratio is nearly 1:3.6 by line count — dangerously close to parity. This
   means the design documents are approaching the complexity of a second
   codebase that must be maintained in parallel, with drift as the inevitable
   result. Each new design revision (v3.20–v3.28) adds detail for features 30+
   versions away while the implementation team is still proving Phase 2 basics.
   The v0.10 design-freeze directive has not been enforced in practice: the
   document continues to accumulate speculative sections.

2. **No real object-store latency has been endured.** Eighteen versions into the
   project, every test still runs against an in-memory object store or local
   filesystem. The "SQL Alpha soak" is a fuzzer over plan lowering and explain
   annotations — it does not exercise SlateDB under real S3/GCS latency
   profiles. The Phase 0 "storage operational budget" decision gate
   (post-v0.10) is listed but there is no evidence it was exercised with a
   cloud object store. The design commits to `target_shard_state_bytes = 20 GB`
   and `write_amplification_ratio = 10` but these remain theoretical. A system
   that has never seen a 200ms PUT tail latency cannot claim to be cloud-native.

3. **The v0.19–v0.27 Phase 3 scope is enormous and under-sequenced.** Phase 3
   packs window functions, time windows with watermarks, Top-K, recursion,
   bootstrap/snapshot, view-on-view DAGs, lateral/SRF/UDF, and a full
   correctness soak into 9 versions. Each of these is a hard distributed-systems
   or query-semantics problem in isolation. Recursion alone (v0.22) requires
   nested timestamps, convergence detection, strategy selection (semi-naive vs.
   DRed vs. recompute), and safety caps. The plan treats them as equal-effort 10
   person-week units, but recursion and time-window watermark semantics are
   demonstrably 2-3x harder than filter/project/map. No fallback sequencing
   exists if any single milestone proves intractable.

4. **The workload/DDL/lifecycle surface area introduced in v0.16–v0.17 is
   untested under operator load.** `CREATE WORKLOAD`, `FRESHNESS_SLO`,
   `MEMORY_LIMIT`, `PAUSE/RESUME MATERIALIZED VIEW`, `SHOW VIEW STATUS`,
   `EXPLAIN INCREMENTAL ESTIMATE`, and the backfill cost-preview prompt all
   shipped as SQL grammar and catalog entries. But without a distributed runtime,
   adaptive epoch loop, or actual state-budget enforcement (deferred to Phases
   3-5), these are type-checked promises with no runtime backing. A user who
   sets `FRESHNESS_SLO = '100ms'` today gets no enforcement, no degradation
   signal, and no feedback that the SLO is purely decorative. This creates an
   expectation gap at the exact moment the project becomes "demo-able to
   external users."

5. **Cross-document drift between IMPLEMENTATION_PLAN and ROADMAP is
   accumulating.** The ROADMAP declares v0.18 as "SQL Alpha" and "Developer
   Preview" simultaneously (both listed in the Public Milestones table). The
   IMPLEMENTATION_PLAN's Phase 2 operability deliverables specify full
   `EXPLAIN INCREMENTAL VERBOSE/ANALYZE` and source-statistics pipelines, but
   the v0.18 sign-off proves only basic explain output and a plan-level fuzzer.
   The `ANALYZE` level explicitly requires "a live worker round-trip" that cannot
   exist in the current single-shard-no-distributed-runtime architecture.
   VERBOSE with "shard counts and parallelism" is meaningless when there is one
   shard and no parallelism selection.

---

## 2. In-Depth Architectural Critique

### `DESIGN.md` Analysis

**Strengths:**
- The latency-class taxonomy (§3.0) is the strongest element in the entire
  design. The dual-frontier model (`visible_frontier` vs. `durable_frontier`)
  correctly separates the laptop experience from the distributed promise. This
  is architecturally rare and correct.
- Virtual-bucket partitioning (§7.1) with rendezvous hashing is the right
  abstraction for online resharding. The migration state machine
  (§10.2, v3.28) is concrete enough to implement without ambiguity.
- The watermark fail-closed policy (§6.9) is a courageous design choice that
  prevents the most common silent-data-loss bug in streaming systems.

**Weaknesses:**

1. **§3.1 Runtime Profiles are under-specified at the transition boundaries.**
   The design says "a pipeline can move from `embedded` to `single_worker` to
   `distributed` by changing placement and shard maps" but does not specify:
   - How the transition is triggered (operator action? auto-detected?).
   - Whether existing arrangement state requires migration.
   - What happens to in-flight epochs during the transition.
   - Whether `visible_frontier` semantics change mid-flight.
   This will bite at v0.28 when the first distributed deployment needs to be
   tested against a pipeline that was created in embedded mode.

2. **§5.4 Arrangement segment cache creates an invisible consistency window.**
   The cache is invalidated "on compaction via manifest-poll." But
   `manifest_poll_interval` is configurable, meaning a stale cache entry can
   serve reads for up to one full poll interval after a compaction that rewrites
   the segment. During this window, a `DbReader`-based join lookup could read
   stale pre-compaction data. The design notes this is safe because "SST
   segments are immutable between the checkpoint at which they were created and
   the compaction that rewrites them" — but a compaction that runs between two
   manifest polls creates exactly this window. The fix is trivial (version the
   segment reference in the read plan), but it is not specified.

3. **§6.12 Arrangement Working Set model is a memory budgeting afterthought.**
   The formula `operator_cache_mb = MEMORY_LIMIT / operator_count` is a uniform
   distribution that ignores operator heat. A 50-way join plan with one hot
   dimension table and 49 cold fact partitions gets 1/50th of cache for the
   dimension — exactly wrong. The auto-tuner "redistributes proportional to
   observed access frequency" but this is specified in one sentence with no
   algorithm, no convergence proof, and no oscillation bound. This will be a
   production performance cliff.

4. **§13.5 Direct-write source connector has an implicit backpressure gap.**
   The design says `COMMIT` flushes as an atomic Z-set delta via `WriteBatch`
   to a base-table shard. But there is no described mechanism for the gateway to
   reject or delay writes when the receiving shard is under checkpoint pressure,
   migration, or memory exhaustion. The Kafka source has `credits_available()`
   for this; the direct-write path has no equivalent. Under write storms, this
   becomes unbounded queue growth inside the gateway.

5. **§12.7 Two-tier view storage design decision notes "cold tier is NOT a
   Phase 9 deliverable" but the `ViewReader` trait ships in Phase 8.** This is
   correct forward-engineering, but the `TwoTier` variant that returns
   `RS-4101 cold_tier.not_enabled` creates a user-visible error code for a
   feature that won't exist for 8+ versions. Any tool that enumerates error
   codes or auto-completes strategies will surface this dead path, creating
   confusion. A simpler approach: define `ViewReadStrategy` as
   `#[non_exhaustive]` with only `HotOnly` until the cold tier actually ships.

### `IMPLEMENTATION_PLAN.md` Analysis

**Strengths:**
- The MergeLaw contract landing as IVM-0 (v0.5) alongside IVM-1 is
  architecturally sound. The shared property-test harness catching
  associativity/commutativity/idempotence violations early prevents
  the category of bugs that Materialize spent years debugging in its
  differential-dataflow arrangements.
- The per-phase operability callouts are genuinely useful. The "error-code
  registry from day one" constraint has measurably improved code quality
  (every error in the codebase has an RS-XXXX code).
- The Phase 3.5 correctness soak before distribution is the single most
  important sequencing decision in the plan. It correctly prevents
  distribution from compounding undetected IVM bugs.

**Weaknesses:**

1. **Phase 2 "Exit criteria" mismatch with actual v0.18 proof.** The Phase 2
   exit criteria state:
   > "TPC-H Q1, Q3, Q5, Q6, Q11, Q21 all pass parity vs. DataFusion batch."

   The v0.18 sign-off proves: lowering, explain, catalog round-trip, and a
   plan-structure fuzzer. There is no batch-parity proof for any TPC-H query.
   Either the exit criteria should be moved to a later version within Phase 2,
   or the Phase 2 boundary should be redrawn. Currently, v0.18 claims Phase 2
   completion but has not met the Phase 2 exit criteria.

2. **Phase 4 exchange path classifier assumes stable peer topology.** The
   exchange path selection (`elided`/`loopback`/`direct`/`durable`) is decided
   per-batch at runtime. But the decision depends on "receiver reachable" —
   which under network partitions can oscillate rapidly. A batch sent via direct
   that is not ACKed must be re-sent via durable, but the outbox entry was
   already written for direct. The plan does not specify the state machine for
   path upgrade/downgrade mid-epoch. This will be the #1 correctness bug in
   Phase 4 if not addressed proactively.

3. **Phase 6 fault tolerance specifies recovery budgets (5s/30s/60s) without
   specifying what is measured.** Is "failure detection ≤ 5s" measured from the
   moment the worker process dies, or from the moment the control plane's
   heartbeat timer expires? Is "pipeline freshness recovery ≤ 60s" measured to
   the first new epoch commit, or to the frontier reaching the pre-failure
   position? These ambiguities will cause the chaos test to pass or fail
   depending on interpretation.

4. **Phase 8 packs too many independent features into one version (v0.43).**
   The v0.43 deliverables include: direct-write DML, CRDT column types,
   idempotency-key enforcement, optimistic transaction metadata hooks, session
   read-your-writes, INSERT RETURNING, max-staleness sessions, zero-downtime
   view replacement, write fences, background DDL, and schema-level lifecycle.
   This is at minimum 3 separate 10-person-week versions compressed into one.
   Any single feature here (e.g., INSERT RETURNING with post-commit point-read)
   has non-trivial interactions with the frontier model that deserve their own
   proof.

5. **Phase 12 (Cold Tier) has no specified fallback if SlateDB's compaction
   model conflicts with Iceberg's snapshot model.** The cold tier writes Iceberg
   v2 tables from checkpoint data. But if a compaction rewrites the SSTs that a
   cold-tier snapshot was reading mid-write, the snapshot writer sees a stale
   segment. The plan assumes checkpoint pinning prevents this, but the
   interaction between SlateDB checkpoint lifetime and Iceberg snapshot flush
   latency is not bounded. A slow Parquet write (large partition, S3 throttle)
   could exceed the checkpoint retention window.

### `ROADMAP.md` Analysis

**Strengths:**
- The "Evidence over dates" philosophy and the decision gates are genuinely
  rare in the industry. The explicit "no" criteria for the v0.55 coordinator
  group gate prevent premature commitment.
- The design-freeze directive after v0.10 with CI enforcement (`freeze-exception`
  trailer, 10-line-net threshold) is the correct mechanism.
- The "Things To Keep Out Until After 1.0" list is honest and correctly scoped.
  The temptation to add active-active multi-region writes is explicitly named
  and resisted.

**Weaknesses:**

1. **The "Developer Preview" milestone at v0.18 is premature given the
   implementation state.** The roadmap says: "Single-shard SQL engine demo-able
   to external users. Blog post + feedback loop." But the implementation can
   only lower plans and print explain output — it cannot actually execute a SQL
   query against real data and return results via psql. The `rockstream sql`
   command prints an explain plan, not query results. Calling this a "Developer
   Preview" risks external credibility if anyone tries to use it as described.

2. **The parallel work tracks table creates a false sense of parallelizability.**
   It says "Gateway and pgwire: can start seriously after v0.18." But the gateway
   requires distributed reads, cross-shard scatter, frontier-pinned snapshots,
   and session state management. These all depend on Phase 4-5 infrastructure
   that doesn't exist until v0.32. The "can prototype against single-shard
   snapshots" qualifier is buried in a note but the table implies v0.18 is a
   hard start signal.

3. **The common "Definition of Done" requires simulation tests for coordination
   paths, but Phases 1-2 have no coordination paths.** This means the simulation
   discipline has been dormant for 14 versions (v0.5–v0.18). The `SimRuntime`
   exists but its exercise has been limited to determinism checks, not adversarial
   fault injection. When Phase 4 arrives and suddenly needs simulation-proven
   coordination, the team will have forgotten how to write effective simulation
   tests.

4. **No version budget accounts for the documentation debt.** The roadmap
   specifies "a blog post" at v0.18, an "operator's guide" and "SQL reference"
   at v0.52, and "deployment playbooks" alongside. But no version budget includes
   documentation as a first-class deliverable with its own proof. The risk: v0.52
   arrives and the documentation sprint consumes an entire version budget,
   delaying production beta.

---

## 3. Concrete Improvement Proposals

### 3.1 Enforce the Design Freeze with a Hard Document Split

**Problem**: DESIGN.md at 5,173 lines is unmaintainable as a single document.
The design-freeze directive is violated by its own mass.

**Proposal**: Split DESIGN.md into:
- `DESIGN.md` — Principles, topology, storage layout, operator catalog, exchange,
  frontier protocol, epoch commit (§1–§9). ~2,500 lines. Frozen after v0.10.
- `DESIGN-GATEWAY.md` — Query serving, isolation, subscribe, HTAP ergonomics
  (§12). ~800 lines. Frozen after v0.40.
- `DESIGN-CONNECTORS.md` — Connector contracts, cold tier, catalog server,
  coordinator group (§13). ~1,000 lines. Frozen after v0.45.
- `DESIGN-OPS.md` — Operations, deployment, observability, security (§14–§17).
  ~800 lines. Frozen after v0.47.

Each split document carries its own `freeze-exception` CI gate tied to the
version at which it stabilizes. This makes the freeze enforceable rather than
aspirational.

### 3.2 Add a "Cloud Soak" Version Between v0.10 and v0.19

**Problem**: No version exercises real object-store latency.

**Proposal**: Insert **v0.18.1** (or rename the next version) as a "Storage
Operational Budget" version that:
- Runs the v0.18 SQL Alpha soak against S3-compatible storage (MinIO with
  simulated latency or actual S3).
- Measures and records: PUT p50/p95/p99, GET p50/p95/p99, LIST p50/p95/p99,
  manifest write cadence, WAL listing cost, compaction debt at 1GB/5GB/10GB
  shard sizes.
- Proves that `min_epoch_ms` floor prevents manifest churn below a budget.
- Establishes the first concrete numbers for `write_amplification_ratio`.
- Provides the evidence needed for the "Storage operational budget" decision
  gate that is currently listed but unenforced.

**Exit criteria**: All operator-hot-path latencies stay within 2x of in-memory
baseline at shard sizes up to 5GB on real object store.

### 3.3 Decompose Phase 3 into Risk-Ordered Sub-Phases

**Problem**: Phase 3 treats recursion and lateral subqueries as equal effort to
window functions.

**Proposal**: Reorder Phase 3 by descending risk and add explicit escape hatches:

| Version | Focus | Risk | Escape hatch |
|---|---|---|---|
| v0.19 | Window functions (partition recompute) | Medium | Segment-tree deferred; partition-size NOTICE is sufficient |
| v0.20 | Time windows + watermarks | **High** | If watermark contract proves too restrictive, allow `WATERMARK = PROCESSING_TIME` as default with explicit opt-in to fail-closed |
| v0.21 | Top-K | Low | N/A |
| v0.22 | Bootstrap/snapshot | Medium | Sequential bootstrap sufficient; streamed can be v0.22.1 |
| v0.23 | View-on-view DAG | Medium | Chain only; diamond deferred until frontier meet is proven |
| v0.24 | Recursion (semi-naive only) | **High** | DRed and full-recompute fallback are v0.24.1; ship monotone-only first |
| v0.25 | Lateral/SRF/UDF + UDAF hooks | Medium | UDAFs are interface-only; implementation deferred |
| v0.26–v0.27 | Correctness soak + performance | Low | N/A |

The key change: recursion and time-windows get explicit "monotone-only first"
escape hatches. If DRed proves intractable in a 10-week budget, the roadmap
does not stall.

### 3.4 Add Write-Path Backpressure to the Direct-Write Gateway

**Problem**: The direct-write connector has no admission control.

**Proposal**: The gateway maintains a per-shard write-credit budget (modeled on
the source connector's `credits_available()` signal). `COMMIT` blocks if the
target shard's pending write bytes exceed `direct_write_buffer_limit_bytes`
(default 64 MB). The blocked session receives a `NOTICE` after 1s and an error
(`RS-2019 write.shard_backpressure`) after `direct_write_timeout_ms` (default
30s). This mirrors Kafka's `linger.ms` + `buffer.memory` backpressure model.

Add to DESIGN.md §13.5 and IMPLEMENTATION_PLAN.md Phase 8 (v0.43).

### 3.5 Make the Simulation Discipline Continuous from v0.9

**Problem**: `SimRuntime` is dormant between v0.3 and v0.28.

**Proposal**: Starting at v0.9 (epoch commit and replay), every version must
include at least one `SimRuntime`-driven fault-injection test that exercises the
version's new correctness boundary:
- v0.9: kill-inject mid-commit (already exists).
- v0.13–v0.15: kill-inject mid-join-arrangement-write, verify replay produces
  identical join output.
- v0.16–v0.18: inject catalog-corruption (missing law version), verify
  `RS-5002` fires and the pipeline refuses to attach.

This keeps the simulation muscle exercised so Phase 4 doesn't start cold.

### 3.6 Redefine "Developer Preview" Scope Honestly

**Problem**: v0.18 is not demo-able to external users as currently implemented.

**Proposal**: Redefine the milestones:

| Milestone | Version | Actual meaning |
|---|---|---|
| SQL Alpha | v0.18 | Core SQL lowers correctly; explain and fuzzer prove structure. Internal milestone. |
| Developer Preview | v0.27 | Single-shard SQL engine runs end-to-end queries; external blog post appropriate. |
| SQL Beta | v0.36 | Multi-shard SQL with exactly-once; external pilot possible. |

This aligns expectations with implementation reality and prevents premature
external exposure that damages credibility.

### 3.7 Budget Documentation as a Version Deliverable

**Problem**: No version budget accounts for docs.

**Proposal**: Every phase boundary version (v0.10, v0.18, v0.27, v0.36, v0.45,
v0.52) includes a 2-person-week documentation deliverable in its scope:
- v0.18: Internal architecture overview (for contributors).
- v0.27: SQL dialect reference (what works, what doesn't, with examples).
- v0.36: Distributed deployment quickstart (3 workers, MinIO, end-to-end).
- v0.45: Connector development guide.
- v0.52: Full operator guide + runbook.

Add these to the ROADMAP common Definition of Done for phase-boundary versions.

---

## 4. Markdown Diff / Remediation Recommendations

### 4.1 DESIGN.md — Add Runtime Profile Transition Specification

**Location**: After §3.1 "Runtime Profiles: Tiny to Massive"

Add a new subsection `§3.1.1 Profile Transitions`:

```markdown
### 3.1.1 Runtime Profile Transitions

A pipeline transitions between runtime profiles via a control-plane command
(`ALTER PIPELINE ... SET RUNTIME_PROFILE = 'distributed'`). Transitions are
**epoch-aligned**: the control plane waits for the current epoch to commit,
quiesces the pipeline (no new source data accepted), re-plans operator
placement, and resumes at the next epoch. State is not migrated — it remains
in the same SlateDB shards on the same object-store prefix. Only the
placement map and exchange topology change.

Transitions are valid in one direction only: `embedded → single_worker →
distributed`. Downgrade is not supported; operators must destroy and recreate
the pipeline at a lower profile. This prevents the ambiguity of merging
multiple distributed shards back into one embedded shard.

The `visible_frontier` semantics do NOT change mid-pipeline. A pipeline
created in `embedded` mode that transitions to `distributed` retains its
`visible_frontier` on the originating worker; distributed reads use
`durable_frontier`. The transition command emits a `NOTICE` explaining
this semantic change.
```

### 4.2 IMPLEMENTATION_PLAN.md — Correct Phase 2 Exit Criteria

**Location**: Phase 2 exit criteria block

Replace:
```markdown
- TPC-H Q1, Q3, Q5, Q6, Q11, Q21 all pass parity vs. DataFusion batch.
```

With:
```markdown
- TPC-H Q1, Q3, Q5, Q6, Q11, Q21 all pass *plan-level* parity: lowered
  PlanNode graph is structurally equivalent to the expected join/aggregate/
  set-op topology. Batch-execution parity (actual row-level output equality)
  is deferred to Phase 3.5's TPC-H 22/22 correctness soak.
```

### 4.3 ROADMAP.md — Clarify Milestone Definitions

**Location**: Public Milestones table

Replace the current v0.18 row:
```markdown
| Developer Preview | v0.18 | Single-shard SQL engine demo-able to external users. Blog post + feedback loop. |
| SQL Alpha | v0.18 | Core SQL views, joins, set ops, and `EXPLAIN` work on one shard. |
```

With:
```markdown
| SQL Alpha (internal) | v0.18 | SQL compilation, plan lowering, explain, and deterministic soak pass. Internal validation gate — not externally demo-able. |
| Developer Preview | v0.27 | Single-shard SQL engine runs end-to-end with real data. First external demo and blog post. |
```

### 4.4 IMPLEMENTATION_PLAN.md — Add Storage Budget Version

**Location**: Between Phase 2 (v0.18) and Phase 3 (v0.19) sections

Add:
```markdown
### Storage Operational Budget Gate (between Phase 2 and Phase 3)

Before Phase 3 begins, the project must prove that the SlateDB operational
budgets specified in DESIGN.md §5.4 hold under real object-store latency
at shard sizes exceeding 1 GB. This is not a new roadmap version — it is a
gate that must be passed as part of the v0.19 entry criteria.

**Gate evidence required:**
- Object-store request p99 latencies for PUT/GET/LIST at 1GB and 5GB shard
  sizes on a real S3-compatible endpoint.
- Manifest write cadence measured under steady-state (100k rows/s source)
  and bursty (1M rows/s for 10s) load.
- WAL listing cache hit ratio > 99% under sustained operation.
- `write_amplification_ratio` measured and recorded.
- `min_epoch_ms` floor demonstrably prevents manifest churn.

If any budget is exceeded by >2x the specified target, the project must
file a tracking issue and either adjust the target or implement a mitigation
before advancing past v0.19.
```

### 4.5 DESIGN.md — Specify Direct-Write Backpressure

**Location**: §13.5 (Internal Source Connector section)

Add after the `ROLLBACK` paragraph:
```markdown
**Write admission control.** The gateway tracks per-shard pending write bytes
(`direct_write_pending_bytes{shard_id}`). When pending bytes exceed
`direct_write_buffer_limit_bytes` (default 64 MB per shard), new `COMMIT`
operations on sessions targeting that shard are blocked with a
`RS-2019 write.shard_backpressure` NOTICE after `direct_write_notice_ms`
(default 1000 ms) and rejected with the same error code after
`direct_write_timeout_ms` (default 30000 ms). This prevents unbounded
write-buffer growth when downstream IVM processing or object-store writes
cannot keep pace with application writes.

The admission signal is the shard's `credits_available()` equivalent for the
internal source: when the shard's uncommitted epoch buffer exceeds the limit,
credits are exhausted and writes queue. This mirrors the Kafka source's
credit-based flow control from §13.3.
```

### 4.6 ROADMAP.md — Add Simulation Continuity Requirement

**Location**: Common Definition of Done section

Add bullet:
```markdown
- Any version that introduces a new durable state transition (arrangement
  write, epoch commit, catalog mutation, checkpoint creation) includes at
  least one `SimRuntime` fault-injection test exercising kill/restart across
  that transition. The simulation discipline is continuous from v0.9, not
  deferred to Phase 4.
```

---

## 5. Risk Matrix (Post-v0.18)

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Phase 3 recursion proves intractable in 10pw | Medium | High — blocks all subsequent phases | Ship monotone-only (§3.3); DRed as follow-up |
| SlateDB compaction debt exceeds budget at 10GB+ | Medium | High — invalidates shard-size target | Add storage budget gate (§3.2) before Phase 3 |
| MergeLaw abstraction proves over-engineered | Low | Medium — refactor cost in Phase 4 | Payoff starts at v0.30; re-evaluate at CRDT gate |
| External "Developer Preview" demo damages credibility | High | Medium — perception, not technical | Redefine milestone (§3.6); no blog until v0.27 |
| Simulation atrophy between v0.3 and v0.28 | High | High — Phase 4 correctness risk | Continuous simulation from v0.9 (§3.5) |
| v0.43 scope creep delays Phase 8 by 2-3x | High | Medium — schedule pressure | Split v0.43 into 3 versions (CRDT, OLTP ergonomics, view lifecycle) |

---

## 6. Positive Observations

For balance, five things the specification documents do exceptionally well:

1. **The error-code taxonomy with CI enforcement** is best-in-class. No
   production streaming system (Flink, Kafka Streams, Materialize, RisingWave)
   has this discipline. It will pay dividends in support and debugging from
   day one.

2. **The decision-gate framework** with explicit "default action is not to
   accelerate" is structurally resistant to premature optimization pressure.
   This is the single most important cultural artifact in the project.

3. **The explicit non-goals list** prevents scope creep by naming temptations.
   "Not an OLTP Postgres clone" and "no global write sequence number" are
   particularly important boundaries that other projects (Materialize, CockroachDB)
   learned the hard way.

4. **The connector contract's `credits_available()` + `should_flush()`
   separation** solves the small-files problem for Iceberg/Delta sinks without
   breaking exactly-once semantics. This is a novel contribution.

5. **The MergeLaw `not_merge_safe_reason` closed enum** makes the system
   self-documenting: operators can inspect exactly why a given operator cannot
   use combiners or pushdown, rather than guessing. No existing system exposes
   this level of algebraic introspection to the user.

---

## 7. Conclusion

The v0.18 milestone represents a credible single-shard SQL compilation and
analysis engine. The theoretical foundations (DBSP, frontier algebra, merge laws)
are sound. The sequencing philosophy (correctness before scale, simulation from
the beginning, operability as a phase deliverable) is architecturally mature.

The primary risk at this juncture is not technical unsoundness — it is the
growing gap between specification ambition and implementation reality. The
project must resist the temptation to specify the v0.55 world in detail while
the v0.19 world remains unbuilt. The design freeze should be enforced
aggressively, the milestone definitions should be honest about what has been
proven, and the next 9 versions (Phase 3) should be risk-ordered with explicit
escape hatches for the hard problems (recursion, time-window watermarks).

The single most impactful action the project can take before v0.19 is to run
the existing test suite against a real object store and record the numbers.
Everything else in the design depends on SlateDB being fast enough at scale.
That assumption is currently unvalidated.

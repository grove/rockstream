# Post-Phase Assessment: v0.18 SQL Alpha / v0.27 Single-Shard Correctness

**Date**: 2026-05-30
**Assessor scope**: DESIGN.md (v3.28), IMPLEMENTATION_PLAN.md, ROADMAP.md
**Assessment trigger**: v0.18 SQL Scope Control gate passed; v0.27 Single-Shard
Correctness gate passed; project entering distributed phase (v0.28+)
**Implementation state**: v0.27.0 shipped (all versions v0.1–v0.27 marked Done)

---

## 1. Executive Summary & Core Weaknesses

The specification documents remain among the most thorough system design
documents in the stream-processing space. Since the v0.10 assessment, the
project has delivered 17 versions (v0.11–v0.27) covering the full SQL frontend,
inner/outer/semi/anti joins, set operations, window functions, time windows,
Top-K, recursion, bootstrap, view-on-view DAGs, lateral functions, approximate
sketches, the TPC-H 22/22 correctness soak, and the single-shard performance
profile. This is an exceptional implementation pace.

However, five critical architectural vulnerabilities threaten the project at
this critical juncture — the transition from single-shard to distributed
execution:

1. **The design freeze is systematically violated.** DESIGN.md grew from v3.13
   (at the v0.10 gate) to v3.28 — 15 major revisions adding coordinator
   groups, cold-tier Iceberg sinks, DuckLake catalogs, HTAP session
   ergonomics, secondary indexes, shard column statistics, and more. The
   ROADMAP.md specifies CI enforcement (`freeze-exception` trailers, 10-line
   limit on non-exempt PRs), but this enforcement is demonstrably not active.
   The design document is now 5,200+ lines and continues to function as the
   primary work product rather than an implementation reference. This creates
   two risks: (a) premature commitment to decisions that should remain open
   until the distributed phase reveals real constraints, and (b) cognitive
   load that makes the document harder to use as a reference during active
   implementation.

2. **Escape-hatch outcomes from Phase 3 are not consolidated.** Three escape
   hatches were exercised: DRed recursion proved unsound under concurrent
   deletes (v0.22), HOP/SESSION windows were deferred to v0.21 during v0.20,
   and HLL accuracy was confirmed sufficient (v0.21). These outcomes are
   recorded in individual sign-offs but are not consolidated in
   IMPLEMENTATION_PLAN.md or ROADMAP.md as "known limitations entering the
   distributed phase." DESIGN.md §6.8 still describes DRed as a live strategy
   option for "monotone mixed insert/delete/update recursion" despite the
   implementation rejecting non-monotone terms with RS-1009. This is a
   specification-implementation divergence that will confuse future readers.

3. **The storage operational budget gate is ambiguously validated.** The
   IMPLEMENTATION_PLAN.md storage gate between Phase 2 and Phase 3 requires
   validation "on a real S3-compatible endpoint" with "object-store request
   p99 latencies for PUT/GET/LIST at 1GB and 5GB shard sizes." The v0.27
   sign-off includes storage budget tests (PUT p99 < 200ms, GET p99 < 100ms),
   but these run under `SimRuntime` with in-memory object store, not against
   real S3. The project has not yet confronted real object-store tail latency
   at multi-GB shard sizes. This is the single highest-risk gap entering the
   distributed phase, because every distributed mechanism (exchange, frontier,
   checkpoint, recovery) amplifies object-store costs.

4. **Phase 4 (distributed) is the largest complexity cliff in the project.**
   The transition from single-shard to multi-shard introduces: shard leasing
   and scheduling, gRPC exchange with four path types, rendezvous hashing,
   distributed operator placement, credit-based backpressure across workers,
   and distributed recursion. The IMPLEMENTATION_PLAN.md bundles all of this
   into three versions (v0.28–v0.30), each at 10 person-weeks. The v0.10
   assessment flagged Phase 8 scope as aggressive; the same concern applies to
   Phase 4. The control plane (v0.28) alone — worker discovery, topology
   catalog, mTLS scaffolding, shard manager, lease acquisition, writer fencing
   — is a full 10-person-week version without any exchange or shuffle work.

5. **Cross-document terminology and reference drift has accumulated.** Multiple
   small inconsistencies between DESIGN.md, IMPLEMENTATION_PLAN.md, and
   ROADMAP.md have accrued over the 17-version implementation sprint: the
   `rockstream sql` CLI subcommand (shipped in v0.18) is absent from
   DESIGN.md §14.7's CLI surface; error code RS-1009 (recursion rejection) is
   outside its documented owner range (RS-1000–1499 is connector/source/sink);
   IMPLEMENTATION_PLAN.md Phase 3 Milestone IVM-10 still describes DRed as a
   live compiler strategy; the Phase overview table doesn't reflect that all
   Phase 0–3.5 rows are completed; and the `rockstream explain` command
   syntax in Phase 1 operability deliverables references an older form than the
   current `EXPLAIN INCREMENTAL` surface.

---

## 2. In-Depth Architectural Critique

### 2.1 DESIGN.md Analysis

**Strengths (since v0.10):**
- The v3.28 coherence pass successfully resolved error-code collisions,
  tightened the freshness contract with latency classes (§3.0), specified
  stable CDC row identity (§6.4), and replaced ambiguous key-range splitting
  with virtual buckets (§7.1, §10.2).
- The 10-state bucket migration state machine (§10.2) with per-state timeouts,
  idempotent transitions, and frontier-gated cleanup is a significant
  improvement over the v0.10 design's implicit migration model.
- The arrangement working-set management (§6.12), write amplification budget
  (§5.4), and exchange path thresholds (§7.2) — all recommendations from the
  v0.10 assessment — have been incorporated.
- The CALM epoch-commit invariant (§8.4) giving external tools verifiable
  snapshot safety is an elegant architectural contribution.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| §6.8 Recursion | DRed is listed as a strategy for "monotone mixed insert/delete/update recursion" but was proved unsound and deferred in v0.22. The design should reflect the implemented reality: semi-naive only for monotone insert-only; full recomputation fallback for everything else; DRed deferred with RS-1009 rejection. | High |
| §14.7 CLI | The `rockstream sql "<query>"` subcommand shipped in v0.18 is not listed in §14.7's CLI surface. This is a user-facing feature gap in the design reference. | Medium |
| §14.14 Error codes | RS-1009 (`recursion.non_monotone_not_supported`) falls in the RS-1000–1499 "Connector / source / sink ingestion" range but is a DDL/compiler error. It should be in the RS-1500–1999 "Schema / DDL validation" range. | Low |
| §3.1.1 Transitions | Runtime profile transitions are "one direction only: embedded → single_worker → distributed" with no downgrade. This is pragmatic but the explicit rejection should document the workaround (export + recreate). | Low |
| §17 Simulation | The simulation testing section doesn't address the gap between `SimRuntime` (in-memory object store) and real S3 behavior. Key differences include: S3 conditional writes, LIST-after-PUT consistency variance across providers, and HTTP 429 rate limiting. The simulation should document which S3 behaviors it models and which it doesn't. | Medium |

**IVM Engine Soundness:**
The single-shard IVM engine has been thoroughly validated through v0.27:
TPC-H 22/22, Nexmark subset, random query fuzzer (1 hour+), law-equivalence
corpus, and per-law RMW-avoidance measurement. The DBSP formalism continues to
provide correct foundations. The remaining IVM risks are:

- **Partition-based window recomputation** (§6.7) is O(partition_size) per
  change. The segment-tree optimization for sliding aggregates was deferred.
  This will become a production concern for large-partition window functions
  under tight SLOs. The EXPLAIN NOTICE at `partition_recompute_warn_threshold`
  (default 100k) is correct mitigation documentation, but the optimization
  itself should be tracked as a Phase 4+ follow-up.

- **Recursive DRed deferral** means non-monotone recursive views are rejected.
  This is a SQL coverage gap that should be prominently documented in the
  "SQL reference" planned for Phase 10, not buried in an error code.

### 2.2 IMPLEMENTATION_PLAN.md Analysis

**Strengths (since v0.10):**
- Every version from v0.11 through v0.27 has been delivered with concrete
  sign-off evidence, testable exit criteria, and oracle-backed correctness
  proof.
- The escape-hatch pattern (define Plan A, specify Plan B fallback, document
  which was chosen) is excellent engineering discipline demonstrated in v0.20
  (TUMBLE only) and v0.22 (DRed deferred).
- The storage operational budget gate between Phase 2 and Phase 3 was a
  direct incorporation of the v0.10 assessment recommendation.
- Phase 4 exit criteria now require "4 hosts × 4 shards minimum, real network"
  — another v0.10 recommendation successfully incorporated.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| Phase overview table | The table maps phases to ROADMAP versions but doesn't indicate completion status. Phases 0–3.5 are all complete; the table should show this. | Low |
| Phase 3 IVM-10 (Recursion) | Still lists "DRed for monotone mixed insert/delete/update recursion" as a compiler strategy without noting the v0.22 escape-hatch outcome. The escape hatch section at the end correctly describes the fallback, but the main description is misleading. | High |
| Storage budget gate | The gate documentation says "This is not a new roadmap version — it is a gate that must be passed as part of the v0.19 entry criteria." The current formulation doesn't specify whether the gate was satisfied with SimRuntime or real object storage. | High |
| Phase 4 scope | v0.28 (control plane + worker discovery), v0.29 (shard leasing + scheduling), and v0.30 (exchange + combiners) are individually underscoped at 10 person-weeks each. v0.28 alone includes: control service, worker registration with capacity reporting, topology catalog, bootstrap command, mTLS scaffolding, and role flags. This is infrastructure that requires careful security engineering (mTLS), integration testing, and operational validation. | Medium |
| Escape-hatch tracking | No consolidated section tracks which escape hatches were triggered and their implications for later phases. DRed deferral affects Phase 4 (distributed recursion falls back to semi-naive only). HOP/SESSION deferral was resolved in v0.21. These should be summarized before Phase 4 begins. | Medium |

### 2.3 ROADMAP.md Analysis

**Strengths (since v0.10):**
- Decision gates at v0.18 (SQL scope control) and v0.27 (Single-shard
  correctness) are both passed. The evidence from sign-offs supports this.
- The Developer Preview milestone was placed at v0.27 (not v0.18 as the v0.10
  assessment suggested, but the later placement aligns better with the
  project's actual demo-ability timeline).
- The version-effort variance caveat ("Foundation versions typically
  under-budget; gateway/connector versions typically over-budget") was added
  per the v0.10 recommendation.
- The storage operational budget decision gate was added after v0.10.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| Decision gate status | The decision gates table lists gates at v0.4, v0.10, v0.18, v0.27 but doesn't record their outcomes. Past gates should be marked with their result and date. | Medium |
| Design freeze enforcement | The freeze directive ("every DESIGN.md commit should be small and targeted") and CI enforcement ("freeze-exception trailer, 10-line limit") are specified but demonstrably not enforced — DESIGN.md has had 15 major revisions since v0.10. Either enforce the freeze or update the directive to acknowledge that the design is a living document through the single-shard phase, with the freeze taking effect at v0.27. | High |
| Completed-version annotations | Advanced IVM versions (v0.19–v0.27) are correctly marked Done with scope, but several lack escape-hatch outcome annotations. v0.20 should note "Escape hatch applied: TUMBLE only; HOP/SESSION deferred." v0.22 should note "Escape hatch applied: DRed deferred; non-monotone terms rejected with RS-1009." | Medium |
| Parallel work tracks | The table says "Gateway and pgwire: Can start seriously after v0.18." With v0.18 passed, this track is now eligible. But the gateway design (§12.6) depends on the distributed exchange (Phase 4) for multi-shard reads. The table should clarify that gateway prototyping against single-shard snapshots can start now, but full multi-shard gateway requires Phase 5 (frontier protocol). | Low |

---

## 3. Concrete Improvement Proposals

### 3.1 Enforce the Design Freeze Starting Now (v0.27)

**Problem:** The design freeze was specified for v0.10 but 15 major revisions
have been merged since. The freeze has no practical enforcement.

**Proposal:** Update ROADMAP.md to acknowledge the freeze was deferred to v0.27
(the natural single-shard correctness boundary) and enforce it from this point:

- The CI enforcement described in ROADMAP.md (freeze-exception trailer,
  10-line limit) activates on the commit following the v0.27 merge.
- DESIGN.md and IVM.md become implementation references only.
- New design decisions required for v0.28+ are tracked as GitHub issues with
  targeted, small corrections — never as numbered "v3.X" revision passes.

### 3.2 Consolidate Escape-Hatch Outcomes

**Problem:** Escape-hatch results are scattered across sign-offs and not
reflected in the specification documents.

**Proposal:** Add an "Escape Hatches Exercised" section to IMPLEMENTATION_PLAN.md
after Phase 3.5, and update DESIGN.md to match:

| Version | Escape Hatch | Outcome | Impact on Later Phases |
|---------|-------------|---------|----------------------|
| v0.20 | HOP/SESSION windows | Applied: TUMBLE only in v0.20; HOP/SESSION shipped in v0.21. | Resolved — no impact. |
| v0.21 | HLL accuracy | Not triggered: accuracy sufficient. | No impact. |
| v0.22 | DRed recursion | Applied: DRed proved unsound under concurrent deletes. Non-monotone terms rejected with RS-1009. | Phase 4 distributed recursion uses semi-naive only. Non-monotone recursive views remain unsupported. |

Update DESIGN.md §6.8 to replace "DRed for monotone mixed insert/delete/update
recursion" with "DRed deferred (proved unsound in v0.22); non-monotone terms
rejected with RS-1009."

### 3.3 Resolve the Storage Budget Gate Ambiguity

**Problem:** The storage gate requires real object-store validation but it's
unclear whether this has been done.

**Proposal:** Add a note to the storage budget gate documentation in
IMPLEMENTATION_PLAN.md that explicitly states: "The v0.27 storage budget tests
validated budgets under `SimRuntime` with in-memory object store. Real S3
validation at 1GB+ shard sizes is required before Phase 4 v0.30 (exchange)
ships." This resolves the ambiguity without adding a new roadmap version.

### 3.4 Add `rockstream sql` to the Design CLI Surface

**Problem:** The `rockstream sql "<query>"` subcommand shipped in v0.18 is
missing from DESIGN.md §14.7.

**Proposal:** Add to §14.7:

```
rockstream sql    "<query>"               # parse, lower, and print EXPLAIN
                                          # INCREMENTAL against the catalog
```

### 3.5 Fix Error Code RS-1009 Range Assignment

**Problem:** RS-1009 is used for `recursion.non_monotone_not_supported` but
falls in the RS-1000–1499 "Connector / source / sink" range.

**Proposal:** Reassign to RS-1509 (`recursion.non_monotone_not_supported`) in
the RS-1500–1999 "Schema / DDL validation" range. Update the error code
registry in `rockstream-types` and the IMPLEMENTATION_PLAN.md reference.

### 3.6 Document Simulation Fidelity Boundaries

**Problem:** The simulation testing section (§17) doesn't distinguish between
behaviors modeled by `SimRuntime` and real S3 behaviors that are not modeled.

**Proposal:** Add a §17.8 "Simulation Fidelity Boundaries" subsection:

| Behavior | Modeled by SimRuntime | Real S3 |
|----------|----------------------|---------|
| Latency distribution | Uniform random (configurable) | Long-tailed, prefix-dependent |
| Conditional writes (If-Match) | Yes (in-memory CAS) | Yes (S3 conditional writes) |
| LIST consistency after PUT | Immediate | Strong (since Dec 2020 for S3; varies for other providers) |
| Rate limiting (HTTP 429) | Via `buggify!()` injection | Provider-specific, prefix-scoped |
| Partial object writes | Not modeled | Can occur on large PUTs |

Behaviors not modeled by `SimRuntime` must be covered by integration tests
against real object storage (MinIO minimum) at the Phase 4 and Phase 6 gates.

### 3.7 Update Phase Overview Table Completion Status

**Problem:** The IMPLEMENTATION_PLAN.md phase overview table doesn't show which
phases are completed.

**Proposal:** Add a "Status" column:

| Phase | ROADMAP versions | Status | Focus |
|-------|-----------------|--------|-------|
| 0 | v0.1–v0.4 | ✅ Complete | Repository, simulation, storage, no-op pipeline |
| 1 | v0.5–v0.10 | ✅ Complete | Single-shard IVM core (IVM-1 … IVM-3) |
| 2 | v0.11–v0.18 | ✅ Complete | SQL frontend, joins, set ops (IVM-4 … IVM-6) |
| 3 | v0.19–v0.26 | ✅ Complete | Advanced operators (IVM-7 … IVM-12) |
| 3.5 | v0.27 | ✅ Complete | IVM correctness soak (IVM-13) |
| 4 | v0.28–v0.30 | Not started | Multi-shard execution and exchange |

### 3.8 Annotate Decision Gate Outcomes

**Problem:** ROADMAP.md decision gates don't record their outcomes.

**Proposal:** Add an "Outcome" column to the decision gates table:

| Gate | After | Question | Outcome |
|------|-------|----------|---------|
| Architecture sanity | v0.4 | Do SlateDB, the runtime abstraction, and local developer ergonomics still fit the design? | ✅ Passed. Confirmed. |
| IVM kernel confidence | v0.10 | Is the core delta engine simple enough to debug, and does replay work cleanly? | ✅ Passed. Full assessment in plans/full-assessment-v0.10.md. |
| Storage operational budget | v0.10 | Do SlateDB operational budgets hold at 5GB+ shard sizes on real object storage? | ⚠ Partial — validated under SimRuntime. Real S3 validation pending. |
| SQL scope control | v0.18 | Are we still building the right SQL subset first, or have edge cases started to dominate? | ✅ Passed. SQL Alpha soak clean. |
| Single-shard correctness | v0.27 | Is the IVM engine correct and fast enough to justify distribution work? | ✅ Passed. TPC-H 22/22, fuzzer, law-equivalence corpus clean. |

---

## 4. Markdown Diff / Remediation Recommendations

### 4.1 DESIGN.md §6.8 — Update Recursion to Reflect DRed Deferral

In the compiler strategy selection area of §6.8, replace text describing DRed
as a live strategy option with:

> **DRed (delete-and-rederive) was evaluated in v0.22 and proved unsound under
> concurrent deletes; non-monotone recursive terms are rejected with `RS-1509
> recursion.non_monotone_not_supported`.** DRed may be revisited as a future
> optimization once the distributed recursion surface (Phase 4) stabilizes.
> The implemented strategies are: semi-naive for monotone insert-only recursion;
> full recomputation fallback for non-monotone terms.

### 4.2 DESIGN.md §14.7 — Add `rockstream sql` Subcommand

Add after `rockstream debug    arrangement ...`:

```
rockstream sql    "<query>"               # parse, lower to PlanNode IR, and print
                                          # EXPLAIN INCREMENTAL against the catalog
```

### 4.3 DESIGN.md §14.14 — Fix RS-1009 Range

Move RS-1009 from the connector range to the DDL range. In the canonical
registry, replace:

```
RS-1009  recursion.non_monotone_not_supported
```

with (in the RS-1500–1999 Schema / DDL validation section):

```
RS-1509  recursion.non_monotone_not_supported
```

### 4.4 DESIGN.md §17 — Add Simulation Fidelity Boundaries

Add a new §17.8 "Simulation Fidelity Boundaries" subsection documenting
the gap between SimRuntime's in-memory object store and real S3 behavior,
per the table in §3.6.

### 4.5 IMPLEMENTATION_PLAN.md — Update Phase Overview with Status

Add a Status column to the Phase overview table showing completion state for
Phases 0–3.5 (all ✅ Complete).

### 4.6 IMPLEMENTATION_PLAN.md — Annotate IVM-10 DRed Deferral

In Phase 3, Milestone IVM-10 (Recursion), update the compiler strategy
selection to note that DRed was deferred:

> **v0.22 outcome**: DRed proved unsound under concurrent deletes.
> Non-monotone recursive terms are rejected with RS-1509. Only semi-naive
> (monotone insert-only) and full recomputation are implemented.

### 4.7 IMPLEMENTATION_PLAN.md — Add Escape-Hatch Summary After Phase 3.5

Add a new section after the Phase 3.5 exit criteria:

```markdown
### Escape Hatches Exercised (Phase 0–3.5 Summary)

| Version | Escape Hatch | Outcome | Impact on Later Phases |
|---------|-------------|---------|----------------------|
| v0.20 | HOP/SESSION windows deferred | Applied: TUMBLE only in v0.20. HOP/SESSION shipped in v0.21 — resolved. | None. |
| v0.21 | HLL accuracy fallback | Not triggered: HLL accuracy sufficient for cost-model correctness. | None. |
| v0.22 | DRed recursion unsound | Applied: DRed proved unsound under concurrent deletes. Non-monotone terms rejected with RS-1509. | Phase 4 distributed recursion uses semi-naive only. Non-monotone recursive views remain unsupported. DRed is a candidate future optimization. |
```

### 4.8 IMPLEMENTATION_PLAN.md — Clarify Storage Budget Gate Scope

Add a note to the storage budget gate section:

> **v0.27 status**: storage budget tests validated under `SimRuntime` with
> in-memory object store. The PUT p99 < 200ms and GET p99 < 100ms gates pass
> in-memory. Real S3 validation at 1GB+ shard sizes is required as a Phase 4
> entry condition before v0.30 (exchange) ships.

### 4.9 ROADMAP.md — Update Design Freeze Directive

Replace the current design freeze text to acknowledge deferral to v0.27:

> **Design freeze after v0.27.** The design freeze was originally specified for
> v0.10 but was deferred through the single-shard implementation phase
> (v0.11–v0.27) to allow design refinements informed by implementation
> experience. As of v0.27, DESIGN.md has stabilized at v3.28. From this point
> forward, new sections may not be added to DESIGN.md or IVM.md unless they are
> required to unblock a specific coded milestone.

### 4.10 ROADMAP.md — Record Decision Gate Outcomes

Update the decision gates table to include outcome annotations for passed
gates (v0.4, v0.10, v0.18, v0.27), per the table in §3.8.

### 4.11 ROADMAP.md — Annotate Escape Hatches in Version Table

In the Advanced IVM version table, add escape-hatch annotations to v0.20
and v0.22:

- v0.20 Scope: add "**Escape hatch applied**: TUMBLE only implemented;
  HOP/SESSION deferred to v0.21."
- v0.22 Scope: add "**Escape hatch applied**: DRed proved unsound under
  concurrent deletes; non-monotone terms rejected with RS-1509."

---

## 5. Cross-Document Alignment Findings

| # | Finding | Documents | Severity | Recommendation |
|---|---------|-----------|----------|----------------|
| 1 | DESIGN.md §6.8 lists DRed as a live strategy; IMPLEMENTATION_PLAN IVM-10 does the same; but v0.22 sign-off documents DRed deferral. Specification-implementation divergence. | DESIGN × IMPL × Sign-off | High | Update both documents per §4.1, §4.6. |
| 2 | ROADMAP.md design freeze says "after v0.10" but DESIGN.md went through 15 revisions since v0.10. The directive is not enforced. | ROADMAP × DESIGN | High | Update freeze to "after v0.27" per §4.9. |
| 3 | IMPLEMENTATION_PLAN.md Phase overview table has no completion status column. All Phase 0–3.5 entries are complete but unmarked. | IMPL | Medium | Add Status column per §4.5. |
| 4 | `rockstream sql` subcommand (v0.18) is absent from DESIGN.md §14.7 CLI surface. | DESIGN × IMPL | Medium | Add per §4.2. |
| 5 | RS-1009 (recursion rejection) is in the RS-1000–1499 connector range, not the RS-1500–1999 DDL range where it belongs. | DESIGN × IMPL | Low | Reassign to RS-1509 per §4.3. |
| 6 | ROADMAP decision gates have no recorded outcomes. Gates at v0.4, v0.10, v0.18, v0.27 have all passed but the table doesn't show this. | ROADMAP | Medium | Add outcomes per §4.10. |
| 7 | ROADMAP escape-hatch outcomes not annotated on version rows. v0.20 and v0.22 exercised escape hatches but the version descriptions don't note this. | ROADMAP | Medium | Add annotations per §4.11. |
| 8 | Storage budget gate status is ambiguous — validated under SimRuntime but gate requires "real S3-compatible endpoint." | IMPL × ROADMAP | High | Resolve per §4.8. |

---

## 6. Scaling Spectrum Assessment

### Single-Process Ergonomics (Grade: A-)

Significant improvement since v0.10:

- The `rockstream sql "<query>"` subcommand (v0.18) provides a zero-setup
  way to explore SQL lowering and explain output.
- The `GENERATE ROWS` source enables a working materialized view in under
  two minutes with no external dependencies.
- The embedded runtime profile correctly elides distributed overhead.
- Error codes with `RS-XXXX` are consistently applied.
- The v0.27 performance profile demonstrates ≥10x IVM-vs-batch speedup at 1%
  change rate.

The one remaining concern is cognitive load: the developer story requires
understanding Z-set semantics, epoch boundaries, and frontier vocabulary to
interpret `EXPLAIN INCREMENTAL` output. This is inherent to the domain but
could be mitigated with a "Getting Started" tutorial that explains these
concepts in 5 minutes.

### Cloud-Native Distributed Scale (Grade: B)

The architecture remains sound in principle. The v0.10 concerns are largely
addressed in the design (arrangement working-set management, exchange
thresholds, write amplification budgets). However:

- **The distributed phase is entirely unproven.** All 27 versions have run
  single-shard. The multi-shard exchange, frontier protocol, checkpoint
  coordination, and recovery mechanisms exist only as design sections.
- **Object-store request amplification is the real scaling bottleneck.** The
  v0.27 storage budget tests validate per-shard costs, but the cluster-wide
  cost is `per_shard_cost × shard_count`. At 1000 shards with 100ms epochs,
  the cluster produces 10,000 object-store writes/second sustained. This is
  within S3's documented limits but leaves no margin for spikes.
- **The frontier aggregator's scalability** with "thousands of shards ×
  hundreds of operators" is specified but unvalidated. Phase 5 must prove
  this or the architecture has a hidden centralization bottleneck.

---

## 7. Performance Engineering Assessment

### Throughput

The v0.27 benchmarks demonstrate:
- Filter throughput: ≥1M rows/s (in-memory), ≥500k rows/s (local FS)
- GROUP BY SUM: ≥200k rows/s (in-memory), ≥100k rows/s (local FS)
- GROUP BY MIN: ≥100k rows/s (in-memory), ≥50k rows/s (local FS)
- IVM vs batch speedup: ≥10x at 1% change rate

These numbers meet the Phase 1 targets. The Phase 3.5 targets (≥10x vs
batch for TPC-H at 1% change rate) are also met.

### Latency

The embedded latency class validation (p95 `commit_to_visible_ms` < 5ms for
trivial workloads) is specified but the v0.27 sign-off focuses on throughput
benchmarks, not latency. The latency-class taxonomy (§3.0) is well-designed
but needs quantitative validation before Phase 4.

### Resource Utilization

The per-law RMW-avoidance ratio is a strong signal:
- `WeightAdd/v1` and `SumCount/v1`: 100% avoidance (abelian group)
- `MaxRegister/v1` and `MinRegister/v1`: 0% avoidance (requires RMW)
- `HyperLogLog/v1` and `BloomUnion/v1`: 0% avoidance (requires RMW)

The 100% avoidance for the hot-path aggregate laws validates the merge-law
architecture's performance claim. The 0% for semilattice laws is expected and
correctly documented.

---

## 8. Operability Assessment

### Day-0 Experience (Grade: A)

Excellent. `rockstream start --storage=./data` + `CREATE SOURCE FROM GENERATE
ROWS` + `CREATE MATERIALIZED VIEW` is a compelling zero-dependency onboarding
story. The `rockstream sql` subcommand adds a lightweight exploration mode.

### Day-1 Operations (Grade: B+)

`EXPLAIN INCREMENTAL` with three levels (default, VERBOSE, ANALYZE) is
well-specified. The backfill cost preview prompt prevents accidental expensive
deployments. Workload DDL with SLO-driven configuration is intuitive. Named
degraded states with audit trail are production-ready discipline.

### Day-2 Operations (Grade: Incomplete)

The observability stack (metrics, tracing, dashboards, support bundle) is
extensively specified but ships primarily in Phase 10 (v0.47). Core hot-path
metrics ship in v0.10/v0.11, which is correct, but the full operational
toolkit (OTEL traces, admin CLI, IVM debugger, dashboard templates) is
20+ versions away from the current state.

---

## 9. Final Recommendations

The project is at a critical inflection point. The single-shard IVM engine is
proven correct and performant. The next 10 versions (v0.28–v0.37) introduce
the distributed execution layer that will determine whether the architecture
delivers on its promise. Five actions should be taken before v0.28 begins:

1. **Enforce the design freeze now.** DESIGN.md at v3.28 is comprehensive
   enough for the distributed phase. New decisions should be GitHub issues
   with targeted corrections, not document revisions. Activate the CI
   enforcement described in ROADMAP.md.

2. **Run the storage budget benchmarks against real object storage.** The
   v0.27 benchmarks against SimRuntime are necessary but not sufficient.
   Before Phase 4 begins, run the same suite against MinIO over a real
   network at 1GB and 5GB shard sizes. Record the results. This is a
   one-day effort that could prevent a multi-month rearchitecture.

3. **Update DESIGN.md, IMPLEMENTATION_PLAN.md, and ROADMAP.md** to reflect
   the escape-hatch outcomes, decision gate results, and phase completion
   status documented in this assessment. These documents are the project's
   institutional memory; they must reflect reality.

4. **Split Phase 4 if scope exceeds budget.** v0.28 (control plane + worker
   discovery) is individually substantial. If it exceeds 10 person-weeks,
   split rather than rush. The roadmap philosophy ("split before rushing")
   explicitly endorses this.

5. **Track the DRed deferral and sliding-window segment-tree as formal
   follow-up issues.** These are not blockers but they are SQL coverage
   gaps that prospective users will notice. They should be visible in the
   project's issue tracker, not buried in sign-off files.

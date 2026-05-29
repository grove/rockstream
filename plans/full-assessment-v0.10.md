# Post-Phase Assessment: v0.10 Developer Alpha

**Date**: 2026-05-29
**Assessor scope**: DESIGN.md (v3.28), IMPLEMENTATION_PLAN.md, ROADMAP.md
**Assessment trigger**: v0.10 IVM Kernel Confidence gate passed

---

## 1. Executive Summary & Core Weaknesses

The specification documents are extraordinarily thorough — among the most
detailed system design documents in the stream-processing space. The v0.10
implementation proves the single-shard IVM core works. However, five critical
architectural vulnerabilities threaten the project's stated goal of "the
absolute best cloud-native IVM system ever created":

1. **Speculative over-design past the implementation horizon creates false
   confidence.** DESIGN.md is 3,000+ lines describing v0.55+ features
   (Coordinator Group, cold-tier Iceberg, DuckLake, multi-shard SERIALIZABLE)
   while the implementation is at v0.10 — a single-shard engine that processes
   `RecordBatch` deltas in memory. The gap between specified and proven creates
   a risk of premature commitment to decisions that should remain open. The
   design freeze directive at v0.10 is correct but is already violated by the
   document's own bulk.

2. **SlateDB coupling is underexplored under distributed load.** The design
   treats SlateDB as a proven substrate but the implementation has only exercised
   it in single-shard, in-memory-object-store mode. Real object-store latency
   (S3 p99 > 200ms for small PUTs), compaction amplification at 20GB shard
   targets, and merge-operator behavior under concurrent `DbReader` snapshots
   remain empirically unvalidated. The "determinism gate" in Phase 0 tests
   bit-identical replay — it does not test latency amplification or operational
   budget compliance under realistic I/O profiles.

3. **The MergeLaw abstraction carries significant accidental complexity for
   Phase 1-3 deliverables.** The law catalog, property-test harness, and
   per-arrangement `(law_id, law_version)` header overhead are threaded through
   every layer from v0.5 onward, but their payoff (exchange combiners, gateway
   pushdown, CRDT columns) doesn't materialize until v0.30+ / v0.41+ / v0.43+.
   Until then, the abstraction creates cognitive overhead and API surface area
   for two functional laws (`WeightAdd/v1`, `SumCount/v1`). This is a bet on
   algebraic unification that may or may not pay off — the design treats it as
   certain.

4. **No concrete memory management or back-pressure model for arrangement
   state.** The design specifies `state_budget_gb` quotas (Phase 3), but the
   arrangement storage model pushes all state to SlateDB (LSM on object store).
   There is no articulated hot-arrangement cache, no eviction policy for
   in-memory arrangement segments, and no mechanism for an operator to spill
   gracefully when its working set exceeds available RAM. The `segment_cache`
   (§5.4) is for cross-shard reads, not for the primary operator write path.
   Under high-cardinality GROUP BY or large join states, operators will either
   OOM or pay unbounded object-store latency on every row.

5. **The 55-version roadmap (v0.1–v0.55) with 10 person-weeks per version
   implies ~11 person-years before 1.0.** At 8-9 engineers this is 14-16
   months of calendar time assuming zero rework, zero scope creep, and perfect
   parallelization. For a project at v0.10 with zero external users, this is an
   extremely long feedback loop. The roadmap lacks any mechanism for an early
   "minimum useful product" that could generate real-world signal before the
   multi-year journey is complete. The Developer Alpha (v0.10) is useful only to
   the development team itself.

---

## 2. In-Depth Architectural Critique

### 2.1 DESIGN.md Analysis

**Strengths:**
- The DBSP formalism gives provable correctness guarantees. The choice to use
  Feldera as semantic reference and pg_trickle as oracle is sound.
- The dual-frontier model (`visible_frontier` vs `durable_frontier`) elegantly
  solves the laptop-to-cluster freshness contract.
- The virtual-bucket-based partition function (§7.1) with rendezvous hashing is
  a correct choice that avoids the key-range scan problem during rebalancing.
- The latency-class taxonomy (§3.0) and the explicit non-goals (§1.1) show
  architectural maturity.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| §5 Storage | No articulated write amplification budget. LSM write-amp at 20GB targets with aggressive merge operators could be 10-30x. Design says "min_epoch_ms floors" but doesn't bound total write-amp. | High |
| §6.2 Aggregation | `db.merge()` with associative operators assumes SlateDB resolves merge operands on read. Design acknowledges fallback to RMW but doesn't quantify the performance cliff: going from O(1) merge-append to O(state) read-modify-write per row is a 1000x regression for hot groups. | High |
| §7.2 Exchange | The "direct + durable" hybrid has no adaptive threshold documented. "Batch too large" and "receiver unavailable" are stated but there's no concrete threshold, no hysteresis, and no mechanism to prevent oscillation between paths. | Medium |
| §9.3 Scheduling | Cooperative scheduling with `max_rows_per_quantum = 64k` is a good start, but the design doesn't address priority inversion: a heartbeat-critical task can still be starved if all executor threads are in quantum-bounded polls. The `higher priority` claim needs a concrete tokio mechanism (separate runtime? priority hints?). | Medium |
| §10.2 Migration | The 10-state migration state machine is thorough but has no documented timeout per state. A stuck `CATCHING_UP` phase (donor advancing faster than recipient can replay) has no escalation path other than operator intervention. | Medium |
| §12.7 Two-Tier Storage | The hot/cold merge is described as "signed Z-set merge ordered by epoch" but this requires maintaining delete tombstones in the cold tier for non-monotone views. The Parquet/Iceberg format has no native delete tracking — the design needs to specify whether it uses Iceberg v2 equality deletes, positional deletes, or a custom side-car. | Low (v0.53 concern) |
| §13.5 Direct Write | The transaction-shape classifier (`TxnShape` enum) creates an explosion of code paths before any of them are exercised. Designing the full taxonomy pre-implementation risks building abstractions for shapes that never appear in real workloads. | Medium |
| §14 Operations | Observability is specified in detail but there's no discussion of metric cardinality explosion. With `metrics per (law_id, law_name, law_version, op_id, shard_id)`, a 1000-shard cluster with 50 operators × 6 laws produces 300k time series for merge-law metrics alone. | Medium |

**IVM Engine Soundness:**
The DBSP foundation is solid. The key risk is the translation from in-memory
Z-set algebra (where Feldera operates) to a per-epoch LSM commit model where
"read the current state" requires traversing an SST hierarchy. The design
assumes arrangement reads are cheap; in practice, at 20GB per shard with
hundreds of SSTs, a point lookup for a join arrangement can cross 3-5 SST levels
plus bloom filter checks. The `segment_cache` helps for cross-shard reads, but
the local operator's own arrangement reads hit the same LSM.

### 2.2 IMPLEMENTATION_PLAN.md Analysis

**Strengths:**
- The phase-by-phase operability callouts are excellent discipline.
- Exit criteria are concrete and measurable.
- The oracle-driven testing strategy (batch equivalence) is the right
  correctness foundation.
- The IVM-0 (MergeLaw) + IVM-1 (filter/project/map) + IVM-2 (aggregate) +
  IVM-3 (MIN/MAX) decomposition is clean and testable.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| Phase 1 exit criteria | "1M row/s for filter on laptop" is achievable but "200k row/s for GROUP BY SUM" is measured against what? In-memory only? Including SlateDB merge commit? The benchmark target is ambiguous about the storage path exercised. | High |
| Phase 2 scheduling | `EXPLAIN INCREMENTAL ESTIMATE` (v0.17) requires source statistics that don't exist until connectors are built (v0.44). The implementation plan puts the cost model cart before the statistics horse. | Medium |
| Phase 3.5 | "≥10x speedup vs batch at 1% change rate" — this is a reasonable target but the change-rate distribution matters enormously. 1% random uniform change is very different from 1% hot-key change on a skewed distribution. The benchmark specification doesn't bound the distribution. | Medium |
| Phase 4 | "16-shard cluster (single host, 16 processes)" tests parallelism but NOT the network and object-store costs of distribution. Running 16 processes on one host with local filesystem avoids the defining challenge (S3 latency, network partition, cross-host shuffle). The real distributed proof should be a 4-host × 4-shard cluster minimum. | High |
| Phase 6 | "100k simulation seeds" sounds impressive but without stating the per-seed coverage (number of operations, depth of interleaving, fault injection probability), it's meaningless. 100k seeds at 10 operations each is trivial; 100k seeds at 10,000 operations each is substantial. | Medium |
| Phase 8-9 | Phase 8 (gateway) and Phase 9 (connectors) are massive scope expansions: pgwire + CRDT columns + DML + session semantics + auth + inline views + subscribe + historical queries + secondary indexes + shard stats — all in ~5 roadmap versions. Each of these is individually non-trivial. The scope estimate of "~10 person-weeks per version" seems aggressive for v0.40-v0.50. | High |

**Sequencing concerns:**
- Secondary indexes (v0.49) are specified as IVM-backed system views. This is
  architecturally elegant but means index build performance depends on the full
  IVM pipeline being production-quality — a Phase 10 feature depending on Phase
  1-3 infrastructure. If IVM performance is worse than expected, indexes become
  a liability.
- The Coordinator Group (Phase 13) is specified in extreme detail but gated
  behind v0.55. Designing the full protocol now when the prerequisites (frontier
  protocol, distributed checkpoint, exactly-once) don't exist yet is premature
  specification. The design may not survive contact with the implementation of
  its prerequisites.

### 2.3 ROADMAP.md Analysis

**Strengths:**
- The "evidence over dates" philosophy is correct for infrastructure.
- Decision gates at meaningful boundaries (v0.10, v0.18, v0.27, v0.36, v0.45,
  v0.52, v0.55) are well-placed.
- "Design freeze after v0.10" is the right discipline to prevent specification
  from becoming the primary work product.
- The "things to keep out until after 1.0" list shows restraint.

**Weaknesses and gaps:**

| Area | Issue | Severity |
|------|-------|----------|
| Feedback loop | No version before v0.45 ("Integration Beta") produces something an external user could evaluate. That's ~25 versions or ~250 person-weeks away. The project needs an earlier external signal — even v0.18 (SQL Alpha) should be demonstrable to a prospective customer. | High |
| Parallel tracks | The table says "Distributed runtime can start seriously after v0.27" but v0.28 immediately requires a control plane, worker registration, and shard leasing — infrastructure that benefits from early prototyping. Waiting until v0.27 to think about distribution risks a costly rearchitecture if the Phase 1-3 abstractions don't compose well with placement. | Medium |
| Resource estimation | "10 person-weeks per version" is presented as uniform but complexity varies enormously. v0.5 (Z-set types) is much simpler than v0.43 (direct-write CRDT surface + session semantics + optimistic transactions + view replacement). The uniform sizing creates false predictability. | Medium |
| Design freeze enforcement | The freeze says "every DESIGN.md commit should be small and targeted" but DESIGN.md is already at v3.28 with 28 major revisions. Unless the freeze is actively enforced with CI or process gates, the specification will continue to grow. | Medium |

---

## 3. Concrete Improvement Proposals

### 3.1 Address the Memory/State Management Gap

**Problem:** No mechanism for operator working-set management when arrangement
state exceeds available RAM.

**Proposal:** Add a §6.12 "Arrangement Working Set and Memory Pressure" section:

- Each operator maintains a hot-arrangement cache (LRU by group key or row
  key) bounded by `operator_cache_mb` (default: auto-derived from workload
  `MEMORY_LIMIT / operator_count`).
- When the cache is full, arrangement reads fall through to SlateDB
  `get()`/`scan()` — measurably slower but correct.
- Metric: `arrangement_cache_hit_ratio{op_id}` and
  `arrangement_cache_evictions_total{op_id}`.
- `EXPLAIN INCREMENTAL ESTIMATE` reports estimated working-set size vs
  available cache per operator.
- Operators whose estimated working set exceeds 2x available cache emit
  `RS-XXXX arrangement.working_set_exceeds_cache` as a NOTICE at deploy.

### 3.2 Introduce an Early "Demo-able" Milestone

**Problem:** No external feedback before v0.45 (~250 person-weeks away).

**Proposal:** Add a "Developer Preview" milestone at v0.18 (SQL Alpha):

- Package the single-shard engine with a `rockstream demo` command that
  spins up the engine with `GENERATE ROWS`, creates a materialized view,
  and shows live updates.
- Publish a blog post and invite early-adopter feedback.
- Measure interest before committing to the full distributed path.

### 3.3 Tighten SlateDB Operational Budget Validation

**Problem:** Operational budgets (§5.4) are stated but not validated against
real object storage until much later.

**Proposal:** Add a Phase 1.5 "Storage Stress" validation gate between v0.10
and v0.11:

- Run the existing aggregate benchmarks against S3 (not in-memory object store).
- Measure: p50/p95/p99 `WriteBatch` latency, compaction write amplification,
  `get_merged()` latency at 1GB/5GB/20GB shard sizes, WAL listing cost.
- Document concrete operational budgets as numbers, not aspirations.
- Gate: if `get_merged()` p99 > 50ms at 5GB shard size, the merge-operator
  strategy needs redesign before Phase 2 starts.

### 3.4 Simplify Phase 4 Distributed Proof

**Problem:** The Phase 4 exit criteria test distribution on a single host.

**Proposal:** Replace the "16-shard single-host" proof with:

- "4-host × 4-shard cluster on real object storage (S3/MinIO over network).
  Measure: shuffle p99 latency, epoch commit p99, frontier convergence time.
  Results must be within 2x of single-host baseline for partitionable queries."

This forces the implementation to confront real network and storage latency
from the first distributed milestone.

### 3.5 Bound Simulation Seed Coverage

**Problem:** "100k seeds" is specified without per-seed depth requirements.

**Proposal:** Define seed coverage budget explicitly:

- Each seed must execute at least 1,000 operations (source events, epoch
  commits, frontier advances, fault injections).
- At least 10% of seeds must inject at least one fault (worker kill, object-store
  stall, network partition).
- Coverage metric: `(operations_per_seed × seeds) / fault_permutations` must
  exceed a named threshold.

### 3.6 Reduce Metric Cardinality Risk

**Problem:** Per-law × per-op × per-shard metrics explode at scale.

**Proposal:** Add a §14.15.1 "Metric Cardinality Budget":

- Hot-path metrics (emitted every epoch): cap label cardinality at
  `{pipeline_id, op_type}` — no `op_id` or `shard_id` in the hot path.
- Diagnostic metrics (emitted on-demand via `debug arrangement`): may include
  full labels.
- Budget: total active time series per worker ≤ 10,000 under normal operation.

### 3.7 Add Write Amplification Budget to Storage Section

**Problem:** §5.4 discusses operational budgets but omits write amplification.

**Proposal:** Add to §5.4:

- `target_write_amplification` per shard (default: 10x, meaning each logical
  byte written results in ≤ 10 bytes written to object storage including
  compaction).
- Metric: `write_amplification_ratio{shard_id}`.
- `EXPLAIN INCREMENTAL ESTIMATE` reports predicted write amplification based
  on the operator mix (merge-heavy workloads have lower amp; delete-heavy
  workloads have higher amp from tombstones).
- Shard alert at `2x target_write_amplification`.

### 3.8 Address the Exchange Path Threshold Gap

**Problem:** No documented threshold for direct-vs-durable exchange.

**Proposal:** Add to §7.2:

- `exchange_durable_threshold_bytes` (default: 4MB) — batches above this
  size take the durable path regardless of receiver health.
- `exchange_direct_timeout_ms` (default: 500ms) — if the direct path doesn't
  ACK within this window, the batch is re-routed to durable.
- `exchange_durable_backoff_ms` (default: 5000ms) — after falling back to
  durable, don't retry direct for this window (hysteresis).

---

## 4. Markdown Diff / Remediation Recommendations

### 4.1 DESIGN.md Corrections

**§5.4 — Add write amplification budget** (after the "Arrangement segment cache"
paragraph):

```markdown
- **Write amplification is bounded.** Each shard tracks its logical-to-physical
  write ratio (`write_amplification_ratio`). The default budget is
  `target_write_amplification = 10` (10 physical bytes per logical byte
  including compaction rewrites). Shards exceeding `2 × target` trigger an
  operator NOTICE (`RS-5020 storage.write_amplification_high`) and a
  compaction tuning adjustment (larger SST target size to reduce level count).
  `EXPLAIN INCREMENTAL ESTIMATE` reports predicted write amplification per
  operator based on its arrangement type (merge-append-heavy workloads predict
  lower amp; delete-heavy workloads predict higher amp from tombstone rewrites).
```

**§6 — Add §6.12 working-set management** (after §6.11):

```markdown
### 6.12 Arrangement Working Set and Memory Pressure

Operators access their arrangements via `ShardDb`. For hot-path performance,
each operator maintains a bounded in-process arrangement cache (LRU by key)
configurable as `operator_cache_mb`. Cache misses fall through to SlateDB
`get()` / `scan()` — correct but slower by the object-store round-trip
latency amortized by the segment cache (§5.4).

| Signal | Action |
|---|---|
| `cache_hit_ratio < 0.5` for 5 min | Emit `RS-5021 arrangement.cache_thrashing` NOTICE |
| Working set estimate > 2× available cache | Emit NOTICE at `CREATE MATERIALIZED VIEW` |
| Worker RSS > `worker_memory_limit × 0.9` | Evict coldest arrangement caches first |

The auto-tuner redistributes cache budget across operators on the same worker
proportional to observed access frequency. The working-set estimate is derived
from the arrangement's key cardinality × average value size, reported by the
storage layer at each checkpoint.
```

**§7.2 — Add exchange threshold specification** (after "Either way, the
canonical record is in object storage"):

```markdown
**Path selection thresholds.** The exchange dispatcher selects the path per
batch using concrete thresholds:

| Parameter | Default | Effect |
|---|---|---|
| `exchange_durable_threshold_bytes` | 4 MB | Batches above this size always take the durable path. |
| `exchange_direct_timeout_ms` | 500 ms | If direct ACK doesn't arrive within this window, re-route to durable. |
| `exchange_durable_cooldown_ms` | 5000 ms | After a durable fallback, stay on durable for this window before retrying direct (hysteresis). |
| `exchange_loopback_threshold_bytes` | 64 KB | Same-worker batches above this size use bounded channel + durable metadata rather than direct memory pass. |
```

### 4.2 IMPLEMENTATION_PLAN.md Corrections

**Phase 1 exit criteria — Clarify benchmark storage path:**

Replace:
```
- 1M-row/s throughput for filter on a laptop (single-threaded).
- 200k-row/s for `GROUP BY SUM`; 100k-row/s for `GROUP BY MIN`.
```

With:
```
- 1M-row/s throughput for filter on a laptop (single-threaded, in-memory
  object store). 500k-row/s against local filesystem SlateDB.
- 200k-row/s for `GROUP BY SUM` (in-memory); 100k-row/s against local
  filesystem. Benchmark must report both paths.
- 100k-row/s for `GROUP BY MIN` (in-memory); 50k-row/s against local
  filesystem. Benchmark must report both paths.
```

**Phase 4 exit criteria — Require real network:**

Replace:
```
- 16-shard cluster (single host, 16 processes) runs TPC-H ...
```

With:
```
- 16-shard cluster (minimum 4 hosts or containers with real network between
  them, MinIO as object storage) runs TPC-H with documented throughput.
  Additionally, single-host 16-process test for baseline comparison.
  Cross-host shuffle p99 latency, epoch commit p99, and frontier convergence
  time are documented.
```

**Phase 6 simulation — Specify seed depth:**

Add after "≥ 100k seeded `SimRuntime` runs across the coordination suite pass
cleanly":

```
  Each seed executes at least 1,000 operations (epoch commits, frontier
  advances, shuffle batches, checkpoint barriers). At least 10% of seeds
  inject at least one fault from the explicit fault model. Coverage is
  reported as `total_operations / known_interleaving_classes`.
```

### 4.3 ROADMAP.md Corrections

**Add Developer Preview milestone at v0.18:**

In the "Public Milestones" table, add:

```
| Developer Preview | v0.18 | Single-shard SQL engine demo-able to external users. Blog post + feedback loop. |
```

**Add storage validation gate:**

In the "Decision Gates" table, add after "IVM kernel confidence":

```
| Storage operational budget | v0.10 | Do SlateDB operational budgets (write amp, get_merged p99, compaction debt) hold at 5GB+ shard sizes on real object storage? |
```

**Refine version sizing caveat:**

After "The version number is not a promise of public release quality", add:

```
Version effort varies. Foundation versions (v0.1–v0.4) are typically under-budget;
gateway and connector versions (v0.40–v0.50) are typically over-budget due to
integration surface area. Teams should allocate 1.5× for integration-heavy
versions and 0.7× for kernel-focused versions.
```

---

## 5. Cross-Document Alignment Findings

| # | Finding | Documents | Severity |
|---|---------|-----------|----------|
| 1 | ROADMAP.md "Design freeze after v0.10" contradicts DESIGN.md being at v3.28 with extensive post-v0.10 additions still being merged. The freeze must be actively enforced now. | ROADMAP × DESIGN | High |
| 2 | IMPLEMENTATION_PLAN Phase 8 maps to ROADMAP v0.40–v0.43 (4 versions × 10pw = 40pw), but the scope includes pgwire, CRDT types, DML, sessions, auth, subscribe, historical queries, view replacement, session semantics, and write fencing — each individually 10+ pw. Underestimated by ~3x. | IMPL × ROADMAP | High |
| 3 | DESIGN.md §12.6.1 specifies `rockstream_catalog` as the canonical system schema, but IMPLEMENTATION_PLAN v0.41 still references `rockstream.epochs`, `rockstream.pipelines` etc. The alias deprecation path exists but implementation references are inconsistent. | DESIGN × IMPL | Low |
| 4 | ROADMAP v0.50 includes "rolling upgrade test, migration skeleton, disaster recovery drill, independent security review" AND shard column statistics AND secondary index stat injection. This is not one version — it's at least two. | IMPL × ROADMAP | Medium |
| 5 | DESIGN.md §3.1 describes `embedded` mode eliding distributed boundaries, but IMPLEMENTATION_PLAN Phase 1's embedded benchmark only validates "zero gRPC shuffle calls" — it doesn't validate that the `embedded` profile actually achieves the `local_visible` sub-ms latency class promised in §3.0. | DESIGN × IMPL | Medium |

---

## 6. Scaling Spectrum Assessment

### Single-Process Ergonomics (Grade: B+)

The `rockstream start --role=all --storage=./data` story is well-specified and
the v0.10 implementation validates it works. The embedded runtime profile
correctly elides distributed overhead. However:

- The developer still needs to understand MergeLaw concepts, epoch semantics,
  and frontier vocabulary to debug basic issues. The abstraction doesn't fully
  hide distributed internals in local mode.
- The `GENERATE ROWS` source is excellent for zero-dependency onboarding.
- Error messages with `RS-XXXX` codes and `next_steps` fields (specified for
  Phase 10) should ship earlier — the developer experience at v0.10 likely
  shows raw Rust error types.

### Cloud-Native Distributed Scale (Grade: B-)

The architecture is sound in principle — compute/storage separation via SlateDB,
sharding for write parallelism, frontier protocol for coordination-free progress.
However:

- **State is not truly decoupled from compute.** Each worker opens SlateDB as
  the sole writer; "stateless workers" is aspirational since the writer fence
  creates strong affinity between a worker and its shards. Recovery requires
  WAL replay on the new owner — not instant failover.
- **The object-store request budget is the real scaling bottleneck.** Each
  epoch commit is a WAL write + potential manifest update. At 100ms epochs ×
  1000 shards = 10,000 object-store writes/second sustained. S3's per-prefix
  rate limit (5,500 writes/s) could be hit without careful prefix distribution.
- **Auto-scaling signals are push (metric export) not pull (API call).** This
  is correct for k8s HPA but means RockStream cannot proactively signal "I need
  more capacity NOW" — it waits for the metric to propagate through Prometheus →
  HPA → kubelet → pod-ready, which is 30-60 seconds minimum.

---

## 7. Final Recommendation

The project is architecturally sound and exceptionally well-specified. The
primary risk is not the architecture — it's the distance between specification
and implementation. The team should:

1. **Enforce the design freeze immediately.** No new DESIGN.md sections until
   the implementation catches up. Track gaps as GitHub issues, not document
   revisions.
2. **Validate SlateDB at scale before Phase 2 starts.** Run the aggregate
   benchmark against MinIO/S3 at realistic shard sizes. Discover the
   performance cliff now, not at v0.28.
3. **Create a "Demo-able Preview" at v0.18** that external users can try.
   The feedback will be more valuable than 6 more months of specification.
4. **Split Phase 8 into at least 3 sub-phases** with independent deliverables:
   v0.40 (read gateway only), v0.41-v0.42 (introspection + subscribe), v0.43
   (direct write — this alone is a full phase).
5. **Add the arrangement working-set management design** before Phase 2
   introduces joins (which create unbounded bilateral state).

The IVM kernel confidence gate is well-passed. The next critical gate is
"Storage Operational Budget" — proving that SlateDB's performance
characteristics support the design's assumptions at realistic scale.

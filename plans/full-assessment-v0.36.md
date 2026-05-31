# Post-Phase Architectural Assessment: v0.36.0

**Date**: 2026-05-31
**Phase**: Distributed Alpha (v0.36 boundary)
**Assessed by**: Principal Systems Architect

---

## 1. Executive Summary & Core Weaknesses

The v0.36.0 release delivers meaningful distributed infrastructure: 2PC sink protocol, chaos simulation alpha, law-equivalence-under-fault corpus, wire-protocol version negotiation, and object-store brownout handling. These are genuine engineering advances. However, the architecture now carries five critical vulnerabilities that will compound as the project moves toward Integration Beta (v0.37–v0.45). Each finding is grounded in the spec text and code.

### Critical Vulnerability 1 — 2PC Commit Window Is Not Atomically Sealed

DESIGN.md §11.4 describes a `pre_commit(epoch, rows)` → checkpoint → `commit(epoch, checkpoint_id)` protocol. The spec states that if a worker crashes after writing `pre_commit` but before writing `commit`, recovery re-runs the commit path because "external commit operations MUST be idempotent." This assertion is false in the general case. Kafka producer transactions can only be re-committed on an already-committed transaction if the transactional producer is alive and the transaction is still open; a crash causes the transaction to be aborted by the broker after `transaction.timeout.ms`. S3 multipart uploads that completed but whose completion record was not written to `sink_state/committed` before the crash leave an orphaned multipart object — S3 does not re-expose it for idempotent completion. The spec does not model these sink-specific crash semantics. The `TwoPcSinkState` enum in `rockstream-sim/src/two_pc.rs` is simulation infrastructure, not a proof of correctness. Until §11.4 enumerates the commit-idempotency contract per sink type and provides per-type recovery logic, the exactly-once guarantee is a claim, not a proof.

### Critical Vulnerability 2 — Plan Drift Creates Contributor Confusion and Milestone Risk

IMPLEMENTATION_PLAN.md's Phase overview table (around line 40) shows Phases 4 through 12 as status "Not started." ROADMAP.md's version table shows v0.28 (Phase 4), v0.30 (Phase 5), v0.32 (Phase 6), v0.34 (Phase 7), and v0.36 (Phase 8 alpha) all marked ✅ Done. These two documents are irreconcilably contradictory. A contributor reading IMPLEMENTATION_PLAN.md to understand current project state will conclude Phases 4–12 are untouched. A contributor reading ROADMAP.md will conclude 8 phases are complete. The Phase 4 exit criteria in IMPLEMENTATION_PLAN.md require "16-shard cluster on ≥ 4 hosts with real network latency injection" and sign-off by principal architect — there is no evidence this criterion was signed off, no `plans/phase4-signoff.md` file exists, and the v0.28 release description in ROADMAP.md does not mention the 4-host test. This ambiguity poisons release readiness claims for all subsequent phases.

### Critical Vulnerability 3 — v0.43 Scope Is Four to Six Releases Compressed Into One

ROADMAP.md's v0.43 row ("Integration Beta: DML + CRDT columns") lists the following deliverables in a single version: DML over pgwire (`INSERT`, `UPDATE`, `DELETE`, `INSERT…RETURNING`), five CRDT column types (`GCounter`, `PNCounter`, `LWWValue`, `ORSet`, `MVRegister`), session-scoped automatic read-your-writes, idempotency enforcement, optimistic transaction metadata hooks, session max-staleness, zero-downtime view replacement, write-fence tokens, background DDL with `WAIT`/`NO WAIT`, and namespace lifecycle commands (`CREATE/DROP NAMESPACE`). DESIGN.md §13.5 covers DML alone across five subsections (§13.5.1 optimistic transactions, §13.5.2 idempotency, §13.5.3 session isolation, §13.5.4 fence tokens, §13.5.5 background DDL). DESIGN.md §6.11 covers the MergeLaw CRDT contract across four pages. These are architecturally independent subsystems. Combining them into a single version with no sub-task breakdown or time budget makes v0.43 unshippable as scoped.

### Critical Vulnerability 4 — AlignmentBuffer Stores Raw Bytes Without Schema Version Metadata

`crates/rockstream-runtime/src/checkpoint.rs` defines `AlignmentBuffer` as `Vec<(Vec<u8>, Vec<u8>)>` — raw key-value byte pairs. The Chandy-Lamport barrier alignment (DESIGN.md §11.2) requires buffering rows from the fast input while the slow input catches up. If a view schema change (`ALTER VIEW`) propagates through the control plane between the injection of the barrier and the draining of the alignment buffer, the buffered bytes were serialized under the old schema version but will be deserialized under the new schema at drain time. DESIGN.md §5.5 specifies Storage Format Versioning but applies it to durable epoch-commit WriteBatches, not to in-flight alignment buffers. The struct stores no schema epoch, no column layout tag, and no magic bytes. A concurrent schema change crossing a checkpoint boundary will produce silent column-type mismatches or panics at deserialization.

### Critical Vulnerability 5 — Hot-Key Bucket Salting Breaks DISTINCT Aggregates

DESIGN.md §10.5 describes the hot-key salting strategy: a key `k` is spread across `salt ∈ [0, B)` virtual buckets as `(k, salt)`, aggregated per bucket, then combined with an unsalted `(k, ∅)` combiner shard. The spec states "for algebraic aggregates this is exact partial aggregation." This is correct for sum-compatible monoids like `CountAdd/v1`, `SumAdd/v1`, and `MaxValue/v1`. It is incorrect for `WeightAdd/v1` (DISTINCT detection). `WeightAdd/v1` fires an output only when the total weight for a key crosses zero in either direction: from 0 to +n emits +1; from +n to 0 emits -1 (DESIGN.md §6.10). With bucket salting, each per-bucket shard sees a partial weight independently. A key with total weight +2 split as (+1, bucket-0) and (+1, bucket-1) correctly produces no output per bucket and no output at the combiner. But a key with total weight 0 expressed as (+1, bucket-0) and (-1, bucket-1) produces a +1 output from bucket-0 and a -1 output from bucket-1 before the combiner merges them — meaning two spurious delta events escape the salting tier and enter the combiner. The combiner itself will cancel them, but only if the combiner receives both before its own epoch commit. Out-of-order epoch delivery (§7.1 shuffle reordering) can allow one delta to commit to a downstream subscriber before the cancellation arrives. DESIGN.md §10.5 does not acknowledge this and provides no sequencing guarantee for the combiner's epoch-commit relative to the bucket shards.

---

## 2. In-Depth Architectural Critique

### 2.1 DESIGN.md Analysis

#### 2.1.1 IVM Engine Soundness

**Z-Set Transformer Completeness (§2, §6)**

DESIGN.md §6 defines the full operator catalog: `Map`, `Filter`, `FlatMap`, `Distinct` (`WeightAdd/v1`), `Aggregate`, `Join`, `Antijoin`, `Outer Join`, `Union`, `Except`, `Window`, `TopK`. The DBSP formalism guarantees that every linear operator applied to a Z-set stream produces a Z-set stream, and that `Distinct` is the only non-linear operator. This is correct per Budiu et al. 2023. However, DESIGN.md does not prove or cite a proof that the `Window` operator as specified in §6.9 is correctly incrementalized. Windowed aggregation over event-time windows requires tracking which input rows belong to which window, which requires state proportional to the window width times the input cardinality. The spec says "windows maintain a per-key ring buffer of depth `window_size_epochs`" — this is a correct approach, but it is not a Z-set transformer in the strict DBSP sense. It is a stateful transducer. The spec never addresses this explicitly, leaving open the question of whether late arrivals (rows arriving after a window's epoch has been committed) are handled by retraction. If a row arrives three epochs late and falls inside a committed window, the correct DBSP response is to emit a retraction delta for the old window result and a positive delta for the new one. DESIGN.md §6.9 does not describe this retraction path. The word "retraction" does not appear in §6.9. This is an IVM soundness gap for time-windowed aggregates.

**Arrangement Compaction and Stale Read Risk (§5.4, §8)**

DESIGN.md §5.4 specifies SlateDB operational budgets: L0 file limit 10, compaction interval target 30s, arrangement read SLA 20ms at p99. The arrangement is a persistent Z-set indexed by (key, weight) and used by Join, Antijoin, and Index operators to look up the current state of their non-streaming side. §8.4 states that arrangements are versioned at the epoch granularity and that a join operator reads from the arrangement at the epoch's frontier. But §5.4 also notes that SlateDB compaction is background and asynchronous — the arrangement's L0 files may not be compacted when a Join operator accesses them at epoch N. An arrangement access during a compaction pause will see multiple overlapping L0 SSTs and must merge them in the read path. Under high-write-rate conditions (§5.4: "100 MB/s write rate per shard at 1 GB shard size"), L0 accumulation can cause arrangement read latency to spike from 20ms to 200ms+. This breaks the `local_durable` latency class SLA (§3.0: ≤100ms). DESIGN.md §5.4 does not define a backpressure trigger for high L0 count — there is no "stall writes if L0_count > threshold" mechanism specified. SlateDB's own implementation may provide this, but it is not part of the RockStream operational contract.

**CALM Epoch-Commit Verifiability (§8.4)**

DESIGN.md §8.4 states the CALM Epoch-Commit Invariant: "the committed state at epoch N is verifiable by any observer with read-only object-store access." This is an important property for auditability and cold-tier consumers. The implementation requires that every epoch commit writes a manifest entry to `epochs/N/manifest.json` with the list of shard-level SlateDB WriteBatch keys. §8.4 is correct in principle, but it does not specify what happens to the manifest if the coordinator crashes after writing some shard manifests but before writing the cluster-level rollup. A partial manifest at epoch N means an observer cannot distinguish "N was not committed" from "N was committed but the manifest is incomplete." The spec provides no tombstone or two-phase manifest write protocol. This is a minor but real correctness gap in the CALM invariant as described.

**Frontier Aggregator Leader Crash (§3.2)**

The lease-based frontier aggregator leader election (§3.2) specifies that a new leader re-reads all shard frontiers from control SlateDB. But DESIGN.md §3.2 does not specify the atomicity window: a departing leader may write a frontier update to control SlateDB and then crash. If the successor leader reads control SlateDB before the write flushes (SlateDB has configurable `wal_flush_interval`), the successor will compute a frontier from a stale shard summary. This produces a `visible_frontier` that is lagging by up to `wal_flush_interval`. DESIGN.md §3.2 does not specify whether control SlateDB is configured with `wal_flush_interval = 0` (synchronous) or whether frontier writes use `sync: true` WriteBatch semantics. If not synchronous, frontier skew of up to one WAL flush interval (default 10ms) can occur at every leader failover.

#### 2.1.2 Scaling: Laptop to Kubernetes

**Runtime Profile Transitions Are One-Way with No Migration Path (§3.1.1)**

DESIGN.md §3.1.1 states explicitly: "Profile transitions are one-way: `embedded` → `single_worker` → `distributed`. There is no downgrade path." This is pragmatic for correctness (avoiding half-distributed states), but it creates an operational cliff. A developer who starts with `embedded` for local testing, promotes to `single_worker` for a staging environment, and then promotes to `distributed` for production cannot roll back to `single_worker` if a distributed deployment fails. The spec provides no guidance on what an operator should do if the distributed deployment is broken (control plane unreachable, K8s scheduling failure) and they need to serve traffic from a single worker. The `worker_self_fence` mechanism (§11.6) will terminate the single worker when it cannot reach control plane for 30s. The net result: a failed distributed promotion leaves the system in a state where no runtime profile can serve traffic. This is a deployment liveness gap with no documented recovery procedure.

**Thundering Herd Mitigation Is Insufficient for Large Clusters (§11.7)**

DESIGN.md §11.7 describes the thundering herd mitigation: startup delay of `worker_id mod jitter_buckets` × 1s, plus `max_lease_grants_per_second` rate limiting at the control plane. For a 100-worker cluster, `jitter_buckets = 32` means workers 0–31 each get a unique bucket (0–31s delay) but workers 32–63 share buckets with workers 0–31. Two workers sharing bucket 3 both start at exactly 3s and simultaneously request leases. With `max_lease_grants_per_second = 10`, a 100-worker cluster takes at least 10 seconds to fully start up under ideal conditions. But the object-store brownout handler (§11.8, `local_buffer_max_epochs = 10`) starts buffering immediately at worker startup — if the worker starts but cannot access object storage for 10s, it has consumed its entire brownout budget before any epoch has been committed. DESIGN.md §11.7 and §11.8 are not co-designed to account for this startup interaction.

**Virtual Bucket Count Is Fixed at Configuration Time (§10.1)**

DESIGN.md §10.1 sets `B = 16 × max_expected_shards` with a default of 4096. This is a deploy-time constant. The spec says "B cannot be changed without a full resharding." For a cluster that was initially sized at `max_expected_shards = 256` (B = 4096) and later grows to 512 shards, the rendezvous hashing maps 4096 buckets to 512 shards — 8 buckets per shard. This is still fine. But if the cluster needs to grow beyond 4096 shards (e.g., for a multi-tenant cloud deployment with 10,000 customer shards), B = 4096 means multiple shards must share a bucket, which breaks the key-isolation guarantee that buckets provide. DESIGN.md §10.1 does not acknowledge this upper bound or provide an online B-expansion protocol. A future cloud tenant deployment that exceeds the original `max_expected_shards` estimate will require a full offline resharding event, which §10 explicitly states takes O(shard_count) time.

**Distributed-Mode Latency Budget Is Not Validated End-to-End (§3.0)**

DESIGN.md §3.0 defines five latency classes. `distributed_fresh` is budgeted at ≤5s (sources → view output, 90th percentile). This budget covers: source ingest → worker epoch processing (§9) → frontier aggregation (§3.2) → shuffle (§7) → downstream epoch → gateway read. The spec gives individual component budgets: epoch processing ≤500ms (§9.1), shuffle network ≤200ms (§7.1), frontier aggregation ≤100ms (§3.2), gateway read ≤500ms (§12.1). These sum to ≤1.3s in the happy path. But the spec does not account for: SlateDB L0 compaction stalls (potentially 200ms+, unbounded under spike writes), shuffle retry on network partition (§7.2: up to 3 retries × 200ms = 600ms), control-plane frontier publication round-trip (§3.2: one additional SlateDB read/write pair). The true p90 budget under realistic conditions is likely 2–3s, not 5s — but this has never been measured in a real distributed deployment. DESIGN.md §17.9 (Simulation Test Coverage) lists latency class SLA tests as "planned" rather than "implemented." There is no production latency validation suite.

#### 2.1.3 Cloud-Native Topology

**Iceberg REST Catalog Endpoint Lacks Authentication Specification (§12.4)**

DESIGN.md §12.4 specifies that the gateway serves an Iceberg REST catalog at `/iceberg/v1/` on port 8181. The Iceberg REST catalog spec (Apache Iceberg open spec) requires OAuth2 bearer token authentication for catalog operations. DESIGN.md §12.4 does not specify: whether the RockStream Iceberg endpoint requires authentication, what OAuth2 scopes are needed, whether the same API key used for the pgwire gateway (§14.11) is valid for Iceberg REST, or how catalog namespaces map to RockStream namespaces. DESIGN.md §14.11 covers API key management in detail but never cross-references §12.4. An Iceberg consumer (e.g., Spark, DuckDB, Trino) connecting to the RockStream Iceberg endpoint will encounter undefined authentication behavior.

**Two-Tier View Storage Merge Is Undefined for Negative Weights in Cold Parquet (§12.2)**

DESIGN.md §12.2 specifies that the gateway merges hot LSM deltas with cold Parquet snapshots using "versioned signed Z-set merge." Parquet does not natively support negative-weight rows (retractions). The spec says "negative-weight rows are stored in Parquet with a `__weight` column." The merge procedure must read all cold Parquet files, filter for `__weight < 0`, and cancel them against positive-weight rows. For large cold tiers (multi-GB Parquet), this is an O(cold_tier_size) scan on every query for views with frequent retractions. §12.2 does not specify a compaction policy that eliminates cancelled pairs from the cold tier. Without compaction, the cold tier grows monotonically even for stable views (where inserts and deletes balance), causing query latency to increase without bound over time.

**Direct-Write Internal Source Lacks Back-Pressure to pgwire Client (§13.5.1)**

DESIGN.md §13.5.1 describes the Direct-Write Internal Source Connector: DML over pgwire routes through an optimistic transaction path that uses SlateDB's compare-and-swap to detect write conflicts. On conflict, the transaction is aborted and the pgwire client receives a serialization error (SQLSTATE 40001). The spec says "clients SHOULD retry with exponential backoff." Under high write contention (multiple writers to the same key), the retry storm from multiple clients with exponential backoff is not bounded by any rate limiter or admission control mechanism. DESIGN.md §13.5.1 does not specify: a maximum retry count, a server-side write throttle, or a queue depth limit for optimistic transaction attempts. This is a denial-of-service vector if a high-contention workload is submitted via pgwire DML.

### 2.2 IMPLEMENTATION_PLAN.md Analysis

#### 2.2.1 Phase Sequencing and Bottlenecks

**Phase 4 Exit Criteria Were Never Formally Satisfied**

IMPLEMENTATION_PLAN.md Phase 4 exit criteria (around lines 380–420) require:
1. 16-shard cluster running on ≥ 4 physical hosts
2. Real network latency injection (tc-netem or equivalent)
3. Shuffle protocol end-to-end latency measured at p50/p90/p99
4. Principal architect sign-off

ROADMAP.md marks v0.28 (Phase 4 deliverable) as ✅ Done. But no `plans/phase4-signoff.md` exists, no `plans/phase4-benchmark.md` exists, and the v0.28 release description in ROADMAP.md ("Control plane and worker discovery") does not mention the 4-host test. The IMPLEMENTATION_PLAN.md Phase 4 column still shows "Not started." This is not a documentation oversight — it is a structural risk. If Phase 4 was shipped without the 4-host network test, then the distributed shuffle (§7), worker discovery (§11.1), and lease management (§3.2) have never been validated under real network conditions. Every subsequent phase (v0.30–v0.36) builds on this foundation.

**Phase 5 Storage Real-S3 Validation Gate Is Unresolved**

IMPLEMENTATION_PLAN.md Phase 5 notes (around lines 480–520): "Real S3 validation at 1GB+ shard sizes is required before v0.30 ships." This was flagged in the prior assessment (`plans/full-assessment-v0.18.md`). ROADMAP.md marks v0.30 as ✅ Done ("Single-shard compaction, GC, cold-tier snapshots"). The v0.30 release description does not mention real-S3 validation. The `crates/rockstream-runtime/src/checkpoint.rs` implementation was written in v0.34 (checkpointing) and v0.36 (cluster checkpoints). If checkpoint correctness was never validated against real S3 (with its eventual-consistency behavior for LIST operations and its MPU race conditions), then the v0.36 exactly-once guarantee is untested against the actual storage substrate. SimObjectStore (simulation) does not model S3 LIST consistency delays or MPU lifecycle rules.

**Phase 6 Join State Size Estimation Is Missing from the Plan**

IMPLEMENTATION_PLAN.md Phase 6 covers the join and arrangement subsystem. The plan specifies implementing `HashJoin`, `SortMergeJoin` (for range predicates), and `IndexedLookup`. But IMPLEMENTATION_PLAN.md Phase 6 does not include a step for join state size estimation or bloom-filter pre-filtering. DESIGN.md §6.6 specifies that joins maintain arrangements on both sides. For a join between a large slowly-changing table (10M rows) and a high-rate stream (10K rows/epoch), the arrangement for the large side requires 10M Z-set entries in SlateDB. Phase 6 makes no provision for arrangement size budgets, arrangement pruning (for left-outer joins where the right side is empty), or bloom-filter optimization to skip arrangement lookups for non-matching keys. These are not nice-to-haves — they determine whether joins are feasible at production scale.

**Phase 7 WAL and Recovery Timeline Depends on Phase 4 Network Validation**

IMPLEMENTATION_PLAN.md Phase 7 (recovery, Phase ~v0.34) delivers WAL-based recovery, crash-safe epoch commit, and ControlPlaneFence. The `crates/rockstream-runtime/src/recovery.rs` implementation is complete and reviewed. However, Phase 7 recovery assumes that shard-level WAL entries are durably written to SlateDB before the epoch-commit write batch is issued. DESIGN.md §9.1 specifies this order. But if Phase 4's network validation was skipped, the interaction between WAL write latency (object-store round trip) and epoch-commit deadline (controlled by `epoch_duration_ms`) has never been measured under real network conditions. Phase 7 sets `epoch_duration_ms = 1000` as the default. If object-store round-trip latency is 150ms (realistic for S3 cross-region) and there are 3 WAL writes per epoch (operator state, WAL segment, frontier summary), the WAL path alone consumes 450ms of the 1000ms epoch budget. The remaining 550ms must cover SQL processing and shuffle. This is tight and unvalidated.

**Phase 8 (v0.36) Kafka/S3/Postgres Sink Stubs Are Not Production Implementations**

IMPLEMENTATION_PLAN.md Phase 8 and ROADMAP.md v0.36 both reference "Kafka/S3/Postgres sink stubs." The word "stub" is critical. A stub implements the 2PC interface (`pre_commit`, `commit`, `abort`) with mock semantics. It does not implement the actual Kafka producer transaction protocol, the actual S3 MPU lifecycle, or the actual Postgres two-phase commit (`PREPARE TRANSACTION` / `COMMIT PREPARED`). DESIGN.md §11.4 describes the full 2PC protocol. But if v0.36's exactly-once guarantee is tested only against stubs, the guarantee has not been validated against the real protocols. This is the most important gap in v0.36's release claim: "Exactly-once" is only proven against a simulated sink, not a real one.

#### 2.2.2 Edge Cases and Missing Specifications

**RecoveryDriver Timestamp Collision Bug**

`crates/rockstream-runtime/src/recovery.rs` implements `RecoveryDriver::mark_complete(started_at_ms: u64)`. The `active_recoveries: Vec<ActiveRecovery>` is searched by `started_at_ms` to find the completed recovery record. If two recoveries are triggered in the same millisecond (plausible in simulation or on fast hardware), both will match `started_at_ms`, and `mark_complete` will complete the wrong one (whichever is first in the Vec). The correct key should be a unique recovery ID (UUID or monotonic counter), not a timestamp. This is a latent bug in the recovery bookkeeping that will manifest as "recovery B completed but recovery A is still active" false-positive state under concurrent recovery.

**RS Error Code Range Misplacements**

DESIGN.md §14.14 defines error code ranges:
- RS-1000–1499: connector/source/sink errors
- RS-3000–3499: shard/runtime/placement errors

`crates/rockstream-runtime/src/checkpoint.rs` uses RS-1601 ("alignment buffer overflow") and RS-1602 ("checkpoint injection conflict"). `crates/rockstream-runtime/src/recovery.rs` uses RS-1603 ("RECOVERING_SLOW"). All three are runtime/shard errors and should be in the RS-3xxx range. Using RS-1xxx codes for runtime errors means monitoring systems that categorize by error range (as recommended in §14.14) will misclassify checkpoint and recovery failures as connector failures. This is a silent operational observability bug.

**AlignmentBuffer Row Count Limit Is Not Epoch-Count-Based**

`crates/rockstream-runtime/src/checkpoint.rs` defines `AlignmentBuffer { max_rows: usize }`. The limit is a row count. But the correct metric for checkpoint alignment risk is the number of epochs buffered, not the number of rows. A checkpoint barrier injection with 1 million rows over 2 epochs is acceptable; with 1 row over 100 epochs it signals a stalled slow input. The current `max_rows` limit will trigger RS-1601 (overflow) on legitimate high-throughput inputs while allowing indefinitely stalled slow inputs (if they produce few rows). DESIGN.md §11.2 says "alignment buffer bounded by max_alignment_epochs" — the spec specifies epoch-based bounding, but the code implements row-based bounding. This is a spec-code divergence.

#### 2.2.3 Integration Beta Readiness (v0.37–v0.45)

The Integration Beta gate is at v0.45 (IMPLEMENTATION_PLAN.md Phase 9 exit). Reaching it requires completing v0.37–v0.44. Based on current state:

**v0.37 (Observability Alpha)**: DESIGN.md §14 (Metrics, Observability) is detailed and well-specified. The metrics catalog (§14.1–§14.13) covers epoch latency histograms, shuffle queue depth, arrangement size gauges, frontier lag counters, and 2PC phase durations. The implementation plan for v0.37 includes Prometheus endpoint, OpenTelemetry trace export, and structured log enrichment. This is the most achievable near-term milestone and the one with clearest spec backing.

**v0.38 (Cold Tier + Iceberg)**: DESIGN.md §12.3 covers cold-tier Parquet snapshot generation. The cold-tier compaction policy (§12.3.2) is underspecified — it says "compact when cold_tier_file_count > compact_threshold" but does not specify what happens if compaction falls behind (cold tier grows, query latency increases). Iceberg REST catalog authentication is unspecified (see §2.1.3). Risk: medium-high.

**v0.39–v0.42 (Connectors)**: DESIGN.md §13 covers the connector contract. Kafka source (§13.1), S3 source (§13.2), Postgres CDC source (§13.3) are specified but not yet implemented (only stubs in v0.36). The Postgres CDC connector requires: replication slot management, logical replication decoding (`pgoutput` protocol), schema change event handling, and transaction boundary detection. This is a substantial engineering effort that is underrepresented in IMPLEMENTATION_PLAN.md Phase 9 scope.

**v0.43 (DML + CRDTs)**: As noted in Critical Vulnerability 3, this version is over-scoped by 4–6x.

**v0.44 (Secondary Indexes)**: DESIGN.md §12.5 specifies secondary indexes as maintained arrangements with point-lookup optimization. Phase 9 includes secondary index implementation. IMPLEMENTATION_PLAN.md does not specify whether secondary indexes are fully consistent (updated in the same epoch commit as the primary view) or eventually consistent (updated asynchronously). DESIGN.md §12.5 says "indexes are updated atomically in the same WriteBatch as view_output" — this is the correct answer but it implies that every epoch commit must write to N+1 SlateDB paths (primary + N indexes). For a view with 5 secondary indexes, this multiplies WriteBatch size by 6×, which may exceed SlateDB's batch size limits.

### 2.3 ROADMAP.md Analysis

#### 2.3.1 Open Decision Gates with No Outcomes

ROADMAP.md contains two critical open decision gates for v0.36 with no outcomes filled in:

**Gate 1: "Distributed Architecture"**
The gate text reads: "Decide: single-region vs. multi-region topology for Integration Beta." This decision determines: whether the shuffle protocol (§7) needs cross-region latency budgets, whether the frontier aggregation (§3.2) needs multi-region consensus, whether the control plane (SlateDB) needs cross-region replication, and whether the latency class `distributed_fresh` (≤5s) is achievable cross-region. Without this decision, IMPLEMENTATION_PLAN.md Phases 9–12 cannot correctly scope their work. The v0.36 release does not document this decision's outcome.

**Gate 2: "CRDT Value"**
The gate text reads: "Decide: include CRDT column types in Integration Beta or defer to v1.0." If CRDTs are deferred, v0.43 drops the MergeLaw CRDT column types, halving its scope and making it shippable. If CRDTs are included, the full §6.11 MergeLaw contract must be implemented, tested, and documented before v0.45. This decision has cascade effects on v0.37–v0.44 planning. ROADMAP.md v0.43 currently assumes CRDTs are included (they are listed as a primary deliverable). The gate has not been formally resolved.

#### 2.3.2 Long-Term High-Scale Alignment

**Multi-Region Is Absent from the Long-Term Plan**

ROADMAP.md's vision section (around lines 350–401) describes "cloud-native, horizontally-scalable IVM at petabyte scale." But the entire ROADMAP.md covers only single-region topology. DESIGN.md §3.1 defines three runtime profiles (`embedded`, `single_worker`, `distributed`) — none of them is `multi_region`. The frontier aggregation protocol (§3.2) uses a single SlateDB instance as the frontier store; this instance is a single-region resource. A multi-region deployment would require either: (a) a global coordinator with cross-region write latency in every epoch commit path, or (b) region-local frontiers with eventual cross-region reconciliation (weaker consistency). Neither is specified. If the distributed architecture gate (see above) decides "single-region only for Integration Beta," then multi-region is a v1.0+ problem — but it needs to be acknowledged as such, with architectural scaffolding decisions made now that don't close off multi-region later.

**Simulation Fidelity Gap for Production Failure Modes**

DESIGN.md §17.8 explicitly documents simulation fidelity limitations: "partial object writes are not modeled." The 2PC Iceberg cold-tier sink writes Parquet files to object storage. A mid-write crash produces a partial Parquet file. S3 will serve this partial file without error (it returns whatever bytes were written to the bucket). A reader of the partial file will see a corrupt Parquet footer and fail to decode the file. SimRuntime does not model this because SimObjectStore stores bytes in memory and never partially commits them. The 100k-seed soak (v0.36 deliverable) cannot exercise this failure mode. DESIGN.md §17.8 says this is a known limitation but does not provide a plan to address it. For the cold-tier Parquet sink to be production-safe, either: (a) SimObjectStore must model partial writes, or (b) the Parquet write path must use S3 MPU with abort-on-failure semantics and a recovery scan for orphaned MPUs.

**Documentation Completeness Gap**

ROADMAP.md v0.36 lists "Distributed architecture guide" as a documentation deliverable. This is a specific artifact — a prose document describing how to deploy RockStream in distributed mode (K8s manifests, control plane setup, worker registration, network requirements, SlateDB cluster sizing). No such file exists in the repository. DESIGN.md §3.1 covers the architecture conceptually but is not an operational guide. The absence of this deliverable means v0.36 is not fully shipped by its own definition.

---

## 3. Concrete Improvement Proposals

### Proposal 1 — 2PC Sink-Type-Specific Recovery Contracts

**Problem**: The 2PC exactly-once guarantee in §11.4 relies on idempotent re-commit, but idempotency semantics differ per sink type and the spec does not enumerate them.

**Proposed Design**:

Define a `SinkIdempotencyProfile` enum in the 2PC sink contract:

```rust
pub enum SinkIdempotencyProfile {
    /// External system supports idempotent re-commit natively (e.g., S3 atomic rename, 
    /// Postgres COMMIT PREPARED after PREPARE TRANSACTION on a named transaction).
    NativeIdempotent,

    /// External system requires a fencing token to distinguish first commit from re-commit.
    /// Sink must store epoch + worker_id + shard_id as the idempotency key.
    FencingTokenRequired { token_ttl_seconds: u64 },

    /// External system does not support re-commit (e.g., Kafka transactional producer 
    /// after broker-side abort). Recovery must check sink_state/committed before re-running.
    CheckBeforeCommit,
}
```

Amend §11.4 to require that each sink implementation declares its `SinkIdempotencyProfile` and that the recovery driver uses the profile to determine whether to re-run commit or skip it. Add a mandatory integration test per sink type that exercises the crash-after-external-commit / crash-before-sink_state-written window.

For Kafka specifically: recovery must inspect the transactional producer's `describe_transactions` API to determine whether the epoch's transaction ID was committed or aborted. If aborted (broker timed it out), recovery must re-run `pre_commit → commit` against a new transaction. This is a fundamentally different recovery path than "just call commit again."

### Proposal 2 — Phase Status Synchronization Protocol

**Problem**: IMPLEMENTATION_PLAN.md and ROADMAP.md are out of sync; Phase 4–12 show "Not started" in IMPLEMENTATION_PLAN.md but v0.28–v0.36 are marked Done in ROADMAP.md.

**Proposed Design**:

1. Add a `Status` column to IMPLEMENTATION_PLAN.md's Phase table that mirrors ROADMAP.md's version-to-phase mapping. This column should be auto-derived: "If the corresponding ROADMAP version is ✅ Done, the phase is ✅ Done."

2. Add a mandatory "Exit Criteria Sign-Off" section to each phase in IMPLEMENTATION_PLAN.md. When a phase is completed, the sign-off section is filled in with: date, sign-off author, link to benchmark/test results, and exceptions (criteria waived with rationale).

3. For Phase 4 specifically: either retroactively document the 4-host network test results (if they were run informally) or explicitly waive the criterion with a documented rationale and compensating control (e.g., "4-host test deferred; compensated by SimNetwork latency injection in 1000 simulation seeds").

### Proposal 3 — v0.43 Decomposition into v0.43–v0.46

**Problem**: v0.43 is over-scoped by 4–6x.

**Proposed Decomposition**:

- **v0.43 (DML Alpha)**: INSERT, UPDATE, DELETE over pgwire only. Optimistic transactions with conflict detection. Session max-staleness. INSERT…RETURNING. No CRDTs, no zero-downtime replacement, no background DDL.
- **v0.44 (DML Hardening)**: Idempotency enforcement (client-supplied idempotency keys). Write-fence tokens. Background DDL with WAIT/NO WAIT. Zero-downtime view replacement.
- **v0.45 (CRDT Columns)**: GCounter, PNCounter, LWWValue only (the three simplest). ORSet and MVRegister deferred to v0.46.
- **v0.46 (Namespace + CRDTs v2)**: Namespace lifecycle commands. ORSet and MVRegister. Integration Beta gate moved to v0.46 exit.

This decomposition is consistent with the CRDT Value decision gate — if CRDTs are deferred to v1.0, drop v0.45 and v0.46 CRDT content, advancing Integration Beta to v0.45.

### Proposal 4 — AlignmentBuffer Schema Version Tagging

**Problem**: `AlignmentBuffer` stores raw bytes with no schema version, risking decode failures across schema changes during checkpoint alignment.

**Proposed Code Change** (before/after in §4):

Amend `AlignmentBuffer` to store a `schema_epoch: u64` alongside each buffered row. At drain time, if the current schema epoch differs from the stored epoch, apply schema migration before deserialization.

Additionally, amend `CheckpointCoordinator` to reject checkpoint barrier injection if a schema change is pending for the affected view (i.e., if `view_schema_epoch_pending != view_schema_epoch_committed`). This prevents the race entirely rather than relying on migration at drain time.

### Proposal 5 — WeightAdd/v1 Hot-Key Salting Correction

**Problem**: `WeightAdd/v1` (DISTINCT) correctness breaks under hot-key bucket salting because per-bucket partial weight deltas can escape the salting tier before the combiner cancels them.

**Proposed Design**:

For `WeightAdd/v1` operators (and any law registered as `requires_full_weight_for_correctness: true`), disable bucket salting and instead route all rows for a hot key to a single dedicated "spill" shard. The spill shard handles the full weight computation and then redistributes to downstream consumers. This eliminates the partial-weight correctness issue at the cost of the hot-key throughput optimization — but `WeightAdd/v1` (DISTINCT) fundamentally requires seeing all weights for a key before emitting output.

Amend DESIGN.md §10.5 to document this exception: "Bucket salting is not applicable to non-composable aggregates. A law is non-composable if its `LawDescriptor.composable = false`. `WeightAdd/v1` MUST set `composable = false`."

### Proposal 6 — SimObjectStore Partial Write Injection

**Problem**: SimRuntime does not model partial object writes, leaving the cold-tier Parquet sink untested for mid-write crash scenarios.

**Proposed Design**:

Add `partial_write_probability: f64` to `SimObjectStore`'s fault model. When a write is triggered with `buggify!` and `partial_write_probability` fires, the `SimObjectStore` truncates the written bytes at a random offset and marks the object as `PartiallyWritten`. Reads of a `PartiallyWritten` object return the truncated bytes without error (modeling S3 behavior).

Add a `PartialWriteRecoveryTest` to the `law_faults` simulation corpus that exercises: write Parquet → partial write injected → worker crash → recovery → cold-tier reader encounters partial Parquet → recovery scan identifies orphaned MPU → re-run cold-tier snapshot.

### Proposal 7 — RecoveryDriver Unique ID Fix

**Problem**: `RecoveryDriver::mark_complete(started_at_ms)` uses millisecond timestamp as a unique key, which is ambiguous under concurrent recoveries.

**Proposed Change**:

Replace `started_at_ms: u64` with `recovery_id: uuid::Uuid` (or a `u64` monotonic counter seeded at process start). Generate the ID at `trigger_recovery()` time and return it to the caller. `mark_complete(recovery_id)` then uses the UUID for lookup. This is a one-line interface change with a straightforward implementation change.

### Proposal 8 — RS Error Code Registry Correction

**Problem**: RS-1601, RS-1602 (checkpoint), RS-1603 (recovery) use codes in the connector/source/sink range (RS-1xxx) instead of the shard/runtime/placement range (RS-3xxx).

**Proposed Change**:

Reassign:
- RS-1601 → RS-3601 (checkpoint alignment buffer overflow)
- RS-1602 → RS-3602 (checkpoint injection conflict)
- RS-1603 → RS-3603 (recovery slow threshold exceeded)

Update DESIGN.md §14.14's error code table to explicitly reserve RS-3600–3699 as "Checkpoint and Recovery Subsystem." Update both source files accordingly.

### Proposal 9 — Cold-Tier Compaction Policy Specification

**Problem**: DESIGN.md §12.3.2 does not specify cold-tier compaction policy, leading to unbounded cold-tier growth for views with frequent retractions.

**Proposed Design**:

Add a `cold_tier_compaction_policy` field to the view configuration with the following modes:

- `NONE`: No compaction (lowest write amplification, highest read cost for retraction-heavy views).
- `PERIODIC`: Compact cold tier every `compaction_interval_epochs` by reading all Parquet files, applying Z-set cancellation (`+weight` and `-weight` rows with matching key cancel each other), and writing a new compacted Parquet file.
- `SIZE_TRIGGERED`: Trigger compaction when cold-tier file count exceeds `compact_threshold` or total size exceeds `compact_size_bytes`.

For views registered with any `WeightAdd/v1` or `WeightNegate` law, `PERIODIC` should be the default. For append-only views (no retractions), `NONE` is correct.

### Proposal 10 — Distributed Architecture Gate Resolution and Documentation

**Problem**: Two open decision gates in ROADMAP.md v0.36 (Distributed Architecture topology, CRDT value) have no outcomes. The "Distributed architecture guide" documentation deliverable is missing.

**Proposed Actions**:

1. Hold a decision meeting before v0.37 planning to formally resolve both gates. Record outcomes in ROADMAP.md's "Decision Gates" table with: decision, date, decision-maker, rationale.

2. Create `docs/distributed-architecture-guide.md` covering: K8s deployment topology, control-plane sizing, worker resource requirements, network requirements (ports, firewall rules), SlateDB shard sizing for the control plane, latency budget at each component boundary, and operational runbooks for common failure modes (control-plane unavailable, worker self-fence trigger, object-store brownout).

3. Do not mark v0.36 as fully complete until this deliverable exists.

---

## 4. Remediation Recommendations

### 4.1 DESIGN.md Remediation

#### 4.1.1 §11.4 — 2PC Crash Window (Before/After)

**Before (current §11.4 text, abridged)**:
```
If a worker crashes after `pre_commit` but before `commit`, recovery
re-runs the commit path because external commit operations MUST be idempotent.
```

**After (proposed §11.4 amendment)**:
```
If a worker crashes after `pre_commit` but before writing
`sink_state/{epoch}/committed`, recovery behavior depends on the sink's
`SinkIdempotencyProfile`:

  - `NativeIdempotent`: Recovery calls `commit(epoch, checkpoint_id)` directly.
    The external system guarantees at-most-once delivery via its own
    idempotency mechanism (e.g., S3 conditional PUT, Postgres named transaction).

  - `FencingTokenRequired`: Recovery calls `commit(epoch, checkpoint_id,
    fencing_token)` where `fencing_token = hash(worker_id || shard_id || epoch)`.
    The sink implementation must store and check this token to suppress duplicate
    commits.

  - `CheckBeforeCommit`: Recovery first reads the external system's commit status
    (e.g., Kafka `describe_transactions`, Postgres `pg_prepared_xacts`). If the
    transaction was committed externally, recovery only updates `sink_state/`.
    If the transaction was aborted externally (e.g., Kafka broker timeout),
    recovery must re-run `pre_commit` followed by `commit`.

Sink implementations MUST declare their `SinkIdempotencyProfile` in their
`SinkDescriptor` registration. The recovery driver reads this profile at
recovery time and dispatches accordingly. Failing to declare a profile causes
the sink registration to be rejected with RS-3701 (SINK_MISSING_IDEMPOTENCY_PROFILE).
```

#### 4.1.2 §10.5 — Hot-Key Salting and WeightAdd/v1 (Before/After)

**Before (current §10.5 text, abridged)**:
```
For algebraic aggregates this is exact partial aggregation: each bucket
shard computes a partial aggregate, and the combiner merges the partial
aggregates into the final result. This is correct for all registered laws.
```

**After (proposed §10.5 amendment)**:
```
For algebraic aggregates this is exact partial aggregation: each bucket
shard computes a partial aggregate, and the combiner merges the partial
aggregates into the final result.

EXCEPTION: Non-composable aggregates (laws where `LawDescriptor.composable
= false`) MUST NOT use bucket salting. A non-composable aggregate requires
access to the full key weight before emitting output. `WeightAdd/v1` (DISTINCT)
is non-composable because it fires output only on zero-crossings of the total
weight. Applying bucket salting to `WeightAdd/v1` can cause spurious per-bucket
delta emissions before the combiner sees the full weight.

For hot keys whose aggregate operator is non-composable, the planner MUST
route all rows for that key to a single designated spill shard. The spill
shard applies the aggregate over the full weight and redistributes the
result downstream. Hot-key throughput for non-composable aggregates is
therefore bounded by single-shard throughput; this is the fundamental
tradeoff and cannot be avoided without weakening the aggregate semantics.
```

#### 4.1.3 §17.8 — Simulation Fidelity Limitations (Before/After)

**Before (current §17.8 text, abridged)**:
```
Known simulation fidelity limitations:
- Partial object writes are not modeled.
- Network packet fragmentation is not modeled.
- Clock skew between worker and control plane is modeled via SimClock
  with configurable skew_ms parameter.
```

**After (proposed §17.8 amendment)**:
```
Known simulation fidelity limitations:

1. Partial object writes: SimObjectStore atomically commits writes in memory.
   Real S3/GCS/ABS may serve partial bytes for in-progress or crashed MPU
   uploads. Consequence: cold-tier Parquet sink crash-recovery is not fully
   exercised. Mitigation target: v0.37 to add `partial_write_probability`
   to SimObjectStore's fault model (see sim/fault_model.rs PARTIAL_WRITE
   fault ID).

2. Network packet fragmentation: not modeled. TCP segmentation of large
   shuffle frames is not exercised. Consequence: the wire_version negotiation
   handshake is not tested against fragmented reads.

3. S3 LIST consistency delays: SimObjectStore returns synchronously consistent
   LIST results. Real S3 may return stale LIST responses for up to several
   seconds after a PUT. Consequence: the CALM epoch manifest verifiability
   property (§8.4) is not tested against LIST staleness. Mitigation: add
   `list_staleness_epochs` fault parameter to SimObjectStore.

4. Kafka transactional broker timeout: SimNetwork can inject message drops
   but does not model Kafka broker-side transaction timeout (which aborts
   open transactions after `transaction.timeout.ms`). Consequence: the
   `CheckBeforeCommit` recovery path for Kafka sinks is not exercised.

Each known limitation is tracked in the simulation coverage matrix (Appendix B).
Gaps marked [UNMITIGATED] must be resolved before the corresponding feature
reaches the Integration Beta gate (v0.45).
```

#### 4.1.4 §3.2 — Frontier Aggregator Leader Crash (Before/After)

**Before (current §3.2 text, abridged)**:
```
The lease-based leader election ensures that exactly one frontier aggregator
is active at any time. A new leader re-reads all shard frontiers from control
SlateDB and recomputes the cluster-wide frontier.
```

**After (proposed §3.2 amendment)**:
```
The lease-based leader election ensures that exactly one frontier aggregator
is active at any time. A new leader re-reads all shard frontiers from control
SlateDB and recomputes the cluster-wide frontier.

To prevent frontier skew at leader failover, frontier writes to control SlateDB
MUST use `WriteBatchOptions { sync: true }` (synchronous WAL flush). This
ensures that a new leader's initial read sees all frontier updates from the
previous leader, regardless of whether the previous leader's WAL was flushed
before the crash.

The performance cost of synchronous frontier writes is bounded by the frontier
publication interval (default 100ms). A synchronous flush adds at most one
object-store round-trip (≤50ms p99 for co-located control plane). This is
within the frontier aggregation latency budget (§3.0: ≤100ms for the
aggregation step).

Frontier writes that fail the synchronous flush due to object-store unavailability
MUST NOT be silently dropped. The leader MUST retry with exponential backoff for
up to `frontier_write_timeout_ms` (default 5000ms). If the flush cannot complete
within this window, the leader MUST resign its lease and trigger a new election.
```

### 4.2 IMPLEMENTATION_PLAN.md Remediation

#### 4.2.1 Phase Status Table (Before/After)

**Before (current Phase overview table, abridged)**:
```
| Phase | Version Range | Status      |
|-------|---------------|-------------|
| 0     | v0.1          | Done        |
| 1     | v0.2–v0.4     | Done        |
| 2     | v0.6–v0.10    | Done        |
| 3     | v0.12–v0.18   | Done        |
| 4     | v0.20–v0.28   | Not started |
| 5     | v0.29–v0.30   | Not started |
| 6     | v0.31–v0.32   | Not started |
| 7     | v0.33–v0.34   | Not started |
| 8     | v0.35–v0.36   | Not started |
| 9     | v0.39–v0.45   | Not started |
| 10    | v0.46–v0.50   | Not started |
| 11    | v0.51–v0.53   | Not started |
| 12    | v0.54–v0.55   | Not started |
```

**After (proposed Phase overview table)**:
```
| Phase | Version Range | Status             | Sign-Off Location                     |
|-------|---------------|--------------------|---------------------------------------|
| 0     | v0.1          | Done               | —                                     |
| 1     | v0.2–v0.4     | Done               | —                                     |
| 2     | v0.6–v0.10    | Done               | —                                     |
| 3     | v0.12–v0.18   | Done               | plans/full-assessment-v0.18.md        |
| 4     | v0.20–v0.28   | Done (*)           | plans/phase4-signoff.md [OUTSTANDING] |
| 5     | v0.29–v0.30   | Done (*)           | plans/phase5-signoff.md [OUTSTANDING] |
| 6     | v0.31–v0.32   | Done               | plans/full-assessment-v0.18.md        |
| 7     | v0.33–v0.34   | Done               | plans/full-assessment-v0.18.md        |
| 8     | v0.35–v0.36   | Done (*)           | plans/full-assessment-v0.36.md        |
| 9     | v0.39–v0.45   | Not started        | —                                     |
| 10    | v0.46–v0.50   | Not started        | —                                     |
| 11    | v0.51–v0.53   | Not started        | —                                     |
| 12    | v0.54–v0.55   | Not started        | —                                     |

(*) Done with outstanding sign-off items. See sign-off document for waived
    criteria and compensating controls.
```

#### 4.2.2 Phase 4 Exit Criteria (Before/After)

**Before (current Phase 4 exit criteria)**:
```
Exit criteria:
- 16-shard cluster on ≥ 4 hosts
- Real network latency injection (tc-netem or equivalent)
- Shuffle protocol end-to-end latency measured at p50/p90/p99
- Principal architect sign-off
```

**After (proposed Phase 4 exit criteria with waiver option)**:
```
Exit criteria:
- 16-shard cluster on ≥ 4 hosts
- Real network latency injection (tc-netem or equivalent)
- Shuffle protocol end-to-end latency measured at p50/p90/p99
- Principal architect sign-off

WAIVER OPTION (if 4-host test is not feasible at phase completion):
A criterion may be waived if ALL of the following compensating controls are met:
  1. SimNetwork latency injection covering the same latency distribution as
     the real-network test (median 10ms, p99 100ms, configurable jitter ±5ms)
  2. At least 10,000 simulation seeds exercising the shuffle protocol under
     simulated latency
  3. The waiver is documented in plans/phase4-signoff.md with rationale and
     a commitment to run the real-network test before v0.45 Integration Beta
  4. The waiver is reviewed and approved by the technical lead

Waived criteria are marked [WAIVED-WITH-COMPENSATING-CONTROLS] in the
sign-off document. They become blocking before the next gate that depends
on them (Integration Beta, Phase 9 exit).
```

#### 4.2.3 Phase 8 Exactly-Once Scope Clarification (Before/After)

**Before (current Phase 8 deliverables section)**:
```
v0.36 deliverables:
- 2PC sink protocol (pre_commit/commit/abort)
- Chaos simulation alpha
- Law-equivalence-under-fault corpus
- Wire protocol version negotiation
- Object-store brownout handling
- Kafka/S3/Postgres sink stubs
```

**After (proposed Phase 8 deliverables section)**:
```
v0.36 deliverables:
- 2PC sink protocol (pre_commit/commit/abort) [IMPLEMENTED]
- Chaos simulation alpha [IMPLEMENTED]
- Law-equivalence-under-fault corpus [IMPLEMENTED]
- Wire protocol version negotiation [IMPLEMENTED]
- Object-store brownout handling [IMPLEMENTED]
- Kafka/S3/Postgres sink STUBS (not production implementations) [IMPLEMENTED]

v0.36 known gaps (tracked for resolution before v0.45 Integration Beta):
- [ ] Kafka stub does not implement real transactional producer protocol.
      Real Kafka exactly-once requires `transaction.id` registration,
      epoch bumping via `initTransactions`, and broker-side abort on timeout.
      Tracked: plans/gap-kafka-exactly-once.md
- [ ] S3 stub does not implement real MPU lifecycle.
      Tracked: plans/gap-s3-exactly-once.md
- [ ] Postgres stub does not implement PREPARE TRANSACTION / COMMIT PREPARED.
      Tracked: plans/gap-postgres-exactly-once.md
- [ ] Distributed architecture guide missing.
      Tracked: docs/distributed-architecture-guide.md [OUTSTANDING DELIVERABLE]
```

### 4.3 ROADMAP.md Remediation

#### 4.3.1 Decision Gate Outcomes (Before/After)

**Before (current ROADMAP.md decision gates table, v0.36 row)**:
```
| v0.36 | Distributed Architecture | Decide: single-region vs. multi-region for Beta |  |
| v0.36 | CRDT value               | Decide: include CRDTs in Beta or defer to v1.0   |  |
```

**After (proposed decision gates table with required outcome columns)**:
```
| v0.36 | Distributed Architecture | Decide: single-region vs. multi-region for Beta |
|       |                          | OUTCOME [REQUIRED BEFORE v0.37 PLANNING]:        |
|       |                          | [ ] Single-region only for Beta                  |
|       |                          | [ ] Multi-region with relaxed frontier semantics |
|       |                          | [ ] Multi-region with full consistency           |
|       |                          | Decision date: _________ Decided by: _________   |

| v0.36 | CRDT value               | Decide: include CRDTs in Beta or defer to v1.0   |
|       |                          | OUTCOME [REQUIRED BEFORE v0.37 PLANNING]:        |
|       |                          | [ ] Include CRDTs in v0.43–v0.44 (decompose v0.43)|
|       |                          | [ ] Defer CRDTs to v1.0 (drop from v0.43 scope) |
|       |                          | Decision date: _________ Decided by: _________   |
```

#### 4.3.2 v0.43 Decomposition (Before/After)

**Before (current ROADMAP.md v0.43 row)**:
```
| v0.43 | Integration Beta: DML + CRDT columns |
|       | DML over pgwire, CRDT columns, idempotency, optimistic transactions,
|       | session RYW, INSERT...RETURNING, max-staleness, zero-downtime replacement,
|       | write fence, background DDL, namespace lifecycle |
```

**After (proposed ROADMAP.md v0.43–v0.46 rows)**:
```
| v0.43 | DML Alpha: Core pgwire DML                               |
|       | INSERT, UPDATE, DELETE, INSERT...RETURNING                |
|       | Optimistic transactions with conflict detection           |
|       | Session max-staleness                                     |

| v0.44 | DML Hardening: Transactions and DDL                       |
|       | Idempotency enforcement (client idempotency keys)         |
|       | Write-fence tokens                                        |
|       | Background DDL (WAIT / NO WAIT)                          |
|       | Zero-downtime view replacement                            |
|       | Namespace lifecycle (CREATE/DROP NAMESPACE)               |

| v0.45 | CRDT Columns Alpha (if CRDT gate = Include)               |
|       | GCounter, PNCounter, LWWValue                             |
|       | Session-scoped read-your-writes                           |

| v0.46 | CRDT Columns Beta + Integration Beta Gate                  |
|       | ORSet, MVRegister                                         |
|       | Integration Beta exit criteria sign-off                   |

  NOTE: If CRDT gate = Defer to v1.0, drop v0.45 CRDT content and
  merge v0.45 "Session RYW" into v0.44. Integration Beta gate moves
  to v0.45 exit.
```

---

## 5. Additional Findings: Observability and Operational Gaps

### 5.1 Observability Gaps

**Epoch Latency Histogram Granularity (§14.1)**

DESIGN.md §14.1 specifies an `epoch_processing_latency_ms` histogram with buckets at [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]. This histogram tracks per-shard epoch processing time from input ingestion to epoch-commit WriteBatch flush. However, §14.1 does not specify a separate histogram for the sub-components of epoch processing: SQL operator execution time, shuffle encode/decode time, SlateDB WriteBatch assembly time, and SlateDB flush-to-WAL time. Without sub-component histograms, an operator observing a p99 epoch latency spike from 200ms to 2000ms has no automated way to determine whether the spike is in the SQL execution (operator CPU), the shuffle (network/serialization), or the SlateDB flush (object-store I/O). Debugging requires log scraping and manual correlation. DESIGN.md §14 should specify sub-component span instrumentation as a first-class requirement, not a future nice-to-have.

**Frontier Lag Metric Is Missing for the Dual-Frontier Gap (§14.3)**

DESIGN.md §14.3 specifies `frontier_lag_epochs` as the gap between the current epoch and the published `visible_frontier`. But the dual-frontier model (§3.0) distinguishes `visible_frontier` (in-memory, immediately after operator evaluation) from `durable_frontier` (after WAL flush). Under object-store degradation, `durable_frontier` lags `visible_frontier` by the brownout buffer depth (up to `local_buffer_max_epochs = 10` epochs). DESIGN.md §14.3 does not specify a `durable_frontier_lag_epochs` metric. Without this metric, an operator cannot detect that the system is in a brownout condition from metrics alone — they must look at the `BrownoutStatus` log event. The `durable_frontier_lag_epochs` metric should be emitted by the same component that tracks `BrownoutStatus`.

**2PC Phase Duration Metrics Are Absent (§14)**

DESIGN.md §14 provides a comprehensive metrics catalog but does not include 2PC sink phase duration metrics. For exactly-once sinks, the following metrics are operationally critical:
- `two_pc_pre_commit_latency_ms`: time from epoch-commit decision to `pre_commit` acknowledgment from external sink
- `two_pc_commit_latency_ms`: time from checkpoint completion to `commit` acknowledgment
- `two_pc_abort_count`: count of aborted 2PC transactions (should be near zero in steady state)
- `two_pc_recovery_count`: count of 2PC recoveries triggered (each is a potential duplicate-emission risk)

Without these metrics, an operator cannot distinguish "exactly-once sink is slow" from "exactly-once sink is failing and retrying." The `TwoPcPhase` and `TwoPcSinkState` types in `rockstream-sim/src/two_pc.rs` show the state machine is modeled — the metrics should expose this state machine's transitions.

**Arrangement Size Gauge Has No Per-Operator Attribution (§14.5)**

DESIGN.md §14.5 specifies an `arrangement_size_bytes` gauge at the shard level. A shard may host multiple operators, each with its own arrangement (Join left-side, Join right-side, IndexedLookup, TopK state). A shard-level gauge cannot identify which operator's arrangement is consuming disproportionate storage. DESIGN.md §14.5 should require a per-operator-instance label on `arrangement_size_bytes`. This is especially important for joins, where an unexpected cardinality explosion on one side (e.g., a cartesian join introduced by a missing join condition) will inflate arrangement size silently at the shard level.

**Shuffle Queue Depth Has No Backpressure Visibility (§14.6)**

DESIGN.md §14.6 specifies `shuffle_queue_depth` as a gauge per (source_shard, destination_shard) pair. This is correct. But the spec does not specify what metric indicates that backpressure is being applied — i.e., that source shards are being stalled because the shuffle queue is full. DESIGN.md §7.3 specifies that source shards stall their epoch commit when the shuffle outbox exceeds `shuffle_outbox_max_epochs`. The stall duration is not surfaced as a metric in §14.6. An operator watching `shuffle_queue_depth` see a high watermark but cannot tell from metrics whether upstream shards are stalling. A `shuffle_backpressure_stall_ms` histogram (per source shard) is needed.

**Liveness Check Metrics Are Not Specified (§14)**

`rockstream-sim/src/lib.rs` exports `DegradedState`, `LivenessChecker`, and `LivenessStatus` from the `liveness` module. These are new in v0.36. However, DESIGN.md §14 does not include any metrics for the liveness subsystem. An operator cannot tell from Prometheus whether the liveness checker has transitioned a shard to `DegradedState` without reading structured logs. At minimum, a `liveness_status` gauge with label `{shard_id, status}` and a `liveness_degraded_transitions_total` counter should be specified in §14. The liveness subsystem is one of the key v0.36 operational additions; its absence from the metrics catalog is an observability gap for the operators most likely to need it during a degraded cluster event.

**No SLO Budget Burn-Rate Alerts Are Specified (§14.13)**

DESIGN.md §14.13 (Alerting) specifies static threshold alerts (e.g., `epoch_processing_latency_p99 > 500ms for 5 minutes`). This is a lowest-common-denominator alerting approach. For a system with multi-level latency classes (§3.0), the more useful alert model is Google SRE-style error budget burn rate: "how fast are we consuming the `distributed_fresh` SLA error budget?" A burn-rate alert fires earlier than a threshold alert (it fires at 2× the steady-state miss rate, not only when p99 exceeds a fixed threshold) and is less noisy (it does not fire on transient spikes that do not affect the budget). DESIGN.md §14.13 should specify burn-rate alert thresholds for the two latency classes most likely to be SLO'd by external users: `distributed_fresh` (≤5s p90) and `distributed_exact_sink` (≤30s p90 for exactly-once delivery). This is an Integration Beta readiness requirement — external users will ask for SLO guarantees, and the alerting spec should pre-answer how those SLOs are monitored.

### 5.3 Security and Multi-Tenancy Gaps

**API Key Scoping Lacks View-Level Granularity (§14.11)**

DESIGN.md §14.11 specifies API key management with scopes: `read`, `write`, `admin`. These are namespace-level scopes. For a multi-tenant deployment (multiple teams sharing a RockStream cluster), there is no mechanism to restrict a team's API key to a specific set of views within a namespace. A team with `read` scope on namespace `analytics` can read all views in that namespace, including views that contain sensitive data from other teams' pipelines that were routed to the same namespace by a misconfigured pipeline deployment. View-level ACLs are standard in data warehouse products (BigQuery IAM conditions, Snowflake privilege grants) and should be specified in §14.11 before Integration Beta, since multi-tenancy is a common cloud deployment pattern.

**Worker-to-Worker Shuffle Authentication Is Unspecified (§7)**

DESIGN.md §7 specifies the shuffle protocol between workers but does not specify how a receiving worker authenticates that an incoming shuffle frame originates from a legitimate member of the cluster. A malicious process that knows the shuffle port and message format (derivable from the `wire_version` negotiation spec) could inject arbitrary shuffle frames into a worker. DESIGN.md §7 should require mutual TLS (mTLS) between workers, with certificates managed by the control plane or an external CA. The `wire_version` negotiation added in v0.36 is the right place to add TLS handshake negotiation.

### 5.4 Connector Contract Gaps

**Postgres CDC Schema Change Events Are Underspecified (§13.3)**

DESIGN.md §13.3 defines the connector contract for CDC sources. It specifies that the connector must emit a `SchemaChangeEvent` when the upstream schema changes, and that the RockStream planner must re-plan the dataflow after receiving such an event. But §13.3 does not specify:
- What happens to rows already buffered in the connector's ingest buffer that were serialized under the old schema
- Whether the schema change event is guaranteed to arrive in epoch-order (i.e., before any row that uses the new schema)
- Whether the planner's re-plan is synchronous (stalling ingestion) or asynchronous (allowing rows with the new schema to arrive before re-planning completes)
- What the rollback behavior is if re-planning fails

For a Postgres CDC connector using `pgoutput` logical replication, DDL changes are delivered as separate `relation` messages in the replication stream before the first DML row under the new schema. The connector can guarantee epoch-ordered delivery. But the RockStream connector contract does not require this, and a connector that delivers a `SchemaChangeEvent` out of order could cause the planner to apply the wrong schema to buffered rows.

**S3 Source Partition Discovery Race (§13.2)**

DESIGN.md §13.2 specifies the S3 source connector. The connector periodically polls S3 for new objects matching a prefix pattern. There is a race between: (a) the connector listing objects at time T and finding N objects, and (b) a producer writing a new object between T and T+poll_interval that the connector will not discover until T+poll_interval. This is inherent to the poll-based S3 source model. However, §13.2 does not specify the maximum discovery latency guarantee or how it interacts with the `distributed_fresh` latency class (≤5s). If `poll_interval = 60s`, the S3 source's contribution to end-to-end latency is 0–60s, which violates the `distributed_fresh` SLA. §13.2 should specify a maximum `poll_interval` for sources that serve `distributed_fresh` views, or explicitly classify S3 source-backed views as `analytical_cold` only.

**Direct-Write Source Isolation Level Is Ambiguous Under Concurrent Sessions (§13.5.3)**

DESIGN.md §13.5.3 specifies "session-scoped automatic read-your-writes" for the Direct-Write Internal Source. Under concurrent sessions (multiple pgwire clients writing simultaneously), a client that writes row A and then reads the view should see row A in the result. The spec achieves this by tracking the writing session's last committed epoch and stalling the read until the gateway's `visible_frontier` advances past that epoch. This is correct for single-writer sessions. But §13.5.3 does not specify the behavior when two sessions write to the same key within the same epoch: which session's write wins? The optimistic transaction conflict detection (§13.5.1) handles this with a CAS abort. But the losing session receives a 40001 serialization error and must retry. The retry will target a new epoch. If the client retries in the same session, its read-your-writes guarantee now requires waiting for the retry's epoch to commit — but the gateway's `visible_frontier` may have already advanced past the failed epoch. §13.5.3 needs to specify read-your-writes semantics after a conflict-retry.

---

## 6. Summary Assessment and v0.37 Entry Criteria

### 6.1 What v0.36 Got Right

The v0.36 release represents a genuine engineering advance in three areas:

1. **Simulation infrastructure maturity**: The `rockstream-sim` crate now covers brownout, chaos, clock, coord_faults, fault_model, law_faults, liveness, network, object_store, paired_assert, runtime, sim, soak, tokio_rt, two_pc, and wire_version. The 100k-seed soak with regression corpus is a meaningful quality gate. The law-equivalence-under-fault corpus ensures CRDT/monoid correctness is continuously validated.

2. **Wire protocol versioning**: The `wire_version` module (`negotiate_version`, `NegotiationResult`, `ProtocolVersion`, `SupportedVersionRange`) is essential infrastructure for rolling upgrades. Its presence in v0.36 means v0.37+ can begin the rolling-upgrade discipline.

3. **Brownout handling**: The `BrownoutStatus`/`ObjectStoreBrownoutGuard`/`LOCAL_BUFFER_MAX_EPOCHS` implementation provides a sensible backpressure path for object-store degradation. This is an operational requirement for any cloud-hosted deployment.

### 6.2 What Must Be Resolved Before v0.37

The following items must be resolved (or formally waived with compensating controls) before v0.37 planning is locked:

1. **[BLOCKING] Distributed Architecture gate**: Resolve single-region vs. multi-region topology. Record in ROADMAP.md decision gates table.

2. **[BLOCKING] CRDT value gate**: Resolve include vs. defer. Record in ROADMAP.md. Decompose v0.43 per §4.3.2 above regardless of outcome.

3. **[BLOCKING] Phase 4 sign-off**: Create `plans/phase4-signoff.md` documenting either the 4-host test results or the formal waiver with compensating simulation coverage.

4. **[BLOCKING] Distributed architecture guide**: Create `docs/distributed-architecture-guide.md` to satisfy the v0.36 documentation deliverable.

5. **[HIGH PRIORITY] RS error code corrections**: Reassign RS-1601→RS-3601, RS-1602→RS-3602, RS-1603→RS-3603 before v0.37 adds more error codes in the wrong range.

6. **[HIGH PRIORITY] AlignmentBuffer epoch-count bounding**: Amend `AlignmentBuffer` to bound by epoch count (matching DESIGN.md §11.2 spec) rather than row count, and add schema epoch tagging to buffered rows.

7. **[MEDIUM PRIORITY] RecoveryDriver UUID key**: Replace `started_at_ms` with a unique `recovery_id` in `RecoveryDriver::mark_complete`.

8. **[MEDIUM PRIORITY] WeightAdd/v1 composable flag**: Amend `LawDescriptor` registration to include `composable: bool`. Set `composable = false` for `WeightAdd/v1`. Amend planner to disable bucket salting for non-composable aggregates.

### 6.3 Risk Register for Integration Beta (v0.45)

| Risk | Severity | Likelihood | Mitigation |
|------|----------|------------|------------|
| Kafka exactly-once untested against real broker | Critical | High | Real Kafka integration test before v0.43 |
| Phase 4 network validation never run | High | Medium | Formal sign-off or waiver before v0.37 |
| v0.43 over-scope causes milestone slip | High | Very High | Decompose per §4.3.2 |
| Cold-tier Parquet partial write untested | Medium | Low | SimObjectStore partial write fault by v0.37 |
| AlignmentBuffer schema version gap | Medium | Medium | Schema epoch tagging before v0.37 |
| WeightAdd/v1 DISTINCT correctness under hot-key salting | High | Low (requires hot-key + DISTINCT overlap) | composable flag by v0.37 |
| CRDT value gate unresolved | High | High | Gate resolution before v0.37 planning |
| Distributed architecture guide missing | Medium | Certain | Create before v0.36 is fully shipped |
| Frontier aggregator WAL flush non-synchronous | Medium | Low (requires precise crash timing) | sync: true WriteBatch for frontier writes |
| Cold-tier unbounded growth for retraction-heavy views | Medium | Medium | Compaction policy spec before v0.38 |

---

### 6.4 Integration Beta Definition of Done

Integration Beta (v0.45) is the project's first external-user milestone. The following table defines the minimum bar for calling v0.45 the Integration Beta gate, based on the spec documents read for this assessment:

| Criterion | Current State | Required State for v0.45 |
|-----------|---------------|--------------------------|
| Real Kafka exactly-once validated | Stub only | Integration test against real Kafka broker with broker-abort recovery exercised |
| Real S3 exactly-once validated | Stub only | Integration test against real S3 with MPU lifecycle and orphaned-MPU recovery |
| Real Postgres 2PC validated | Stub only | Integration test against real Postgres with PREPARE/COMMIT PREPARED |
| Phase 4 network sign-off | Not documented | Formal sign-off or waiver in plans/phase4-signoff.md |
| Phase 5 real-S3 validation | Not documented | Benchmark results at ≥1GB shard size on real S3 |
| Distributed architecture guide | Missing | docs/distributed-architecture-guide.md published |
| Decision gates resolved | Open | Both gates resolved and recorded in ROADMAP.md |
| v0.43 scope decomposed | Not done | v0.43–v0.46 scope table updated per §4.3.2 |
| RS error code corrections | Wrong range | RS-3601, RS-3602, RS-3603 in source and DESIGN.md §14.14 |
| AlignmentBuffer epoch-based bounding | Row-based | Epoch-based limit + schema epoch tagging |
| WeightAdd/v1 composable flag | Not present | LawDescriptor.composable = false for WeightAdd/v1; planner enforces |
| SimObjectStore partial write fault | Not modeled | partial_write_probability in fault_model; Parquet crash-recovery test |
| 2PC phase duration metrics | Absent | four metrics specified in §14 and emitted by TwoPc subsystem |
| Sub-component epoch latency spans | Absent | Separate histograms for SQL exec, shuffle, WAL flush per epoch |

### 6.5 Closing Assessment Narrative

RockStream at v0.36 is structurally sound as a single-machine or small-cluster system. The DBSP Z-set formalism is correctly applied across the operator catalog. The SimRuntime simulation discipline (seeded RNG, deterministic scheduling, FoundationDB-inspired fault injection) is one of the project's strongest engineering investments and compares favorably with industry practice. The wire-version negotiation added in v0.36 is the right foundation for the rolling-upgrade discipline needed for a distributed system.

The project's primary risk going into Integration Beta is not a single catastrophic design flaw — it is the accumulation of unverified assumptions. The 2PC exactly-once guarantee has never been tested against a real broker. The distributed shuffle and recovery paths have never been validated under real network conditions on multiple hosts. The hot-key DISTINCT correctness issue under bucket salting is a silent correctness bug waiting for the right workload. The v0.43 over-scoping is a planning failure that will manifest as a missed milestone at the worst possible time — when external users are watching.

The most important action before v0.37 is not writing more code. It is closing the two open decision gates, writing the missing distributed architecture guide, and creating the Phase 4 and Phase 5 sign-off documents. These are the four items that define whether v0.36 is actually done by the project's own definition of done. Until they exist, the ✅ marks in ROADMAP.md are aspirational, not evidentiary.

---

### 6.6 Prior Assessment Comparison

The prior assessment (`plans/full-assessment-v0.18.md`, dated 2026-05-30, covering v0.18 SQL Alpha and v0.27 Single-Shard Correctness) identified five critical vulnerabilities:

1. **Design freeze violations** (15 major revisions since v0.10): v0.36 has not resolved this; DESIGN.md is now at v3.28 and continues to be modified without a corresponding IMPLEMENTATION_PLAN.md update discipline.
2. **Escape-hatch consolidation needed**: Partially resolved — §17 simulation coverage is more comprehensive in v0.36 than in v0.18, but the simulation fidelity gaps catalogued in §17.8 are new ones introduced by v0.36 features (cold tier, 2PC stubs).
3. **Storage budget ambiguous validation**: Unresolved — Phase 5 real-S3 gate is still not formally signed off.
4. **Phase 4 complexity cliff**: Partially resolved in execution (v0.28–v0.36 shipped), but the Phase 4 sign-off artifact remains missing.
5. **Cross-document terminology drift**: Worsened — `liveness`, `brownout`, `self_fence`, and `wire_version` are new terms in v0.36 that appear in source code and in DESIGN.md but are absent from the IMPLEMENTATION_PLAN.md glossary.

The net assessment: v0.36 made meaningful progress on capabilities but did not retire the architectural governance debts identified at v0.18. The two documents that most need attention before v0.37 are IMPLEMENTATION_PLAN.md (sync its phase status table) and ROADMAP.md (close its open decision gates). These are low-effort changes with high value for project coordination.

---

*Assessment complete. All findings are grounded in DESIGN.md v3.28, IMPLEMENTATION_PLAN.md, ROADMAP.md, and source files `crates/rockstream-runtime/src/checkpoint.rs`, `crates/rockstream-runtime/src/recovery.rs`, and `crates/rockstream-sim/src/lib.rs` as read in this session.*

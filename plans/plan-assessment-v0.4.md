# RockStream Plan Assessment v0.4

**Scope.** This report critiques the three core RockStream specification documents: [DESIGN.md](../DESIGN.md), [IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md), and [ROADMAP.md](../ROADMAP.md). The review assumes the stated objective is not merely a viable IVM engine, but a definitive cloud-native IVM system spanning embedded laptop use through very large Kubernetes deployments.

**Reviewer stance.** The documents are unusually mature for an early-stage database project. They correctly center DBSP/Z-set semantics, antichain frontiers, stable operator state, object-store constraints, deterministic simulation, and day-2 operability. The remaining issues are less about vision and more about hard boundary conditions: what is actually on the hot path, what can be proven under retractions and late data, how state migrates safely, and whether the public product surface remains simple as the architecture grows.

---

## 1. Executive Summary

### Architectural health check

RockStream's strongest architectural choices are the right ones:

1. **The semantic foundation is sound.** [DESIGN.md §2](../DESIGN.md#L386) adopts Z-sets, negative weights, DBSP incremental operators, and Differential Dataflow-style frontiers rather than ad hoc trigger maintenance. This is the correct mathematical substrate for full-SQL IVM with retractions.
2. **SlateDB constraints are treated as design inputs.** [DESIGN.md §5.3-§5.4](../DESIGN.md#L946) explicitly avoids nonexistent range deletion, treats manifest/WAL costs as budgets, gates compaction filters on frontiers, and separates merge-backed from explicit arrangement state.
3. **Operability is genuinely integrated into the design.** [DESIGN.md §14](../DESIGN.md#L3750), the implementation plan's phase callouts, and [ROADMAP.md Common Definition of Done](../ROADMAP.md#L51) make diagnostics, error codes, audit logs, resource bounds, and simulation tests part of the core product rather than a late dashboard.
4. **The laptop-to-cluster story has a credible shape.** [DESIGN.md §3.1](../DESIGN.md#L502) defines `embedded`, `single_worker`, and `distributed` runtime profiles, and [ROADMAP.md v0.10](../ROADMAP.md#L115) makes the local single-binary loop an early proof.

The vision is structurally sound. The highest risks are concentrated in three areas.

### Top 3 architectural risks

| Risk | Severity | Why it matters | Primary anchors |
|---|---:|---|---|
| **Durable object-store hot path is not yet reconciled with sub-ms to low-ms latency.** | Critical | The design wants object storage as the universal durable substrate, but per-epoch SlateDB writes, manifest churn, cross-worker `DbReader` misses, checkpoint polling, and durable shuffle staging can easily put the p99 path in tens of milliseconds. The docs currently state 10-250 ms freshness in several places, but the absolute objective includes sub-millisecond to low-millisecond latency. | [DESIGN.md §5.4](../DESIGN.md#L968), [§7.2](../DESIGN.md#L1376), [§9](../DESIGN.md#L1686), [§12.7.4](../DESIGN.md#L2522), [IMPLEMENTATION_PLAN.md Performance Targets](../IMPLEMENTATION_PLAN.md#L1814) |
| **Identity, retraction, and time semantics have edge-case gaps that can break correctness.** | Critical | Stable `row_id` is central to replay, joins, windows, indexes, cold-tier deduplication, and `INSERT RETURNING`. The current CDC rule includes source LSN in the row identity, which can make updates impossible to retract by the original key. Cold-tier union-by-`row_id` is underspecified for deletes and updates. Event-time windows can stay open forever when a connector has no watermark. | [DESIGN.md §6.4](../DESIGN.md#L1155), [§6.9](../DESIGN.md#L1248), [§12.7.1](../DESIGN.md#L2458), [§13.3](../DESIGN.md#L2787) |
| **Elastic state movement and skew handling are under-specified relative to the scale claim.** | High | The design mixes rendezvous hashing with range midpoint splits, but those imply different migration units. Online split/merge lacks a dual-write/catch-up/fence/cutover state machine for in-flight shuffle, source offsets, and view outputs. Hot-key virtual buckets are promising, but join skew remains the hardest case. | [DESIGN.md §7.1](../DESIGN.md#L1365), [§10.2](../DESIGN.md#L1793), [§10.5-§10.6](../DESIGN.md#L1832), [ROADMAP.md v0.38](../ROADMAP.md#L164) |

The constructive path is clear: add explicit latency classes, formalize stable identity and vector freshness, replace ambiguous shard movement with virtual buckets and a migration protocol, and tighten terminology/phase consistency before the design freezes.

---

## 2. Deep-Dive Critique (By Pillar)

### Pillar 1: Core IVM Mechanics and State Architecture

#### What is strong

- **Z-set semantics are the right core abstraction.** [DESIGN.md §2](../DESIGN.md#L386) correctly models inserts, deletes, and updates as weighted tuples. This is essential for compositional IVM and avoids the trap of treating deletes as a connector-specific afterthought.
- **The operator catalog separates algebraic and retraction-aware state.** Aggregates use merge-backed partial state in [§6.2](../DESIGN.md#L1121), while MIN/MAX, Top-K, windows, recursion, and outer joins use explicit arrangements in [§6.3-§6.10](../DESIGN.md#L1140). This distinction is crucial.
- **Outer join match accounting is explicitly specified.** [DESIGN.md §6.4.1](../DESIGN.md#L1180) correctly identifies the matched/unmatched transition problem for LEFT/RIGHT/FULL OUTER JOINs. This is where many IVM engines quietly go wrong.
- **Merge laws are elevated to a database-wide contract.** [DESIGN.md §6.11](../DESIGN.md#L1291), [IMPLEMENTATION_PLAN.md IVM-0](../IMPLEMENTATION_PLAN.md#L137), and [ROADMAP.md v0.5-v0.8](../ROADMAP.md#L109) all require law IDs, versions, duplicate policy, compaction policy, and property tests. This is a very strong design move.
- **Late data and watermarks are at least present.** [DESIGN.md §6.9](../DESIGN.md#L1248) and [§13.3](../DESIGN.md#L2787) connect event-time watermarks to source connectors, which is necessary for real streaming SQL.

#### Correctness gaps and edge cases

1. **CDC `row_id` must not include LSN as identity.**

   [DESIGN.md §6.4](../DESIGN.md#L1155) says CDC row identity is derived from "the table primary key plus source LSN." That makes the identity versioned by log position. For an update represented as `(old_row, -1)` plus `(new_row, +1)`, the retraction must target the same arrangement identity as the previous insertion. If the old insertion used LSN 10 and the delete/retract uses LSN 11, the arrangement key changes and the old row is not removed.

   **Refinement:** CDC `row_id` should be stable across versions: `(source_id, table_id, primary_key)` for keyed tables. The LSN belongs in `version_epoch`, `source_offset`, or `commit_metadata`, not in identity. For keyless CDC, the docs need an explicit synthetic identity strategy and a reconciliation rule.

2. **Keyless snapshot identity can duplicate rows across re-snapshot.**

   [DESIGN.md §6.4](../DESIGN.md#L1155) uses `(snapshot_id, file_path, row_group, row_ordinal)` for keyless snapshots. That is stable within one snapshot, but not across reconciliation snapshots. A re-snapshot after connector position loss can produce a new `snapshot_id` and therefore a new identity for the same logical row. [IMPLEMENTATION_PLAN.md IVM-11](../IMPLEMENTATION_PLAN.md#L506) mentions symmetric difference reconciliation, but the identity invariant is not stated in DESIGN.md.

   **Refinement:** keyless snapshots need either a content-derived row fingerprint with collision handling, or a documented rule that keyless tables are append-only unless the connector can provide a stable identity column.

3. **Cold-tier merge semantics are too weak for updates and deletes.**

   [DESIGN.md §12.7.1](../DESIGN.md#L2458) says the gateway merges cold snapshot plus hot LSM tail with "union with deduplication by `row_id`." That is insufficient for Z-set semantics. A hot tail may contain a retraction for a row present in the cold snapshot, an update as old `-1` plus new `+1`, or multiple versions of the same stable row.

   **Refinement:** the cold/hot merge must be a versioned Z-set merge: cold snapshot rows are base weights at `snapshot_epoch`; hot tail applies signed deltas ordered by epoch; final visibility is `sum(weight) != 0`, with row-version tie-breaking for updates. Deduplication by `row_id` is only a special case for insert-only views.

4. **Merge-backed aggregate output may force read-after-write on the hot path.**

   [DESIGN.md §6.2](../DESIGN.md#L1121) says aggregate updates call `db.merge()`, then output deltas are computed by reading/finalizing current state and comparing to last emitted values. If every epoch needs a merged read from SlateDB to emit downstream deltas, the design risks converting a write-optimized merge path into read amplification. The fallback to batched read-modify-write is correct for safety, but could destroy throughput.

   **Refinement:** specify a hot aggregate accumulator per operator instance that folds all deltas in memory for the active epoch, emits deltas from memory, and persists merge operands plus last-emitted cache atomically. SlateDB read resolution should be required for recovery, replay, cold cache, and verification, not every normal aggregate delta.

5. **Hash-keyed distinct and row-hash state need collision policy.**

   [DESIGN.md §6.6](../DESIGN.md#L1207) uses `row_hash(16)` for distinct, and [§6.3](../DESIGN.md#L1140) uses `row_hash(8)` inside MIN/MAX. Hash collisions are rare but not impossible, and database correctness cannot depend on "rare." The docs should specify whether the value stores full canonical row bytes for equality verification, whether hashes are cryptographic, and what happens on collision.

6. **Window function strategy is not yet performance-safe.**

   [DESIGN.md §6.7](../DESIGN.md#L1219) sketches segment trees for sliding aggregates, but [IMPLEMENTATION_PLAN.md IVM-7](../IMPLEMENTATION_PLAN.md#L454) starts with partition-based recomputation and defers the segment-tree variant. That is acceptable for correctness, but not for a system whose north star is ultra-low latency. Large `ROW_NUMBER` or `RANK` partitions are inherently expensive under arbitrary inserts/deletes; the docs should explicitly classify which window functions are latency-safe, which are bounded by partition size, and which require approximate or deferred maintenance.

7. **Watermark absence creates unbounded event-time state.**

   [DESIGN.md §13.3](../DESIGN.md#L2787) says a connector that cannot produce a watermark returns `None`; the event-time frontier never advances and all windows remain open. That is correct but dangerous. Without a required fallback policy, a single connector can create unbounded time-window state while the user believes windows are closing.

   **Refinement:** event-time windows over sources without watermarks should require an explicit `WATERMARK = processing_time | disabled | external` choice at DDL time. The default should fail closed for event-time windows unless the source declares a watermark.

8. **Theta/cross join broadcast is a correctness escape hatch but a scale hazard.**

   [DESIGN.md §6.5](../DESIGN.md#L1201) falls back to broadcasting the smaller side to all shards. That can be correct, but it needs an admission-control rule and `EXPLAIN` warning: any unbounded broadcast join can violate object-store, network, and state budgets.

9. **Distributed recursion cost is correctly identified but still too open.**

   [DESIGN.md §2 Recursion](../DESIGN.md#L421), [§6.8](../DESIGN.md#L1229), and [IMPLEMENTATION_PLAN.md Phase 4](../IMPLEMENTATION_PLAN.md#L651) allow exchange inside recursive scopes. This is semantically right, but the cost can be a full shuffle per iteration. The design should make recursive query admission depend on a static monotonicity/cost classification and should expose `iteration_count`, `bytes_per_iteration`, and `frontier_stall_reason` from the first recursion milestone.

### Pillar 2: The Laptop-to-Cluster Scalability Continuum

#### What is strong

- **The runtime profiles are exactly the right abstraction.** [DESIGN.md §3.1](../DESIGN.md#L502) separates `embedded`, `single_worker`, and `distributed` hot paths while preserving one state format. This is the right way to avoid punishing laptop users with distributed machinery.
- **The control plane is scoped appropriately.** [DESIGN.md §3](../DESIGN.md#L439) uses Raft only for control-plane leadership and lease fencing, not for data-plane transactions. That avoids a global write bottleneck.
- **Frontier aggregation is separated from Raft.** [DESIGN.md §3.2](../DESIGN.md#L558) is a good scale-out decision. The frontier role should not be a Raft proposal firehose.
- **Worker drain, capacity headroom, and autoscaling signals are concrete.** [DESIGN.md §10.7-§10.8](../DESIGN.md#L1892) and [ROADMAP.md v0.38](../ROADMAP.md#L164) define the right operational hooks for Kubernetes.

#### Scale-down risks

1. **The local profile needs an explicit dependency budget.**

   The specs promise one binary and no external broker for local use, but Phase 0 also mentions a dev container with SlateDB, MinIO, Postgres, and Kafka pre-installed in [IMPLEMENTATION_PLAN.md Phase 0](../IMPLEMENTATION_PLAN.md#L60). That is fine for integration tests, but the user-facing first run must not imply those are required.

   **Refinement:** add a local profile contract: no network ports except gateway unless requested, no MinIO/Kafka/Postgres required, no mTLS by default, auth off, local filesystem object-store facade, in-process source generator.

2. **"Object storage is universal" should include local filesystem explicitly.**

   [DESIGN.md P4](../DESIGN.md#L309) says state, shuffle payloads, checkpoints, and WAL all live in S3/GCS/ABS. [§5.6](../DESIGN.md#L1069) correctly introduces `local_fs`. To avoid confusion, P4 should say "object-store API" rather than only cloud buckets.

3. **Loopback still writes durable outbox/inbox metadata.**

   [DESIGN.md §7.5](../DESIGN.md#L1428) says loopback uses in-process channels but still writes durable outbox/inbox keys so replay matches the distributed path. That is correct for `single_worker`, but the embedded profile should aggressively elide replay metadata for exchanges the compiler proves local. Otherwise laptop latency pays for a failure mode that cannot occur in the same way.

#### Scale-up risks

1. **Rendezvous hashing conflicts with range-based split mechanics.**

   [DESIGN.md §7.1](../DESIGN.md#L1365) uses rendezvous hashing, where a key maps to the highest-scoring shard for that key. [§10.2](../DESIGN.md#L1793) and [§10.6](../DESIGN.md#L1869) then talk about identifying key ranges, sampling a midpoint key, and copying upper halves. Those are range-partitioning concepts, not rendezvous-hashing concepts.

   **Refinement:** introduce fixed virtual buckets as the migration unit. Hash keys to many virtual buckets, assign virtual buckets to physical shards with rendezvous or a ring, and split/move buckets rather than arbitrary key ranges. This supports predictable migration, hot-key salting, and stable object prefixes.

2. **Online migration needs a full state machine.**

   [DESIGN.md §10.2](../DESIGN.md#L1793) says snapshot, copy range, catch up, and flip at an epoch boundary. It does not specify how in-flight shuffle, source offsets, view outputs, checkpoint barriers, and readers are handled during migration.

   **Required states:** `PLANNED -> SNAPSHOTTING -> COPYING -> DUAL_WRITING -> CATCHING_UP -> FENCING_OLD -> CUTOVER -> VERIFYING -> GC_ELIGIBLE -> DONE`. Each state needs idempotent recovery, audit events, and a metric. Without this, split/merge correctness will depend on implementation folklore.

3. **Recovery-time budgets are optimistic without checkpoint/WAL byte bounds.**

   [DESIGN.md §11.5](../DESIGN.md#L2024) commits to 5 s failure detection, 30 s shard reassignment, and 60 s freshness recovery at `target_shard_state_bytes`. [§10.6](../DESIGN.md#L1869) sets the target shard size at 20 GB. A 20 GB shard can be opened quickly if checkpoint metadata is fresh and WAL replay is tiny; it cannot be promised if manifest reads, WAL replay, compaction debt, or object-store throttling are uncontrolled.

   **Refinement:** add quantitative invariants: max WAL bytes since last manifest, max manifest chain length, max SST count per shard, max open/checkpoint metadata reads, and required warm cache behavior. Recovery budgets should be expressed as functions of these numbers, not only shard size.

4. **Control-plane single writer remains a possible bottleneck.**

   The docs wisely keep data-plane state out of Raft, but the control SlateDB still serializes catalog writes, audit events, checkpoint index updates, placement decisions, frontier publications, secrets, resource accounting, and autoscaler signals. [DESIGN.md §3.2](../DESIGN.md#L558) reduces frontier write frequency, but the checkpoint and audit paths also need explicit budgets.

5. **Join skew is harder than aggregate skew.**

   [DESIGN.md §10.5](../DESIGN.md#L1832) says hot-key virtual buckets work exactly for algebraic aggregates and may split joins by replicating the small side. That is the right idea, but many real joins are many-to-many or have hot keys on both sides. The design needs a fallback classification: `replicate_small_side`, `salt_large_side`, `pre-aggregate_then_join`, `spill_to_batch`, or `SKEW_BOUND`.

### Pillar 3: Performance, Throughput, and Latency

#### Hot-path risks

1. **The docs do not yet contain a latency budget.**

   [DESIGN.md §12.7.4](../DESIGN.md#L2522) positions incremental freshness at 10-250 ms. [IMPLEMENTATION_PLAN.md Performance Targets](../IMPLEMENTATION_PLAN.md#L1814) targets `< 100 ms` single-shard and `< 200 ms` 64-shard frontier lag. The user objective asks for sub-millisecond to low-millisecond latency. The specs should explicitly distinguish:

   - point lookup latency on an already-materialized view;
   - commit-to-visibility latency in embedded mode;
   - distributed source-to-view freshness;
   - durable external sink commit latency;
   - historical/full-scan query latency.

   Without this taxonomy, the design can appear to promise sub-ms durable distributed IVM over S3, which is not physically credible.

2. **Per-epoch durability through SlateDB can dominate p99.**

   [DESIGN.md §9](../DESIGN.md#L1686) makes `WriteBatch` the only durability event per epoch per shard group. That is good for correctness and write amplification, but still ties frontier advancement to WAL/object-store behavior. [§5.4](../DESIGN.md#L968) correctly recognizes manifest churn, but the design needs explicit p99 object-store budgets and a path for low-latency local durable writes.

3. **Cross-worker `DbReader` misses are incompatible with low-ms joins.**

   [DESIGN.md §5.4](../DESIGN.md#L968) says join lookups that read state owned by another shard use `DbReader` pinned to a checkpoint and can be accelerated by a segment cache. Cache hits are fast; cache misses are 10-100 ms by the document's own estimate. A world-class low-latency IVM engine should make cross-worker arrangement reads an exception, not a normal join path.

   **Refinement:** stateful operator inputs should be co-partitioned and owned locally by default. `DbReader` should be for gateway reads, recovery, backfill, debugging, and exceptional lookup joins that `EXPLAIN` flags as object-store-bound.

4. **Compression and Arrow IPC framing need placement in the path budget.**

   [DESIGN.md §7.3-§7.4](../DESIGN.md#L1405) stores shuffle batches as compressed Arrow IPC. Arrow is the right in-memory format, but Arrow IPC plus compression per small epoch can cost more CPU than the operator itself. The docs should define when in-memory Arrow arrays are handed off zero-copy, when IPC serialization happens, and when compression is disabled for low-latency paths.

5. **Cooperative yield points need atomicity rules.**

   [DESIGN.md §9.3](../DESIGN.md#L1752) lets expensive operators emit partial `EpochOutput` and yield. That is good scheduler hygiene, but partial outputs must not become partially committed epoch state unless the operator state machine is designed for chunked commits. The docs need to specify whether partial `EpochOutput` is an in-memory fragment collected into one epoch commit, or whether it can be durably committed as a sub-epoch with its own frontier semantics.

6. **Tombstone and compaction stalls can become latency cliffs.**

   [DESIGN.md §5.4](../DESIGN.md#L968) acknowledges tombstone density and targeted compaction. The fallback is full compaction of a shard. At 20 GB target shard size, full compaction can be a large I/O event and may interfere with freshness. The spec should define compaction scheduling isolation, maximum compaction debt, and how compaction competes with epoch commits for object-store request budget.

#### Batching vs streaming

- The auto-tuned epoch model is sensible for throughput. [DESIGN.md §9.1](../DESIGN.md#L1730) uses `min_epoch_ms`, `min_epoch_bytes`, and `max_epoch_ms`; [§14.5](../DESIGN.md#L3860) adds adaptive epoch sizing. This will work for high-throughput micro-batching.
- The same model does not yet satisfy the sub-ms/local target. A 10 ms floor in [§14.5](../DESIGN.md#L3860) means local commit-to-view cannot be sub-ms if it waits for epoch closure. The design needs a **nano-batch** or **continuous local epoch** path: process immediately in memory, publish a visible frontier for local reads, and separately advance the durable frontier when SlateDB commits.
- The brownout buffer in [DESIGN.md §11.7](../DESIGN.md#L2085) is bounded by epochs, not bytes. Epochs can become large under adaptive batching, so the bound must include bytes and rows.

### Pillar 4: Ergonomics, Usability, and Day-2 Operations

#### What is strong

- **The SLO-driven configuration model is excellent.** [DESIGN.md §14.3](../DESIGN.md#L3798) lets users declare freshness, memory, and priority rather than shards and antichains.
- **`EXPLAIN INCREMENTAL`, cost preview, and named degraded states are exactly the right UX.** [DESIGN.md §14.8-§14.10](../DESIGN.md#L3943) will make RockStream much easier to operate than a typical streaming system if implemented faithfully.
- **Support bundles and audit logs are first-class.** [DESIGN.md §14.11-§14.12](../DESIGN.md#L4114) are practical and should stay in the critical path.
- **The built-in row generator is a strong first-run choice.** [DESIGN.md §13.5.0](../DESIGN.md#L2997) and [ROADMAP.md v0.10](../ROADMAP.md#L115) remove the usual Kafka/Postgres setup tax.

#### UX and operational risks

1. **The terminology is drifting.**

   The top changelog says v3.25 removed `CREATE PIPELINE` in favor of workloads and materialized views. But [DESIGN.md §14.1](../DESIGN.md#L3756) still says the operator interacts with "Pipelines" and "Views"; [§14.7](../DESIGN.md#L3894) includes workload, view, schema, source, cluster, resource, audit, and debug commands; the control-plane key layout still uses `catalog/pipeline` in [§5.2](../DESIGN.md#L893). The concept may be valid internally, but it needs a clean public/internal split.

2. **Namespace and schema semantics conflict.**

   [DESIGN.md §5.2](../DESIGN.md#L893) says a namespace is like a PostgreSQL database and that there is no schema layer inside a namespace. Yet [§14.3](../DESIGN.md#L3798), [§14.10](../DESIGN.md#L4053), [§14.19](../DESIGN.md#L4318), and the row-generator example use schema-level commands or dotted names like `demo.orders`. This will confuse users and ORMs.

3. **Postgres compatibility risks becoming product sprawl.**

   [DESIGN.md §12.6](../DESIGN.md#L2331) correctly says pgwire is an access layer, not a Postgres clone. But the later additions include direct DML, session read-your-writes, `INSERT RETURNING`, secondary indexes, and optimistic transactions. Each may be justified, but together they pull the product toward HTAP/OLTP complexity. The roadmap should keep IVM freshness as the product wedge and gate OLTP surfaces more aggressively.

4. **Interactive backfill prompts are awkward over pgwire.**

   [DESIGN.md §14.9](../DESIGN.md#L3995) proposes an interactive prompt for expensive `CREATE MATERIALIZED VIEW`. That is great in the CLI, but many SQL drivers cannot handle interactive confirmation inside a SQL statement. The SQL surface should return a structured error/notice with a confirmation token or require `WITH CONFIRMATION_TOKEN = ...` on retry.

5. **Operational profiles need clearer packaging.**

   A self-hosted operator should be able to pick: `embedded`, `single-node-production`, `distributed-minimal`, `distributed-secure`, and `data-lake-enabled`. Right now the docs mix optional features (Iceberg REST catalog, external connector tier, secrets providers) into one very broad mental model.

### Pillar 5: First-Class Observability

#### What is strong

- [DESIGN.md §14.4](../DESIGN.md#L3843) defines a single primary health signal: `view_slo_compliance`.
- [§14.8](../DESIGN.md#L3943) defines human, verbose, analyze, and estimate modes for `EXPLAIN INCREMENTAL`.
- [§14.11-§14.12](../DESIGN.md#L4114) specify audit logs and support bundles.
- [§14.15](../DESIGN.md#L4210) lists core metrics across frontier lag, throughput, state size, shuffle depth, compaction backlog, object-store RPS, cache hit ratio, and historical query counts.
- [§17](../DESIGN.md#L4469) treats deterministic simulation as an observability and correctness instrument, not merely a test harness.

#### Observability gaps

1. **Full observability lands too late in the roadmap.**

   [IMPLEMENTATION_PLAN.md Phase 10](../IMPLEMENTATION_PLAN.md#L1310) and [ROADMAP.md v0.47](../ROADMAP.md#L177) carry much of the metrics/tracing/admin surface. Earlier phases include some operability callouts, but the core hot-path metrics need to exist from the first operator milestones. Otherwise performance and correctness regressions will be invisible until after the architecture is already set.

2. **The metric set needs object-store p99 and write-amplification detail.**

   Add at least:

   - `object_store_request_duration_seconds{op,profile}`
   - `slatedb_manifest_write_duration_seconds`
   - `slatedb_wal_replay_bytes`
   - `slatedb_sst_count{shard}`
   - `write_batch_bytes{shard,kind}`
   - `write_amplification_ratio{shard}`
   - `manual_compaction_duration_seconds`
   - `compaction_debt_seconds`

3. **Watermark observability is underspecified.**

   Time-window correctness requires separate reporting for source offset lag, processing-time lag, event-time watermark lag, late rows, and windows held open by missing watermarks. `connector_late_rows_total` is not enough.

4. **High-cardinality metrics need a policy.**

   Per-view, per-shard, per-operator labels can explode in a 1,000-shard cluster. The design should define which metrics are Prometheus-safe, which are sampled, and which live only in system tables/support bundles.

5. **Tracing needs a causal correlation model.**

   OpenTelemetry is mentioned, but the docs should specify trace IDs across connector batch -> source epoch -> operator epoch -> exchange frame -> write batch -> frontier publish -> sink commit. Without this, distributed traces will not reconstruct a stuck epoch.

6. **Auto-tuner observability should include rejected choices.**

   [DESIGN.md §14.5](../DESIGN.md#L3860) audit-logs tuning decisions. It should also log candidate actions the tuner rejected because of quotas, locality cost, state migration cost, or skew semantics. That is often the fastest path to explaining why a view is degraded.

### Pillar 6: Document Consistency and Alignment

#### DESIGN to IMPLEMENTATION_PLAN alignment

The implementation plan does a good job translating many high-level concepts into phases: MergeLaw first, single-shard IVM before distribution, distributed core before gateway/connectors, deterministic simulation as a foundation, and cold tier deferred behind a decision gate.

However, the plan is not fully aligned in three important ways:

1. **Phase order is inconsistent.** [IMPLEMENTATION_PLAN.md Phase Overview](../IMPLEMENTATION_PLAN.md#L28) maps Phase 8 to v0.40-v0.43 and Phase 9 to v0.44-v0.45. But the detailed body places [Phase 9](../IMPLEMENTATION_PLAN.md#L946) before [Phase 8](../IMPLEMENTATION_PLAN.md#L1037). [ROADMAP.md](../ROADMAP.md#L160) uses the correct version order.
2. **The cold-tier-aware gateway phase is inconsistent.** [DESIGN.md §12.7.3](../DESIGN.md#L2488) says the `ViewReader` cold-tier slot is a Phase 9 obligation. [IMPLEMENTATION_PLAN.md Phase 8](../IMPLEMENTATION_PLAN.md#L1037) and [ROADMAP.md v0.40](../ROADMAP.md#L164) put it in the Postgres gateway phase. The latter is correct; DESIGN.md should be updated.
3. **Window performance is weaker in the plan than the design ambition.** DESIGN.md mentions segment trees in [§6.7](../DESIGN.md#L1219), but IMPLEMENTATION_PLAN starts with partition recomputation in [IVM-7](../IMPLEMENTATION_PLAN.md#L454). That is a fine initial correctness slice, but the plan should explicitly mark it as not meeting low-latency objectives for large partitions.

#### ROADMAP ordering and milestone sizing

The roadmap's sequencing philosophy is excellent, especially [ROADMAP.md principles](../ROADMAP.md#L22), [Common Definition of Done](../ROADMAP.md#L51), and the decision gates in [§Decision Gates](../ROADMAP.md#L223). But several rows violate the roadmap's own "roughly 10 person-weeks" rule:

- **v0.43** combines direct-write DML, multiple CRDT column types, idempotency keys, optimistic transaction metadata, session read-your-writes, `INSERT RETURNING`, max-staleness, and zero-downtime view replacement. That should be split.
- **v0.47** combines metrics, tracing, admin CLI, support bundles, debug arrangement, law diagnostics, actionable errors, resource usage, and schema evolution visibility. That is multiple versions.
- **v0.50** combines rolling upgrades, migration, disaster recovery, security review, and shard column statistics. These deserve separate proof gates.
- **v0.51** combines 30-day soak, 1,000-shard stress, user-defined merge laws, and optimistic transactions. A soak should validate known features; it should not also introduce major new semantics.

The roadmap is patient in spirit, but some late rows compress too much risk.

---

## 3. Inconsistencies & Gaps

This is the definitive list of contradictions or missing details found across the three documents.

1. **Implementation Phase 8/9 ordering is wrong in the body.** The overview and roadmap place Phase 8 before Phase 9, but the detailed plan places Phase 9 first. See [IMPLEMENTATION_PLAN.md Phase Overview](../IMPLEMENTATION_PLAN.md#L28), [Phase 9](../IMPLEMENTATION_PLAN.md#L946), and [Phase 8](../IMPLEMENTATION_PLAN.md#L1037).
2. **Cold-tier-aware gateway phase mismatch.** DESIGN.md says Phase 9; implementation and roadmap put it at v0.40/Phase 8. See [DESIGN.md §12.7.3](../DESIGN.md#L2488), [IMPLEMENTATION_PLAN.md Phase 8](../IMPLEMENTATION_PLAN.md#L1037), and [ROADMAP.md v0.40](../ROADMAP.md#L164).
3. **Pipeline terminology remains after v3.25 claims it was removed.** `pipeline` remains in control keys, system tables, CLI text, metrics, and operator mental model. See [DESIGN.md §5.2](../DESIGN.md#L893), [§12.6.1](../DESIGN.md#L2369), [§14.1](../DESIGN.md#L3756), and [§14.7](../DESIGN.md#L3894).
4. **Namespace is defined as PostgreSQL database, but schema commands remain.** DESIGN.md says there is no schema layer inside a namespace, yet commands use `ALTER SCHEMA`, `SHOW ... FOR SCHEMA`, schema defaults, and dotted names like `demo.orders`. See [DESIGN.md §5.2](../DESIGN.md#L893), [§13.5.0](../DESIGN.md#L2997), [§14.3](../DESIGN.md#L3798), and [§14.10](../DESIGN.md#L4053).
5. **Error code collisions exist.** `RS-4001` is quota violation and cold-tier not enabled; `RS-5002` is protocol version unsupported and unknown merge law; `RS-6001` is schema incompatible evolution and deferred data-quality failure. See [DESIGN.md §14.14](../DESIGN.md#L4168) and [IMPLEMENTATION_PLAN.md Phase 8](../IMPLEMENTATION_PLAN.md#L1037).
6. **`CREATE VIEW` retention language conflicts with inline-view semantics.** [DESIGN.md §4.3](../DESIGN.md#L770) says `CREATE VIEW` is inline and has no storage. [§5.7](../DESIGN.md#L1085) mentions a "VIEW declared incremental for streaming consumers only," and [IMPLEMENTATION_PLAN.md Phase 9](../IMPLEMENTATION_PLAN.md#L946) says `CREATE VIEW WITH (retention = '7d')`. That should be `CREATE MATERIALIZED VIEW ... WITH (CHANGE_RETENTION=...)` or a separate `SUBSCRIBE` retention setting.
7. **`FreshnessToken` is scalar, but views may depend on many sources.** [DESIGN.md §12.4](../DESIGN.md#L2233) defines `{ source_id, source_epoch, cluster_frontier_hash }`. A materialized view can depend on multiple connectors and upstream views; the token needs a vector/map or a committed frontier reference.
8. **Source epoch language conflicts.** [DESIGN.md §8.1.1](../DESIGN.md#L1481) says `source_epoch` is a connector-declared monotonic epoch with an offset map. [§13.1](../DESIGN.md#L2768) says a connector assigns source epoch by packing its native offset into `source_epoch`. These are different models.
9. **Watermark absence is correct but not operationally bounded.** [DESIGN.md §13.3](../DESIGN.md#L2787) says no watermark means event-time frontier never advances. The spec lacks a DDL-time fail-closed behavior or bounded processing-time fallback.
10. **Shard partitioning is ambiguous.** Rendezvous hashing in [§7.1](../DESIGN.md#L1365) and midpoint key-range splitting in [§10.6](../DESIGN.md#L1869) cannot both be the primary migration model without an intermediate virtual bucket abstraction.
11. **Recovery budget lacks the variables that make it true.** [DESIGN.md §11.5](../DESIGN.md#L2024) states time budgets but not max WAL bytes, manifest chain length, SST count, cache-warm requirements, or object-store p99 assumptions.
12. **Object-store brownout buffer is epoch-bounded, not byte-bounded.** [DESIGN.md §11.7](../DESIGN.md#L2085) must include byte and row limits to satisfy [ROADMAP.md bounded-everything](../ROADMAP.md#L51).
13. **`rockstream` vs `rockstream_catalog` system schemas diverge.** [DESIGN.md §12.6.1](../DESIGN.md#L2369) defines a `rockstream` schema; [§13.3.1](../DESIGN.md#L2920) and [§14.19](../DESIGN.md#L4318) use `rockstream_catalog`. Pick one or define the split.
14. **Cold-tier section numbering is inconsistent.** Implementation and roadmap refer to `§13.6.6` for cold snapshot GC, but DESIGN.md has [§13.6.2.1](../DESIGN.md#L3192).
15. **The comparison table risks overclaiming before Data Lake GA.** [DESIGN.md §15](../DESIGN.md#L4355) mitigates this with a GA vs Data Lake GA note, which is good. The roadmap and marketing-facing docs must preserve that distinction.
16. **Window implementation plan does not meet stated latency ambition.** Partition recomputation in [IMPLEMENTATION_PLAN.md IVM-7](../IMPLEMENTATION_PLAN.md#L454) is not compatible with large-partition low-latency windows.
17. **Hot-path observability starts too late.** [ROADMAP.md v0.47](../ROADMAP.md#L177) bundles most observability, while performance-sensitive operator milestones begin at v0.5-v0.9.

---

## 4. Actionable Architectural Innovations

These are concrete patterns that would improve performance, simplify coordination, or sharpen the user experience.

### 4.1 Dual frontier model: visible frontier and durable frontier

Introduce two explicitly named frontiers per view:

- `visible_frontier`: data processed into the local in-memory arrangement cache and queryable in embedded/single-worker mode.
- `durable_frontier`: data committed to SlateDB and safe for replay, checkpoint, external sink commit, and cross-worker recovery.

For strict exactly-once external effects, only `durable_frontier` counts. For local development and ultra-low-latency reads, `visible_frontier` can provide sub-ms read-your-writes. This avoids pretending S3-backed durability can be sub-ms while still giving a great local path.

### 4.2 Hot arrangement cache with RCU snapshots

Keep a bounded in-memory cache per operator instance for hot arrangements:

- Deltas apply to an epoch-local mutable layer.
- Readers pin an immutable RCU snapshot of the last published visible layer.
- SlateDB persists the delta log/merge operands and periodically folds cold layers.
- Recovery reconstructs from SlateDB; normal hot path avoids read-after-merge.

This is especially valuable for aggregates, joins, and session read-your-writes.

### 4.3 Fixed virtual buckets as the universal migration unit

Replace ambiguous range splitting with:

```text
key -> hash -> virtual_bucket_id -> physical_shard_id -> worker_id
```

Use many more virtual buckets than physical shards. Move buckets during rebalancing; split hot logical keys by allocating sub-buckets; persist bucket ownership in the shard map. This makes migration small, idempotent, and explainable.

### 4.4 Lease-coupled migration protocol

Define a reusable state machine for shard/bucket movement:

```text
PLANNED
SNAPSHOTTING
COPYING
DUAL_WRITING
CATCHING_UP
FENCING_OLD
CUTOVER
VERIFYING
GC_ELIGIBLE
DONE
```

Every transition writes an audit event and is replayable after crash. During `DUAL_WRITING`, new writes go to both old and new owners with idempotent epoch keys. During `CUTOVER`, readers pin to the old owner until the published shard-map epoch changes.

### 4.5 Heavy-hitter sketches for skew

Add Count-Min Sketch or SpaceSaving summaries at exchange senders and aggregators. Feed them into the planner and auto-tuner:

- detect hot keys before p99 latency collapses;
- choose aggregate salting, join replication, or `SKEW_BOUND`;
- show the top hot keys in `EXPLAIN INCREMENTAL ANALYZE` with redaction support.

### 4.6 Partition-vector watermarks and freshness tokens

Represent source progress as a vector map:

```rust
FreshnessToken {
    frontier_id: FrontierId,
    sources: BTreeMap<SourceId, SourceProgress>,
    cluster_frontier_hash: Hash,
}

SourceProgress {
    source_epoch: u64,
    offsets: OffsetToken,
    event_time_watermark: Option<EventTimeWatermark>,
}
```

Gateways can hide the vector from casual users, but internally this solves multi-source views, multi-partition sources, and read-your-writes across view DAGs.

### 4.7 Latency classes in the planner

Have the planner classify each view and query path:

| Class | Example | Contract |
|---|---|---|
| `local_visible` | embedded direct write -> local view read | sub-ms to low-ms, not externally durable until durable frontier advances |
| `local_durable` | local SlateDB commit -> view read | low-ms to tens-ms depending on filesystem |
| `distributed_fresh` | Kafka/Postgres -> distributed view | 10-250 ms target |
| `distributed_exact` | sink 2PC/checkpoint committed | checkpoint bounded |
| `analytical_cold` | Iceberg full scan | seconds, throughput optimized |

`EXPLAIN ESTIMATE` should print this class explicitly.

### 4.8 Spec linting

Add a simple CI linter over Markdown specs before design freeze:

- duplicate `RS-XXXX` codes fail;
- nonexistent section references fail;
- forbidden user-facing terms (`pipeline`, if truly removed) fail outside internal sections;
- phase/version map mismatches fail;
- headings referenced by roadmap/implementation must exist.

This will prevent the current drift from compounding.

### 4.9 A minimal production profile

Define a `production_minimal` deployment profile separate from the full data-lake/HTAP surface:

- control group, workers, gateway, object storage;
- Kafka/Postgres connectors;
- no Iceberg REST catalog;
- no user-defined merge laws;
- auth/secrets on;
- core observability on.

This makes day-2 operations much less intimidating and gives pilots a narrower blast radius.

---

## 5. Spec Refinement Proposals

The following are direct Markdown changes that can be applied back to the specs.

### 5.1 DESIGN.md: add latency classes near §3.1 or §9.1

```markdown
### Latency Classes and Frontier Semantics

RockStream exposes different latency contracts for different execution paths:

| Class | Applies to | Target | Frontier used |
|---|---|---:|---|
| `local_visible` | embedded direct writes and in-process reads | sub-ms to low-ms | visible frontier |
| `local_durable` | local filesystem SlateDB commit-to-read | low-ms | durable frontier |
| `distributed_fresh` | distributed source-to-view freshness | 10-250 ms | durable published vector frontier |
| `distributed_exact_sink` | external sink exactly-once visibility | checkpoint bounded | checkpoint frontier |
| `analytical_cold` | Iceberg/Delta full scans | seconds | cold snapshot epoch |

The visible frontier may advance before the durable frontier only in runtime
profiles that explicitly allow it. External sinks, recovery, checkpointing,
and cross-worker reads use only the durable frontier. `EXPLAIN INCREMENTAL
ESTIMATE` prints the latency class for each view and query path.
```

### 5.2 DESIGN.md §6.4: replace CDC row identity rule

Replace the current CDC identity sentence with:

```markdown
For CDC sources with a declared primary key, `row_id` is derived from
`(source_id, table_id, primary_key_bytes)` and is stable across all versions of
that logical row. The source LSN/offset is stored separately as version metadata
and never participates in arrangement identity. An update is represented as a
retraction of the previous row value and insertion of the new row value under
the same `row_id`, allowing joins, windows, indexes, and cold-tier merge to
remove or replace the old version exactly.

For keyless CDC or snapshots, the connector must either provide a stable
identity column, declare the source append-only, or use a content-derived row
fingerprint with collision verification. Keyless mutable sources without a
stable identity are rejected for retraction-capable materialized views.
```

### 5.3 DESIGN.md §12.7.1: replace cold/hot dedup wording

```markdown
The hot/cold merge is a versioned Z-set merge, not a blind union. The cold
snapshot contributes the materialized Z-set at `snapshot_epoch`. The hot LSM
tail contributes signed deltas for epochs greater than `snapshot_epoch`. The
gateway groups by stable `row_id`, applies weights in epoch order, drops rows
whose final weight is zero, and returns the latest visible row version. Simple
`row_id` deduplication is valid only for insert-only views whose root operator
proves monotonicity.
```

### 5.4 DESIGN.md §7/§10: introduce virtual buckets

```markdown
### Virtual Buckets

The partitioning unit is a virtual bucket, not a physical shard. A partition
key is hashed to one of `V` virtual buckets (`V` is much larger than the number
of physical shards). The shard map assigns virtual buckets to physical shards,
and physical shards to workers. Online split, merge, drain, and hot-key salting
move virtual buckets between physical shards. This avoids depending on
contiguous key ranges under rendezvous hashing and makes state movement bounded
and auditable.
```

### 5.5 DESIGN.md §10.2: add migration state machine

```markdown
Online movement of a virtual bucket uses a durable migration state machine:
`PLANNED -> SNAPSHOTTING -> COPYING -> DUAL_WRITING -> CATCHING_UP ->
FENCING_OLD -> CUTOVER -> VERIFYING -> GC_ELIGIBLE -> DONE`. The migration
record is stored in the control SlateDB and every transition is idempotent.
Readers continue using the old owner until the shard-map epoch reaches
`CUTOVER`. During `DUAL_WRITING`, writes are applied to both owners with the
same idempotency keys. Cleanup of the old owner is forbidden until the bucket's
consumer frontier passes the cutover epoch.
```

### 5.6 DESIGN.md §6.9/§13.3: fail closed for missing watermarks

```markdown
A time-window operator over event time requires a watermark policy. If a source
connector cannot emit `EventTimeWatermark`, `CREATE MATERIALIZED VIEW` must
specify one of:

- `WATERMARK = PROCESSING_TIME` - use processing-time windows; correctness is
  processing-time only.
- `WATERMARK = EXTERNAL '<source>'` - use an external watermark source.
- `WATERMARK = NONE` - keep windows open until retention/DDL closes them;
  requires an explicit state budget acknowledgement.

If no policy is specified and the connector returns `watermark = None`, the DDL
is rejected with `RS-10xx connector.watermark_required`.
```

### 5.7 DESIGN.md §12.4: make freshness tokens vector-based

````markdown
A `FreshnessToken` identifies a committed vector frontier, not a single scalar
source position:

```rust
FreshnessToken {
    token_id: FreshnessTokenId,
    source_progress: BTreeMap<SourceId, SourceProgress>,
    cluster_frontier_hash: Hash,
}
```

The gateway may render this opaquely to clients, but internally `wait_for`
checks that the published vector frontier dominates every source progress entry
in the token. This supports materialized views with multiple sources and
view-on-view dependencies.
````

### 5.8 DESIGN.md §5.2/§14: fix namespace vs schema terminology

Choose one of these two options.

**Option A: keep namespace as database and remove schema commands.**

```markdown
RockStream has namespaces, not PostgreSQL schemas. User-facing commands use
`NAMESPACE` consistently: `ALTER NAMESPACE ... PAUSE`, `SHOW VIEW STATUS FOR
NAMESPACE <name>`, and `ALTER NAMESPACE ... SET DEFAULT WORKLOAD`. Dotted names
such as `demo.orders` mean `<namespace>.<object>` only in cross-namespace admin
commands; normal SQL connections target exactly one namespace.
```

**Option B: add a real schema layer.**

```markdown
A namespace is the PostgreSQL database equivalent. Inside a namespace,
RockStream supports lightweight SQL schemas for object naming and defaults.
Catalog keys include `(namespace_id, schema_id, object_id)`. Cross-schema
queries inside one namespace are allowed; cross-namespace queries are not.
```

Option A is simpler and better aligned with the current storage key design.

### 5.9 IMPLEMENTATION_PLAN.md: move Phase 8 before Phase 9

Move the entire `## Phase 8 - Query Gateway & Postgres Compatibility` block before `## Phase 9 - Connectors & Sinks`. Keep the phase overview unchanged. This makes the detailed body match [ROADMAP.md](../ROADMAP.md#L160) and the v0.40-v0.45 sequence.

### 5.10 DESIGN.md §12.7.3: change Phase 9 to Phase 8

Replace:

```markdown
In Phase 9, only `HotOnly` is implemented.
```

with:

```markdown
In Phase 8 / v0.40, only `HotOnly` is implemented. The `ViewReadStrategy` enum
and the `ViewReader` trait are defined in full so that the cold-tier
implementation can be added in Phase 12 without changing the gateway planner.
```

### 5.11 DESIGN.md §14.14: enforce unique error-code ranges

```markdown
Error codes are globally unique. CI fails if two registry entries share the
same `RS-XXXX` value or if a code appears in documentation without a registry
entry. Ranges are reserved as follows:

| Range | Owner |
|---|---|
| RS-1000-RS-1999 | connectors and source decoding |
| RS-2000-RS-2499 | query gateway, history, isolation, session behavior |
| RS-3000-RS-3499 | shard, storage, recovery, merge operand validation |
| RS-4000-RS-4499 | control plane, quotas, cold-tier planning |
| RS-5000-RS-5499 | format, protocol, upgrade compatibility |
| RS-6000-RS-6499 | schema evolution and data-quality extensions |
```

Then reassign the currently colliding coordinator and cold-tier codes into free ranges.

### 5.12 ROADMAP.md: split overloaded late versions

Recommended splits:

- Split v0.43 into:
  - v0.43a: direct DML and session read-your-writes;
  - v0.43b: first CRDT column types and idempotency keys;
  - v0.43c: `INSERT RETURNING` and max-staleness;
  - v0.43d: zero-downtime view replacement.
- Split v0.47 into:
  - observability metrics/tracing;
  - admin CLI/support bundle;
  - debug arrangement/law diagnostics;
  - resource usage and schema evolution visibility.
- Split v0.50 into:
  - rolling upgrade and migration;
  - disaster recovery drill;
  - shard column statistics;
  - independent security review.
- Move `CREATE MERGE LAW` out of v0.51 soak unless the soak is validating an already implemented feature.

### 5.13 DESIGN.md §14.15: add missing core metrics

```markdown
Additional required hot-path metrics:

- `object_store_request_duration_seconds{op,profile}`
- `slatedb_manifest_write_duration_seconds`
- `slatedb_wal_replay_bytes`
- `slatedb_sst_count`
- `write_batch_bytes{kind}`
- `write_amplification_ratio`
- `compaction_debt_seconds`
- `event_time_watermark_lag_ms`
- `windows_open_total`
- `windows_held_without_watermark_total`
- `migration_state_duration_seconds{state}`
- `visible_frontier_lag_ms`
- `durable_frontier_lag_ms`
```

### 5.14 ROADMAP.md: add a design-freeze prerequisite checklist at v0.10

```markdown
Before the v0.10 design freeze, the following spec hygiene checks must pass:

- no duplicate `RS-XXXX` codes;
- no nonexistent section references;
- public terminology is consistent (`workload`, `view`, `source`, `namespace`);
- phase/version maps match across DESIGN.md, IMPLEMENTATION_PLAN.md, and ROADMAP.md;
- every latency claim names its latency class;
- every unbounded queue, buffer, scan, replay, and migration has byte/row/time bounds.
```

---

## Closing Assessment

RockStream is aiming at the right target with unusually good instincts: DBSP semantics, object-store-aware state, deterministic simulation, and humane operations. The design should not be made smaller in ambition, but it should become sharper in contracts. The next spec revision should focus less on adding features and more on closing the semantic and operational gaps above.

The highest-leverage refinements are: fix stable identity, define vector freshness, introduce virtual buckets, specify migration states, split visible vs durable frontiers, and run a spec linter for terminology and error-code drift. Those changes would make the current vision much more credible as the foundation for a world-class cloud-native IVM engine.

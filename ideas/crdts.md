# CRDTs and Merge Laws in RockStream

Status: accepted strategy, effective v0.5

Date: 2026-05-28 (revised after v0.4.0 release)

Audience: implementers of `rockstream-types`, `rockstream-storage`,
`rockstream-plan`, `rockstream-diff`, `rockstream-ops`, `rockstream-runtime`,
`rockstream-gateway`, and `rockstream-connectors`.

## 1. Executive Summary

Starting at v0.5, RockStream treats **merge laws** (commutative monoids,
join semilattices, and operation-CRDTs) as a first-class, database-wide
concept. Every algebraic state in the system — Z-set weights, partial
aggregates, watermarks, distinct/union, monotone recursion, and later
user-visible CRDT columns — is described by an entry in a single shared
**MergeLaw catalog** that storage, planner, exchange, frontier, gateway,
connectors, compaction, and `EXPLAIN INCREMENTAL` all consume.

Concretely:

- **v0.5** lands the `MergeLaw` / `LawBundle` types in `rockstream-types`
  plus the shared property-test harness. The Z-set algebra is the first
  law (`WeightAdd/v1`).
- **v0.6–v0.10** wire the law contract through the single-shard IVM core:
  algebraic aggregates, merge-backed reads, frontier-aware compaction,
  and `EXPLAIN INCREMENTAL`.
- **v0.11–v0.27** propagate laws through the SQL planner, distribution
  pass, advanced operators (windows, recursion, Top-K subcomponents),
  and the IVM correctness soak.
- **v0.28–v0.36** make exchange combining, hierarchical exchange, and
  cross-shard recovery merge-law driven; the v0.36 chaos alpha is the
  first proof that the contract holds end-to-end.
- **v0.37–v0.45** expose built-in CRDT column types at the SQL surface
  (`COUNTER`, `MAX_REGISTER`, `LWW`, `G_SET`, `OR_SET`, `HLL`,
  `BLOOM_UNION`) and let connectors advertise merge-law columns.
- **v0.46–v0.54** finish observability, secrets-aware audit, cold-tier
  persistence of law metadata, and the user-defined CRDT registration
  surface (`CREATE MERGE LAW`).

The non-goal that does not change: active-active multi-region writes are
out of scope through 1.0. The MergeLaw contract is designed so they could
be added later without breaking exact-SQL semantics, but the project does
not promise them.

## 2. Why This Fits RockStream

RockStream is already operating in CRDT territory:

- Per-shard SlateDB instances with associative merge operators.
- Causal vector frontiers instead of a global LSN.
- Z-set deltas with signed weights and zero-crossing semantics.
- Pre-shuffle combiners on the exchange path.
- Single-writer-per-shard fencing via the SlateDB manifest.

CRDT theory is the precise vocabulary for *when independent updates can
be merged without coordination*. Every line of that vocabulary already
shows up in the design — we just have not named it consistently. Naming
it once, in a shared place, removes a recurring source of drift between
storage, planner, exchange, and gateway.

The crucial nuance: **not every associative merge is a CRDT.** Integer
addition is associative and commutative but not idempotent. Replaying
`+1` twice changes the value. RockStream's exactly-once epoch envelope
lets us use non-idempotent monoids safely; we must not pretend they are
replay-safe in general.

## 3. Precise Vocabulary

The codebase, docs, and `EXPLAIN` output must use these terms.

| Term | Required laws | Replay-safe? | Examples | Where used |
|---|---|---:|---|---|
| Commutative monoid | associative, commutative, identity | No | SUM, COUNT, weight addition | Fast IVM under exactly-once epochs |
| Group (invertible monoid) | monoid + inverse | No | signed Z-set weights, SUM with retractions | Insert/delete deltas |
| Join semilattice CRDT | assoc., comm., **idempotent** | Yes | max-register, G-Set, HLL | Replay-tolerant merge, watermarks |
| Operation CRDT | ops commute under unique op IDs and causal rules | Yes, with dedupe | OR-Set, PN-Counter | User write surfaces |
| Retraction-aware arrangement | not a pure merge law | Implementation-dependent | MIN/MAX with deletes, Top-K, windows | Explicit sorted state |

The split matters because exactly-once buys us non-idempotent monoids
for high-throughput IVM, but it does **not** make them replay-safe
outside the epoch envelope (e.g. duplicate connector deliveries before
the dedupe layer, or hypothetical multi-region replication).

## 4. The Contract: `LawBundle`

A merge law is more than metadata — it is the bundle of code and rules
that all consumers share. The shared type lives in `rockstream-types`:

```rust
pub struct MergeLaw {
    pub id: MergeLawId,         // stable u32, allocated in catalog
    pub name: &'static str,     // "WeightAdd", "SumCount", "MaxRegister", ...
    pub version: u16,           // bumped on any operand-encoding change
    pub class: MergeLawClass,   // CommutativeMonoid | Group | JoinSemilattice | OperationCrdt
    pub properties: LawProperties,
}

pub struct LawProperties {
    pub associative: bool,      // always true for a registered law
    pub commutative: bool,
    pub idempotent: bool,
    pub invertible: bool,
    pub monotone: bool,         // join-only, never retracts
    pub duplicate_policy: DuplicatePolicy,   // RequireExactlyOnce | DedupeByOpId | Idempotent
    pub compaction_policy: CompactionPolicy, // FrontierFold | TombstoneGc | NeverFold
    pub frontier_policy: FrontierPolicy,     // ExactOnly | MonotonePartialAllowed
}

pub struct LawBundle {
    pub law: MergeLaw,
    pub encoder: Arc<dyn LawEncoder>,            // value <-> bytes
    pub merge_fn: Arc<dyn LawMergeFn>,           // SlateDB MergeOperator implementation
    pub compaction: Arc<dyn LawCompactionFilter>,// frontier-aware compaction filter
    pub gateway_combiner: Option<Arc<dyn LawGatewayCombiner>>, // partial-pushdown re-merge
    pub explain: Arc<dyn LawExplain>,            // string rendering for EXPLAIN
}
```

The catalog is a single registry, owned by `rockstream-types`:

```rust
pub fn register(bundle: LawBundle) -> Result<(), RegisterError>;
pub fn get(id: MergeLawId) -> Option<&'static LawBundle>;
pub fn get_by_name(name: &str, version: u16) -> Option<&'static LawBundle>;
```

Registration is process-startup-only and panics on conflict (a law-ID
collision is a bug, not a runtime condition).

### 4.1 What every consumer asks of a `LawBundle`

| Consumer | Question | Method |
|---|---|---|
| `rockstream-storage` | "How do I merge two encoded operands?" | `merge_fn.merge(base, ops)` |
| `rockstream-storage` | "Can compaction fold these operands?" | `compaction.fold(frontier, base, ops)` |
| `rockstream-plan` | "Can I insert a pre-shuffle combiner here?" | `law.properties.associative && law.properties.commutative` |
| `rockstream-plan` | "Can I push partial aggregation to shards?" | `bundle.gateway_combiner.is_some()` |
| `rockstream-plan` | "Is this operator monotone?" | `law.properties.monotone` |
| `rockstream-runtime` | "Are duplicates safe?" | `law.properties.duplicate_policy` |
| `rockstream-gateway` | "How do I re-merge per-shard partials?" | `bundle.gateway_combiner.unwrap().combine(rows)` |
| `rockstream-connectors` | "Which schema columns advertise this law?" | `LawSchemaMetadata` (Phase 9) |
| `EXPLAIN INCREMENTAL` | "What do I print?" | `bundle.explain.render(...)` |
| Observability | "Which counters increment?" | `merge_law_*_total{law_id, law_name, law_version}` |

### 4.2 Law versioning and storage upgrade

Persisted arrangement bytes outlive the running code. The contract is:

- **`law.id` is forever.** Once allocated, a `MergeLawId` is never
  recycled. The `MergeLawId` reservation table lives next to the
  storage-format-version table (`DESIGN.md §5.5`).
- **`law.version` bumps for any operand-encoding change.** Old versions
  remain registered; the bundle for `(id, old_version)` must continue to
  decode existing operands and merge them with new ones.
- **Arrangement headers store `(law_id, law_version)`.** A shard
  attaching to existing state reads the header and looks up the bundle.
  If the bundle is missing, the shard refuses to mount with
  `RS-5002 unknown merge law`.
- **Plan persistence stores `(law_id, law_version)`** alongside the
  physical plan. Replaying a plan after a binary upgrade re-resolves
  the bundle. Incompatible upgrades trigger a blue/green plan
  replacement (Phase 7 v0.39).
- **Compaction never folds across a version boundary** unless the law
  declares `compatible_across_versions(prev, curr) == true`.

## 5. Highest-Value Applications

### 5.1 IVM state (v0.5–v0.10)

This is the first and largest win. Today the planner already wants merge
behaviour for SUM, COUNT, AVG, DISTINCT/UNION. Naming the laws lets the
engine:

- avoid read-modify-write on update;
- shrink epoch `WriteBatch` size;
- let compaction fold operands when the checkpoint frontier proves no
  active reader can see the unmerged form;
- fall back to read-modify-write deterministically when a storage
  profile cannot resolve merge operands on the read path (and report
  the fallback in `EXPLAIN`).

MIN/MAX, Top-K, sliding windows, and recursive DRed state stay as
explicit arrangements. They may have *merge-safe subcomponents* (e.g. a
max-register for the cached extremum), but the operator as a whole is
not a pure CRDT.

### 5.2 Exchange and shuffle reduction (v0.30, v0.31)

Pre-shuffle combining becomes generic over `LawBundle` instead of the
v0.4 hand-coded SUM/COUNT/AVG allowlist:

- The planner annotates each `Exchange` node with the law of its
  payload (or `not_merge_safe_reason`).
- The exchange combiner combines by `(target_shard, key, law_id)` and
  emits one compact batch per target.
- Hierarchical exchange (`DESIGN.md §7.5`) re-applies the combiner at
  each domain boundary.
- The combiner has a single equivalence obligation: combined output
  must equal uncombined output after the receiver applies the same
  merge. This is checked as a randomized property in CI for every law.

### 5.3 Planner and optimizer (v0.11–v0.18)

The planner is where laws become a database-wide property instead of a
storage-local trick. The SQL lowering pass propagates a `MergeLawId` (or
`not_merge_safe_reason: &'static str`) through every `PlanNode` and
`OpNode`. Concretely:

| SQL construct | Law | Notes |
|---|---|---|
| `SUM(v)` | `SumCount/v1` | Non-idempotent; exactly-once required. |
| `COUNT(*)` | `SumCount/v1` (count slot) | Non-idempotent. |
| `AVG(v)` | `SumCount/v1` + finalize | Pair `(sum, count)`; finalize on read. |
| `DISTINCT`, `UNION` | `WeightAdd/v1` | Signed weights; zero-crossing emit. |
| `EXCEPT`, `INTERSECT` | `WeightAdd/v1` + min-clamp | Min-clamp is not a pure law; see §6. |
| `MAX(event_time)` as watermark | `MaxRegister/v1` | Monotone, idempotent. |
| `WITH RECURSIVE` insert-only | `WeightAdd/v1`, monotone | Enables partial-progress publication. |
| `APPROX_COUNT_DISTINCT(v)` | `HyperLogLog/v1` | Idempotent sketch union (Phase 3). |

The planner uses laws for: combiner insertion, partial-aggregation
pushdown, monotone classification, storage-encoding choice, compaction
safety, and `EXPLAIN INCREMENTAL` annotations.

### 5.4 Frontiers and freshness (v0.32)

Laws do not replace frontiers. Exact SQL reads still require a pinned
vector frontier so the gateway can name the snapshot. What laws add:

- monotone, idempotent operators (e.g. insert-only recursion, watermark
  registers) can publish per-shard partial progress with a
  `complete_through` token before all shards reach the same frontier;
- exchange GC can use law-specific safety rules;
- the gateway can render "waiting for completeness" vs. "waiting because
  this operator is not merge-safe" as distinct degraded reasons.

For user-facing SQL the default stays exact. Monotone partial reads are
an opt-in flag (`SELECT ... AS OF MONOTONE PARTIAL`) added in v0.42
alongside the existing `AS OF EPOCH` / `AS OF TIMESTAMP` historical
modes.

### 5.5 Connectors and direct writes (v0.43–v0.45)

Built-in delta forms become a connector and DML primitive:

- DDL: `CREATE TABLE balances (account TEXT PRIMARY KEY, amount COUNTER)`
  declares an algebraic column.
- DML: `UPDATE balances SET amount = amount + 1 WHERE account = $1`
  rewrites to a `LawBundle::merge_fn`-friendly delta.
- Connector schema: `discover_schema()` may advertise CRDT columns; the
  gateway validates that the declared law has passed every earlier
  phase before accepting.
- Non-idempotent laws require an idempotency key or exact-once source
  offsets; the gateway rejects writes that supply neither with
  `RS-2007`.

Active-active multi-region is still out of scope: a `COUNTER` column
inside one cluster is a delta primitive, not a replication contract.

### 5.6 Compaction, retention, GC (v0.6, v0.32, v0.47)

Compaction policy is part of every law:

| Law | Compaction policy | What it means |
|---|---|---|
| `WeightAdd/v1` | `FrontierFold` | Fold operands into base value when no snapshot needs them; remove zero-weight rows only when invisibility plus snapshot safety are proven. |
| `SumCount/v1` | `FrontierFold` | Fold `(Δsum, Δcount)` operands into the base pair. |
| `MaxRegister/v1` | `FrontierFold` | Drop strictly-dominated candidates once the winner is stable for all readers. |
| `LWWRegister/v1` | `FrontierFold` | Keep only the timestamp-winning value. |
| `GSet/v1` | `FrontierFold` | Encode many adds into a single compact set segment. |
| `ORSet/v1` | `TombstoneGc` | Drop remove-tombstones only after causal stability proves no old add can reappear. |
| `HyperLogLog/v1` | `FrontierFold` | Fold register-wise max into the base sketch. |
| `BloomUnion/v1` | `FrontierFold` | Bitwise OR into the base bitmap. |

Compaction without a law has to be conservative. Compaction with a law
can be aggressive and still correct.

### 5.7 Gateway reads (v0.41)

The cross-shard partial-aggregation pushdown already planned for v0.41
becomes a `gateway_combiner` consumer. The gateway pushes the partial
form, receives `O(groups)` rows per shard, and re-merges using the same
`LawBundle`. Approximate aggregates (`APPROX_COUNT_DISTINCT`) get the
same path automatically because their sketch union is already a
registered law.

### 5.8 Observability (v0.47)

Every law-aware path emits:

- `merge_law_applied_total{law_id, law_name, law_version}`
- `merge_law_fallback_total{law_id, reason}` (e.g. `read_path_unsupported`)
- `merge_law_compaction_bytes_reclaimed{law_id}`
- `exchange_combiner_input_bytes`, `exchange_combiner_output_bytes`
- `crdt_duplicate_dropped_total{law_id, source}`
- `crdt_tombstone_bytes{law_id}`
- `view_monotone_partial_lag_ms{view, law_id}`

`EXPLAIN INCREMENTAL` prints, for every law-bearing operator:

```
merge_law=SumCount/v1 class=commutative_monoid idempotent=false
duplicate_policy=require_exactly_once compaction=frontier_fold
combiner=enabled partial_pushdown=enabled
```

When a fragment cannot use a law, `EXPLAIN` prints:

```
merge_law=none not_merge_safe_reason=<reason>
```

The set of `not_merge_safe_reason` strings is closed and listed in
`rockstream-types`.

## 6. Built-in Catalog

Built-ins land in this order. Every entry pins its tag byte; tag bytes
are reserved here and never recycled.

| Law | ID | Tag | Class | Lands in | User-visible in |
|---|---:|:---:|---|---|---|
| `WeightAdd/v1` | 0x0001 | 0x10 | Group | v0.5 | n/a (engine) |
| `SumCount/v1` | 0x0002 | 0x01..=0x02 | Commutative monoid | v0.7 | aggregate output |
| `MaxRegister/v1` | 0x0003 | 0x20 | Join semilattice | v0.20 | watermark internal, `MAX_REGISTER` column v0.43 |
| `MinRegister/v1` | 0x0004 | 0x21 | Join semilattice (dual) | v0.20 | `MIN_REGISTER` column v0.43 |
| `LWWRegister/v1` | 0x0005 | 0x22 | Join semilattice | v0.43 | `LWW` column v0.43 |
| `PNCounter/v1` | 0x0006 | 0x30 | Operation CRDT | v0.43 | `COUNTER` column v0.43 |
| `GSet/v1` | 0x0007 | 0x40 | Join semilattice | v0.43 | `G_SET` column v0.43 |
| `ORSet/v1` | 0x0008 | 0x41 | Operation CRDT | v0.44 | `OR_SET` column v0.44 |
| `HyperLogLog/v1` | 0x0009 | 0x50 | Join semilattice (sketch) | v0.21 | `APPROX_COUNT_DISTINCT` v0.25 |
| `BloomUnion/v1` | 0x000a | 0x51 | Join semilattice (sketch) | v0.25 | `APPROX_MEMBERSHIP` v0.25 |

User-defined laws via `CREATE MERGE LAW` land in v0.50 behind the
existing built-in catalog. They are gated on:

- a registered encoder/decoder pair;
- a passing shared property-test suite (associativity, commutativity if
  declared, idempotence if declared, identity, serialization round-trip,
  determinism, fail-closed on malformed operands);
- a declared `duplicate_policy` and `compaction_policy`;
- a registered `EXPLAIN` formatter.

Exact MIN/MAX with retractions, exact Top-K with deletes, window
ranking, and recursive DRed state remain explicit arrangements. They
may use a registered law for a *cached subcomponent* (e.g. the
`MaxRegister/v1` slot inside `MIN/MAX` to short-circuit reads), but the
operator as a whole is not a CRDT.

## 7. Testing Strategy

### 7.1 Shared law-property suite (every law)

- Associativity: `(a · b) · c == a · (b · c)`.
- Commutativity (where declared): `a · b == b · a`.
- Idempotence (where declared): `a · a == a`.
- Identity: `a · identity == a`.
- Inverse (where invertible).
- Serialization round-trip and determinism.
- Version compatibility for every `(version_old, version_new)` pair the
  law claims to support.
- Fail-closed on malformed operands: corrupt one byte, expect
  `RS-3009 malformed merge operand`.

### 7.2 Distributed / `SimRuntime` suite

Every law contributes seeded `SimRuntime` tests for:

- random reorder of merge operands within an epoch;
- duplicate replay (must be safe iff `idempotent`);
- duplicate rejection (when `duplicate_policy = RequireExactlyOnce`
  and idempotency keys exist);
- crash and replay across the epoch commit boundary;
- shard split/merge while operands are pending (Phase 7);
- compaction with a long-lived reader holding the prior frontier;
- exchange combiner equality vs. uncombined exchange;
- gateway partial-aggregation equality vs. full scan;
- writer fencing: a fenced shard's pending operands must never be
  applied after the new writer's first commit.

Each law adds one entry to the explicit fault-model registry in
`rockstream-sim` (`DESIGN.md §17.4`) naming the failure mode the law is
expected to survive.

### 7.3 Continuous simulation (v0.36 onward)

The v0.36 continuous-soak CI job replays every law's seeds and any
historical regression seeds. Adding a new law adds seeds to this corpus;
removing a seed requires a written justification.

## 8. Roadmap Adaptation (v0.5+)

Each cell below is the *additional* CRDT/merge-law work that version
owns. Detailed exit criteria live in `IMPLEMENTATION_PLAN.md`.

| Version | CRDT / merge-law deliverable |
|---|---|
| v0.5 | `MergeLaw`, `LawProperties`, `LawBundle` types in `rockstream-types`; `WeightAdd/v1` registered; shared property-test harness; Z-set operations consume the law. |
| v0.6 | `LawBundle` integrated into `ShardDb::merge`, `get_merged`, `scan_merged`; merge-read fallback wired and reported. |
| v0.7 | `SumCount/v1` registered; `AggregateMergeOp` re-implemented on top of `LawBundle`; arrangement headers carry `(law_id, law_version)`. |
| v0.8 | MIN/MAX use `MaxRegister/v1` / `MinRegister/v1` as cached-subcomponent laws, never as the whole operator. |
| v0.9 | Epoch commit and replay tests assert no operand survives across a fence; arrangement law headers participate in crash/replay parity. |
| v0.10 | `rockstream explain` prints law info for every operator; embedded freshness benchmark reports per-law combiner savings. |
| v0.11 | SQL lowering attaches `MergeLawId` (or `not_merge_safe_reason`) to every aggregate / set op / monotone recursive term in `PlanNode`. |
| v0.12 | Plan persistence stores `(law_id, law_version)`; unknown-law mount returns `RS-5002`. |
| v0.15 | DISTINCT/UNION/EXCEPT/INTERSECT use `WeightAdd/v1` end to end; zero-crossing compaction rule is law-declared. |
| v0.17 | `EXPLAIN INCREMENTAL` law-annotation contract finalized; `not_merge_safe_reason` strings become a closed enum. |
| v0.18 | SQL Alpha soak: zero divergence between law-using and law-bypassed execution of the same query. |
| v0.20 | `MaxRegister/v1` and `MinRegister/v1` registered (used internally by watermarks and time-window expiry). |
| v0.21 | `HyperLogLog/v1` registered (operator-level use). |
| v0.22 | Monotone recursion publishes `complete_through` using `WeightAdd/v1`'s monotone declaration. |
| v0.25 | `APPROX_COUNT_DISTINCT(v)` and `APPROX_MEMBERSHIP(v)` exposed using `HyperLogLog/v1` and `BloomUnion/v1`. |
| v0.27 | IVM correctness freeze includes law-equivalence tests for every registered law. |
| v0.30 | Pre-shuffle combiner is planner-driven; the v0.4 SUM/COUNT/AVG allowlist is deleted; uncombined-equivalence property test for every law. |
| v0.31 | Durable shuffle re-merges per-target operands using the same `LawBundle`. |
| v0.32 | Frontier protocol adds law-aware partial progress (monotone laws may publish `complete_through` ahead of cluster frontier). |
| v0.33 | Distributed recursion uses monotone laws for inner-frontier convergence. |
| v0.36 | Chaos alpha includes a "law-equivalence under fault" suite; continuous soak seeds every law. |
| v0.37 | Shard split/merge preserves arrangement law headers; compaction at split boundary uses the declared compaction policy. |
| v0.39 | Blue/green plan replacement covers incompatible law-version upgrades. |
| v0.41 | Gateway partial-aggregation pushdown is law-driven via `gateway_combiner`. |
| v0.42 | `AS OF MONOTONE PARTIAL` read mode for monotone-law views. |
| v0.43 | User-visible columns: `COUNTER`, `MAX_REGISTER`, `MIN_REGISTER`, `LWW`, `G_SET`; built-in CRDT delta DML (`+=`, set add/remove, register update); idempotency-key enforcement. |
| v0.44 | `OR_SET` column type with tombstone-GC compaction; connectors advertise CRDT columns in `discover_schema`. |
| v0.45 | Tier 2 connector contract carries `LawSchemaMetadata`; SDK examples for declaring CRDT columns. |
| v0.47 | All `merge_law_*` metrics live; support bundle includes per-law statistics; `rockstream debug arrangement` decodes law headers. |
| v0.50 | `CREATE MERGE LAW` DDL behind a feature flag; user-defined laws gated on the property-test suite; long soak covers user-defined laws under fault. |
| v0.52 | Cold-tier Parquet/Iceberg snapshot embeds `(law_id, law_version)` per column; external readers see finalized values, not raw operands. |
| v0.54 | Cold-tier soak proves law-version upgrade is replayable from cold + hot tail. |

## 9. Implementation Rules

1. **Lift the catalog to `rockstream-types`.** Storage, planner, runtime,
   and gateway all consume it; none of them own it. Putting it under
   storage couples query semantics to an executor.
2. **`SlateDB::MergeOperator` is a low-level executor of `LawBundle::merge_fn`.**
   It must not encode query semantics or law selection.
3. **Malformed operands fail closed on certified paths.** Return
   `RS-3009`; never silently last-writer-wins on a merge-safety-critical
   path. Last-writer-wins is acceptable only for `LWWRegister/v1`, which
   advertises the loss explicitly.
4. **Every persisted arrangement stores `(law_id, law_version)`.** No
   bare merge bytes. A shard mount that cannot resolve the bundle
   refuses with `RS-5002`.
5. **Every plan persists `(law_id, law_version)`.** Replay after upgrade
   re-resolves; incompatible upgrades take the blue/green path.
6. **Combiner eligibility comes from the planner, never from storage.**
   Storage cannot know whether the surrounding query is correctness-safe;
   it only knows the law.
7. **Every registered law ships with the shared property suite and a
   fault-model entry.** No exceptions.
8. **`EXPLAIN INCREMENTAL` is the user-visible contract.** If a law is
   used, it appears. If a fragment cannot use one, the reason appears.
9. **User-defined laws are experimental until v0.50.** Built-ins prove
   the entire pipeline first.
10. **Active-active multi-region writes remain a non-goal through 1.0.**
    The contract is structured so they could be added later (idempotent
    laws are already the right shape), but no public surface promises it.

## 10. What This Strategy Explicitly Is Not

- Not a replacement for the vector-frontier model. Exact SQL reads still
  use frontiers.
- Not a way to make non-idempotent monoids safe under arbitrary replay.
  Exactly-once epoching is still load-bearing.
- Not cross-shard `SERIALIZABLE`. Laws are commutativity tools, not
  conflict-detection tools.
- Not a license to expose arbitrary user merge functions. The built-in
  catalog must prove the pipeline first.
- Not a replication protocol. Active-active multi-region is out of scope
  through 1.0.
- Not a substitute for retraction-aware operators (MIN/MAX with deletes,
  Top-K, windows). Those keep their explicit arrangements.
- Not a justification for letting compaction shortcut snapshot safety.
  Every law's compaction policy is gated on the frontier.

## 11. Success Metrics

The CRDT / merge-law work is delivering value if these numbers move
the right direction or become explainable:

- read-modify-write rate per algebraic aggregate update: down;
- shuffle bytes for partitioned aggregate workloads: down;
- object-store request rate from operand compaction: down;
- gateway grouped-read latency via partial-pushdown: down;
- `EXPLAIN INCREMENTAL` operators with a printed law or a printed
  `not_merge_safe_reason`: 100%;
- correctness divergences between combined and uncombined execution:
  zero (CI-enforced);
- fault-model entries per law: at least one, all green in the
  continuous-soak corpus;
- `cold_tier_law_decode_failures_total`: zero across the cold-tier
  soak.

## 12. Open Questions (Tracked, Not Blocking)

- **Multi-region laws.** A future RFC may add a per-cluster
  "region-id"-tagged operation CRDT for inventory-like workloads. The
  contract here does not preclude it; no version promises it.
- **User-defined sketch laws.** `HyperLogLog/v1` and `BloomUnion/v1` are
  built-ins; user-defined sketches arrive only after `CREATE MERGE LAW`
  ships (v0.50) and a separate sketch-correctness review.
- **Range CRDTs (RGA, Yjs-style sequence types).** Out of scope through
  1.0; they need a causal-history model RockStream does not provide.
- **GC of OR-Set tombstones across shard splits.** Tracked as part of
  v0.37 elasticity; the `TombstoneGc` compaction policy must remain
  correct when ownership moves.

## 13. Final Recommendation

Make merge laws a core database concept starting in v0.5, ship the
shared `LawBundle` type in `rockstream-types` immediately after the v0.4
tag, and let every later version pay down its slice of the contract.
The internal wins (less RMW, smaller shuffles, aggressive but correct
compaction, partial pushdown, monotone partial reads) arrive long before
the first user-visible CRDT column lands in v0.43. By v0.45 the database
exposes a real CRDT surface; by v0.50 users can register their own laws;
and the architecture is still honest: exact SQL uses frontiers,
non-idempotent monoids still require exactly-once, and active-active
multi-region writes remain a deliberate non-goal.

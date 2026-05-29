# RockStream — Full Repository Assessment (v0.4)

**Audit scope.** Assessment of the v0.4 implementation: repository state at `main`,
workspace version `0.4.0`, design revision **DESIGN.md v3.27**, code corpus of
**4,181 LOC across 14 crates** against **12,215 LOC of design markdown**.

**Headline finding.** The repository is, today, an extremely polished
Phase-0 scaffold (a no-op pipeline, an audit log, key encoders, a SlateDB
wrapper, and a `SimRuntime` *type*) wrapped inside the design documents of
a v1.0 cloud-native IVM database. The design ambition is sound; the
implementation is roughly **v0.3–v0.4 of the project's own roadmap**. There
is nothing wrong with that *per se* — but the discipline gates that the
roadmap and DESIGN.md insist on (design-freeze after v0.10, `SimRuntime`
adoption from day one, fail-closed merge laws, the `MergeLaw`/`LawBundle`
catalog before IVM-1) are already being **violated by the code that exists**,
not by code that hasn't been written yet. That is the single most important
thing to fix before the implementation grows.

The remainder of this document is the brutal, file-level version of that
statement, organized by the five requested phases.

---

## Phase 1 — Structural & Dependency Analysis

### 1.1 Workspace shape vs. design ambition

| Crate | LOC | State | Comment |
|---|---:|---|---|
| [crates/rockstream-sim](crates/rockstream-sim) | 1,205 | Skeleton present | `SimRuntime`, `SimClock`, `SimObjectStoreHandle`, `SimNetworkHandle`, `buggify!`, `fault_model`, `paired_assert`. None wired into any production code path. |
| [crates/rockstream-storage](crates/rockstream-storage) | 1,346 | Thin SlateDB wrapper | `ShardDb`, key encoders, `SumCountMergeOperator`, `ShardReader`, WAL helpers, decent test coverage. |
| [crates/rockstream-runtime](crates/rockstream-runtime) | 343 | No-op pipeline | Synchronous Vec→Vec passthrough; no SlateDB, no async, no frontiers. |
| [crates/rockstream-connectors](crates/rockstream-connectors) | 239 | `Source`/`Sink` traits + noop | Trait surfaces are pure record-count placeholders. |
| [crates/rockstream-control](crates/rockstream-control) | 191 | Audit-log skeleton | File-backed JSONL append. |
| [crates/rockstream](crates/rockstream) | 125 | CLI entry | `rockstream start` runs the no-op pipeline synchronously. |
| [crates/rockstream-ops](crates/rockstream-ops) | 124 | `Operator` trait + noop | Operator works on `SourceBatch{record_count, epoch}`. No Z-set, no Arrow, no `_weight` column. |
| [crates/rockstream-types](crates/rockstream-types) | 154 | **Effectively empty** | Defines `type Epoch = u64` and 13 error-code constants. No `Row`, `Frontier`, `Schema`, `Antichain`, `MergeLaw`, `LawBundle`. |
| `rockstream-plan` | 10 | **Stub** | `mod tests { #[test] fn plan_crate_compiles() {} }` — that is the entire crate. |
| `rockstream-diff` | 10 | **Stub** | Same. |
| `rockstream-sql` | 10 | **Stub** | Same. |
| `rockstream-gateway` | 10 | **Stub** | Same. |
| `rockstream-oracle` | 10 | **Stub** | Same. |
| `rockstream-cli` | 10 | **Stub** | Same. |

**Verdict.** Half of the workspace exists only to reserve a crate name. That
is acceptable so long as nobody mistakes them for an *interface contract*.
Reserving names without committing to a public API is a real risk for a
project of this scope; the moment two of these stubs grow in parallel they
will disagree on basic shared types (Frontier, Row, MergeLawId) because the
shared crate (`rockstream-types`) currently contains nothing to disagree
against. **The shared types should land before the empty crates start
growing**, not after.

### 1.2 Dependency stack

Workspace `Cargo.toml` pins the sensible foundation (`slatedb = 0.13`,
`object_store = 0.12`, `tokio`, `parking_lot`, `bytes`, `serde`,
`thiserror`, `tracing`). Missing for the design as written:

- **Arrow** (`arrow`, `arrow-array`, `arrow-schema`) — IVM.md §4 and §5
  specify Arrow `RecordBatch` with a `_weight: i64` column as the runtime
  data type. The current `Operator` trait operates on
  `SourceBatch{record_count, epoch}`. There is no way to incrementally add
  Arrow later without rewriting every operator implementation. Bring Arrow
  in **now**, at v0.5 / IVM-1, so the no-op operator already moves
  `RecordBatch`es.
- **DataFusion** — declared as the SQL frontend. Missing as a workspace
  dep. The decision is consequential (it pins Arrow version, physical-expr
  surface, optimizer extension API). Pin it before `rockstream-plan` and
  `rockstream-diff` grow.
- **Substrait** — IMPLEMENTATION_PLAN §Phase 2 promises Substrait + RockStream
  extension encoding for plan persistence. Not present.
- **pgwire / postgres-protocol** — entire Phase 8 hangs on this. Not present.
- **prost / tonic** — promised for the exchange subsystem. Not present.
- **opentelemetry / opentelemetry-otlp** — required by P16. Not present.
- **proptest** — every milestone above v0.5 depends on a property-test
  harness. Not present.
- **criterion** — v0.27 demands a Criterion suite. Not present.

None of this is wrong yet, but the ordering matters: shared dependencies
should be picked at workspace level before the parallel crates take hard
divergent choices. The dependency policy document
([DEPENDENCY_POLICY.md](DEPENDENCY_POLICY.md)) exists but is not yet enforced
by `cargo deny` in CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)).

### 1.3 Repository metadata drift

- `Cargo.toml` line 24: `repository = "https://github.com/geir-gronmo/rockstream"`.
  The README, the audit request, and every reference to "trickle-labs" disagree.
  Pick one before the first published crate.
- README still says "Status: design phase (current revision: v3.24)".
  DESIGN.md is at v3.27. Two doc files, three numbers — drift after only
  three weeks of revisions.

### 1.4 Architectural layer bleeding (already present)

The crate boundaries look clean in `Cargo.toml`, but the *types* leak:

- [rockstream-ops/src/operator.rs](crates/rockstream-ops/src/operator.rs#L3-L4)
  imports `SinkBatch` and `SourceBatch` directly from
  `rockstream-connectors`. The operator layer therefore depends on the
  connector layer. This is upside-down: connectors should depend on a
  shared `Batch` type owned by `rockstream-types`, and operators should
  consume that. Fix this now (one-line move) or it will get baked in.
- [rockstream-runtime/src/pipeline.rs](crates/rockstream-runtime/src/pipeline.rs#L7)
  re-imports the same trio. Same fix.
- [rockstream-runtime/src/support_bundle.rs](crates/rockstream-runtime/src/support_bundle.rs#L6)
  depends on `rockstream-control::audit`. Runtime depending on control plane
  is also inverted; the audit-event *type* belongs in `rockstream-types`,
  the *writer* belongs in `rockstream-control`, and the runtime should
  emit through the type only.

### 1.5 Document duplication

- DESIGN.md (4,704 lines) and IVM.md (1,334 lines) overlap heavily on
  arrangements, frontiers, and operator state. The v3.X revision blocks
  at the top of DESIGN.md occupy **~360 lines of pure changelog** that
  should live in `CHANGELOG.md`, not in the design document a reader needs
  to load first.
- `docs/concepts.md` (1,990 lines) duplicates the README's "Key Concepts"
  section and the introductory sections of DESIGN.md.

---

## Phase 2 — Cloud-Native IVM & Performance Critique

The honest version of this section: **the IVM data path does not exist in
code yet.** What does exist points at real risks once it lands.

### 2.1 State management

- **No `MergeLaw` / `LawBundle` catalog.** DESIGN.md §6.11, ROADMAP v0.5,
  and IMPLEMENTATION_PLAN IVM-0 all say this contract is foundational and
  must land *before* IVM-1. The code has a hard-coded `SumCountMergeOperator`
  ([merge_registry.rs](crates/rockstream-storage/src/merge_registry.rs#L33-L82))
  with two tag bytes (`0x01 Sum`, `0x02 Count`). The "registry" struct has no
  registration API. The "fallback on malformed input is last-writer-wins"
  branches at lines 45, 53, 73 are a **silent-data-loss bug** the moment a
  third law shares the same key space: any tag mismatch overwrites the
  existing value with the new one. The IVM-0 spec is explicit:
  *fail-closed malformed-operand behavior returning `RS-3009`*. The current
  code is fail-open. Worse, `RS-3009` is not even in the error registry
  ([error_code.rs](crates/rockstream-types/src/error_code.rs)).
- **No arrangement header**, no `(law_id, law_version)` on stored values,
  no version-compatibility test. v0.3 cannot be considered complete without
  this; v0.12's "persisted plans store `(law_id, law_version)` per operator"
  cannot be satisfied because the law concept does not exist in types.
- **Write amplification.** When the real epoch coordinator lands it must
  coalesce all operator outputs for a shard into one `WriteBatch`
  (DESIGN.md §9, ROADMAP v0.9). The current `WriteBatch`
  ([shard_db.rs](crates/rockstream-storage/src/shard_db.rs#L129-L175))
  is a thin pass-through wrapper around `slatedb::WriteBatch` with a public
  `len()`/`is_empty()` only. There is no concept of *which operator* a
  fragment belongs to, no fragment-merge step, no idempotent
  `(epoch, op_id, port, seq)` key derivation. Without this, each operator
  will commit on its own and we will pay one fsync per operator per epoch —
  the exact failure mode the design names as "manifest churn budget".
- **`scan_prefix` materializes the entire result into `Vec<(Bytes, Bytes)>`**
  ([shard_db.rs:108-114](crates/rockstream-storage/src/shard_db.rs#L108)).
  This is fine for a no-op pipeline. It is **catastrophic** for arrangements
  the moment they hold more than a few hundred MB. The "bounded everything"
  rule from ROADMAP "Common Definition of Done" is being violated by the
  very first non-trivial method on the storage type. Return an async
  stream/iterator, not a `Vec`, before the first operator consumes it.

### 2.2 Storage / compute decoupling

- The decoupling story rests on a clean `Runtime` + `ObjectStore`
  abstraction. The current `ShardDb` takes
  `Arc<dyn object_store::ObjectStore>` directly
  ([shard_db.rs:40-60](crates/rockstream-storage/src/shard_db.rs#L40)). The
  `SimObjectStoreHandle`
  ([object_store.rs](crates/rockstream-sim/src/object_store.rs)) is a
  concrete in-memory map that **does not implement
  `object_store::ObjectStore`**. Therefore the simulator cannot drive SlateDB
  in deterministic mode at all. The Phase-0 exit criterion
  ("SlateDB determinism gate") is, as written, currently **impossible to
  satisfy with the code in `main`**.
- The local DbReader at [reader.rs](crates/rockstream-storage/src/reader.rs#L27-L31)
  is otherwise correct, but it does not yet pin to a `Checkpoint` — it opens
  the latest manifest. Phase 4+ cross-shard reads must pin to a published
  cluster frontier (DESIGN.md P15); the open API has nowhere to thread a
  checkpoint handle.

### 2.3 Latency & throughput

The current pipeline is synchronous
([pipeline.rs:42-82](crates/rockstream-runtime/src/pipeline.rs#L42)):

```rust
while let Some(batch) = source.poll_batch(epoch) {
    let output = operator.process(&batch);
    sink.write_batch(&output);
    sink.commit(epoch);
    operator.epoch_complete(epoch);
    epoch += 1;
}
```

That is an instructive scaffold but **every line of it will need to be
deleted** to satisfy P14 ("async scheduling, no synchronous global
scheduler tick"). The fact that `main()` is plain `fn main()` with no
`#[tokio::main]` ([crates/rockstream/src/main.rs:32](crates/rockstream/src/main.rs#L32))
is the same problem at a higher level. Both should be ported to async
*now*, before more code accretes around the synchronous shape.

Other latency hazards already visible:

- **Mutexes on hot paths.** `SimObjectStore` and `SimNetwork` use
  `parking_lot::Mutex<BTreeMap>` and `Mutex<VecDeque>` per operation
  ([object_store.rs:79-86](crates/rockstream-sim/src/object_store.rs#L79),
  [network.rs:74-82](crates/rockstream-sim/src/network.rs#L74)).
  Fine for tests, fine for sim, but the same `SimObjectStoreHandle` and
  `SimNetworkHandle` are also returned by `TokioRuntime`
  ([tokio_rt.rs:24-30](crates/rockstream-sim/src/tokio_rt.rs#L24)). The
  *production* runtime currently has no real object store and no real
  network. This is a foundational architectural bug, not a stub — see §3.1
  below.
- **`SimRuntime::sleep` advances the clock synchronously** and returns a
  no-op future ([sim.rs:91-95](crates/rockstream-sim/src/sim.rs#L91)),
  but `TokioRuntime::sleep` actually awaits. Code that calls
  `runtime.sleep(d).await` in a loop will *finish instantly* under
  simulation while running real-time in production. That is not a
  determinism contract; that is a behavioural fork.

### 2.4 Correctness & consistency

- **Recovery is unimplemented.** No frontier persistence
  ([keys.rs:91-93](crates/rockstream-storage/src/keys.rs#L91) declares
  `frontier_key()` but nothing writes it). No source-offset persistence.
  No idempotent-replay test. The v0.9 promise of
  "Kill-injected mid-commit run restarts to bit-identical output" cannot be
  evaluated against the current scaffold.
- **`buggify!`** ([buggify.rs](crates/rockstream-sim/src/buggify.rs))
  compiles cleanly but is gated behind `#[cfg(feature = "simulation")]`
  with **no crate in the workspace enabling that feature**. CI cannot
  exercise buggify. The "every PR touching coordination code must add a
  `buggify!()` annotation reviewed by a second engineer" rule from
  IMPLEMENTATION_PLAN Phase 1 has nothing to enforce against today.
- **`fault_model.rs` and `paired_assert.rs`** are not referenced from any
  production code path; they're sitting in the sim crate waiting to be
  called.
- **Async deterministic execution does not work.** `SimRuntime::spawn` only
  records the task name; it never executes the future
  ([sim.rs:106-118](crates/rockstream-sim/src/sim.rs#L106)). Every future
  spawned under the simulator is silently dropped. There is no executor,
  no run-to-quiescence loop, no event queue, no priority/jitter knob.
  This is the foundational gap that blocks **every** distributed-systems
  test the design depends on (frontier, exchange, checkpoint, recovery,
  2PC). A `SimRuntime` that cannot actually run async tasks is, in
  practice, a documentation fixture, not a simulator.

---

## Phase 3 — Scale-to-Zero to Scale-to-Infinity Continuum

### 3.1 Laptop scale (single process)

What works today:

- `cargo run -- start --storage ./data` runs end-to-end, creates the
  storage directory, writes an audit log, runs five no-op epochs, writes a
  support bundle, exits clean. That is genuine, valuable Phase-0 progress.
- No external dependencies (no Postgres, no Kafka, no MinIO, no etcd) to
  start a process. Good.

What is already wrong for the *embedded* profile (DESIGN.md §3.1, P17):

- **No async runtime in `main()`.** A synchronous `fn main()` is fine
  today; it is not fine the moment we open a real SlateDB (which is
  async-only). The embedded profile must use a single shared `tokio`
  current-thread runtime. Wire `#[tokio::main(flavor = "current_thread")]`
  now.
- **Hard `expect()` on storage directory creation**
  ([main.rs:45](crates/rockstream/src/main.rs#L45)) and on the audit log
  ([main.rs:50](crates/rockstream/src/main.rs#L50)) means a non-writable
  `./data` panics with no `RS-XXXX` code. The very first user error this
  product can produce violates the error-registry rule.
- **No config layer.** The promised "one binary, one CLI, one config"
  surface (DESIGN.md §3.1) is absent. `figment` already appears in the
  build cache (workspace already pulls it transitively) but no
  `RockstreamConfig` struct exists.
- **The `--role` flag accepts arbitrary strings** ([main.rs:23](crates/rockstream/src/main.rs#L23))
  with no validation. `rockstream start --role=nonsense` runs the no-op
  pipeline. Use a clap `ValueEnum`.

### 3.2 K8s scale (distributed cluster)

There is nothing distributed in the codebase today. The honest assessment
is therefore about *what gets baked in if the current shape grows*:

- **No gRPC / tonic** = no exchange wire protocol = no shard-to-shard
  story. The decision should be made before `rockstream-runtime` grows a
  scheduler.
- **`SimNetworkHandle` is fake.** A network simulator that only does
  FIFO per-recipient queueing with no latency, no drops, no reordering,
  no partitions ([network.rs:78-110](crates/rockstream-sim/src/network.rs#L78))
  cannot exercise the failure modes the design enumerates (Jepsen-style
  partition fencing, message reorder, dup, asymmetric outage). This is
  the area where TigerBeetle's and FoundationDB's simulators earned their
  reputation. The current `SimNetwork` is a queue, not a simulator.
- **No `WorkerCapacityModel`** — DESIGN.md §10.8 requires a
  `capacity_headroom` signal for HPA integration. Nowhere in the code.
- **No `--role=` plumbing** — flag exists, only one effective role.
- **No shard manager, no leasing, no fencing token type.** A
  `LeaseToken(u64)` type in `rockstream-types` is a 30-LOC investment
  that prevents 30 future bugs.

### 3.3 The continuum claim is unproven, and provably so

The "same engine on laptop and on a thousand nodes" claim rests on (a) a
real `Runtime` trait, (b) a real `ObjectStore` abstraction, (c) a real
async pipeline. None of these are present. The promise should be
re-stated as a *target invariant the test harness will enforce*, not an
already-true property.

---

## Phase 4 — Ergonomics & Operational Usability (DX)

### 4.1 API & query surface

- **No SQL.** [crates/rockstream-sql/src/lib.rs](crates/rockstream-sql/src/lib.rs)
  is 10 lines. The user-facing query surface does not exist. This is the
  product. It is the surface most likely to be wrong if it is designed
  last. Spike a hard-coded `CREATE MATERIALIZED VIEW ... AS SELECT ...
  FROM source GROUP BY k` parser and lowering pass **inside Phase 1
  alongside operators**, even if it accepts a strict subset, so the rest
  of the system can be exercised through it.
- **No EXPLAIN.** The "law name or `not_merge_safe_reason` for every
  operator" contract from v0.10 cannot be proven without `EXPLAIN
  INCREMENTAL` output. Ship a one-shot textual `Explain` formatter that
  walks the (future) `PlanNode` tree before v0.10.

### 4.2 Bootstrapping

- `make e2e` ([Makefile:23-30](Makefile)) is honest and works. Good.
- There is no `rockstream init` / `rockstream new-project` / template.
  README promises "under two minutes to a working view" (v0.10); the
  first thing missing is a single command that drops a sample SQL file
  and starts streaming from `GENERATE ROWS`. This is a one-day investment
  that pays back in every demo.
- Dev container promised in Phase 0 deliverables — not present
  (`devcontainer.json` does not exist).

### 4.3 Observability

- `tracing` is wired
  ([main.rs:33-37](crates/rockstream/src/main.rs#L33)) with `EnvFilter`.
  Good.
- **No metrics.** No `prometheus` / `metrics-exporter-prometheus`. No
  histogram type. No `view_slo_compliance` counter. The audit log is the
  only observability surface that exists.
- **No OpenTelemetry.** Phase 0 deliverables list "logging via `tracing`
  with OTEL exporter feature flag". Missing.
- **Support bundle is JSON-only and includes the entire audit-event
  stream as `serde_json::Value`** ([support_bundle.rs:21-24](crates/rockstream-runtime/src/support_bundle.rs#L21)).
  Once audit grows to thousands of events this becomes a multi-MB
  unbounded value. Tar+gz with a size cap and a redaction pass before it
  is shipped to users.
- **No `RS-XXXX` `next_steps` field.** ROADMAP v0.47 says every error
  code must have one, enforced in CI. The `ErrorCode` struct today is a
  `struct ErrorCode(u16)` with a `Display` impl
  ([error_code.rs:9-30](crates/rockstream-types/src/error_code.rs)); no
  `next_steps`, no `doc_url`, no `severity`. Change the type **now** so
  later additions can never regress.

---

## Phase 5 — Actionable Engineering Blueprint

The bug-class list and the refactor proposals below are ordered by what
will hurt the most if it is not fixed before the next milestone.

### B-01 — `SimRuntime` cannot run async tasks

**Root cause.** `SimRuntime::spawn` records names and discards futures
([sim.rs:106-118](crates/rockstream-sim/src/sim.rs#L106)). The runtime has
no executor, no event queue, no run-to-completion loop.

**Refactor.** Replace the current single-threaded recorder with a
deterministic single-thread executor built on a min-heap keyed by
`(virtual_time, spawn_seq)`. Each `spawn` enqueues a `Task` (Pin'd
future + waker). `sleep(d)` enqueues a wakeup at `now + d`. A
`run_until(predicate)` driver pops the next event, advances the virtual
clock, and polls. This is the *exact* shape `tokio-test`'s paused-time
machinery uses; cloning that pattern is a weekend, not a month.

**Spec draft.**

```rust
pub trait Runtime: Send + Sync + 'static {
    type Clock: Clock;
    type ObjectStore: ObjectStore;     // currently missing — see B-02
    type Network: NetworkTransport;    // currently missing — see B-02

    fn clock(&self) -> &Self::Clock;
    fn object_store(&self) -> &Self::ObjectStore;
    fn network(&self) -> &Self::Network;

    fn sleep(&self, d: Duration) -> BoxFuture<'_, ()>;
    fn spawn<F: Future<Output = ()> + Send + 'static>(&self, name: &'static str, fut: F) -> TaskHandle;

    fn seed(&self) -> u64;
    fn is_simulation(&self) -> bool;
}

// SimRuntime additionally exposes:
impl SimRuntime {
    pub async fn run_until<P: Fn(&Self) -> bool>(&self, pred: P);
    pub fn inject_fault(&self, fault: FaultId);
    pub fn advance_until_quiescent(&self);
}
```

This is the single highest-leverage change in the repository. **Do not
write another operator before this lands.**

### B-02 — `ObjectStore` is a concrete type, not a trait

**Root cause.** `ShardDb` takes `Arc<dyn object_store::ObjectStore>`
([shard_db.rs:40](crates/rockstream-storage/src/shard_db.rs#L40)).
`SimObjectStoreHandle` does not implement that trait, so the simulator
cannot drive SlateDB. `TokioRuntime` *also* returns a
`SimObjectStoreHandle` ([tokio_rt.rs:24](crates/rockstream-sim/src/tokio_rt.rs#L24)),
so the "production" runtime currently has no real object store.

**Refactor.**

1. Make `SimObjectStoreHandle` implement `object_store::ObjectStore` (the
   trait is small — `put`, `get`, `delete`, `list`). This unlocks the v0.3
   SlateDB determinism gate.
2. Move object-store *selection* into `RockstreamConfig`: `--storage=./data`
   → `LocalFileSystem`; `--storage=s3://bucket/prefix` → `AmazonS3`;
   `--storage=sim://` → `SimObjectStoreHandle`.
3. Make `TokioRuntime::object_store()` return the configured store, not a
   fresh empty `SimObjectStoreHandle`.
4. Add a faulty-store wrapper for chaos: `FaultInjectingObjectStore<S>`
   driven by `buggify!`.

### B-03 — No `MergeLaw` / `LawBundle` catalog; merge is silently
unsafe

**Root cause.** `SumCountMergeOperator` is hard-coded and returns
`Ok(value)` on tag mismatch and malformed inputs
([merge_registry.rs:45-77](crates/rockstream-storage/src/merge_registry.rs#L45)).
This is a P12 violation (associativity must be proven). The IVM-0
contract that v0.5 requires is absent.

**Refactor.**

```rust
// rockstream-types/src/merge_law.rs (new)
pub struct MergeLawId(pub u16);
pub struct MergeLawVersion(pub u16);

pub trait LawMergeFn: Send + Sync {
    fn merge(&self, existing: Option<&[u8]>, incoming: &[u8])
        -> Result<Vec<u8>, MergeError>;     // never silently overwrites
}

pub struct LawBundle {
    pub id: MergeLawId,
    pub version: MergeLawVersion,
    pub name: &'static str,
    pub class: MergeLawClass,                // CommutativeMonoid | Semilattice | ...
    pub properties: LawProperties,           // assoc, comm, idem, has_inverse, has_identity
    pub duplicate_policy: DuplicatePolicy,
    pub compaction_policy: CompactionPolicy,
    pub frontier_policy: FrontierPolicy,
    pub encoder: Box<dyn LawEncoder>,
    pub merge_fn: Box<dyn LawMergeFn>,
    pub compaction_filter: Option<Box<dyn LawCompactionFilter>>,
    pub gateway_combiner: Option<Box<dyn LawGatewayCombiner>>,
    pub explain: Box<dyn LawExplain>,
}

pub struct MergeLawCatalog { /* register-only, panics on collision */ }
```

`SumCountMergeOperator` becomes a `LawBundle` for `SumCount/v1`. Every
arrangement key writes a 4-byte `(law_id, law_version)` header. Failed
merges return `RS-3009 merge.malformed_operand`, which must also be added
to the error registry.

**Test.** A shared `law_property_tests::run::<L>(law)` harness
(associativity, commutativity-where-declared, identity, idempotence,
serialization round-trip, fail-closed malformed, version compatibility)
that every registered law has to pass in CI.

### B-04 — Async / sync split in the pipeline freezes us into the wrong shape

**Root cause.** The whole pipeline + main + operator + connector chain is
synchronous today. SlateDB is async-only. The instant the no-op operator
is replaced with anything that touches storage, every signature has to
flip.

**Refactor.**

1. `Operator::process` returns `BoxFuture<'_, EpochOutput>`.
2. `Source::poll_batch` and `Sink::write_batch` become `async fn`.
3. `main()` uses `#[tokio::main(flavor = "current_thread")]`.
4. `run_pipeline` becomes `async fn`.
5. Every call site goes through `runtime.spawn(...)`.

Do this **once**, while the pipeline is still 80 lines.

### B-05 — `WriteBatch` cannot represent shard-level group commit

**Root cause.** `WriteBatch`
([shard_db.rs:129-175](crates/rockstream-storage/src/shard_db.rs#L129))
has no notion of "which operator", no idempotent key derivation, no
fragment merging.

**Refactor.** Introduce `EpochCommit`:

```rust
pub struct EpochCommit {
    pub shard_id: ShardId,
    pub epoch: Epoch,
    pub fragments: Vec<OperatorFragment>,    // one per operator instance on this shard
}

impl EpochCommit {
    pub fn into_batch(self) -> WriteBatch { /* idempotent keys derived from (epoch, op_id, port, seq) */ }
}
```

The shard-level coordinator collects `OperatorFragment`s as operators
finish their epoch contribution, then commits one `WriteBatch` containing
state, output, shuffle-staging, connector offsets, and frontier.

### B-06 — `scan_prefix` returns `Vec<(Bytes, Bytes)>`

**Root cause.** [shard_db.rs:108-116](crates/rockstream-storage/src/shard_db.rs#L108)
materializes the full result. Violates "bounded everything".

**Refactor.** Return `impl Stream<Item = Result<KvPair, StorageError>>`
(or use `async-stream`). Add a `scan_prefix_bounded(prefix, max_bytes)`
helper for the call sites that genuinely need a `Vec` (catalog reads).
**Property:** no callable on `ShardDb` may load more than a configurable
budget into memory without an explicit `_bounded` suffix.

### B-07 — `rockstream-types` is empty; downstream crates will diverge

**Refactor.** Populate `rockstream-types` *now* with:

```
timestamp::Epoch                 // already there
timestamp::ProcessingTime
timestamp::EventTime
frontier::Antichain<T>
row::RowId                       // stable per IVM.md §IVM-4
row::Weight (i64)
schema::Schema (Arrow alias)
batch::DeltaBatch                // Arrow RecordBatch + _weight column convention
merge_law::{MergeLawId, MergeLawVersion, LawBundle, ...}
error_code::{ErrorCode, next_steps, doc_url, severity}
ids::{ShardId, OperatorId, ViewId, NamespaceId, ExchangeId, LeaseToken}
```

Each is small. The cost of *not* doing this is two crates inventing
their own `Frontier`.

### B-08 — Audit-event type lives in the wrong crate

**Refactor.** Move `AuditEvent` into `rockstream-types::audit`. Keep
`FileAuditLog` in `rockstream-control`. Runtime emits typed events,
control writes them. No more upward dependency from runtime → control.

### B-09 — Operator/Connector trait coupling

**Refactor.** Replace `SourceBatch` and `SinkBatch` (record-count
placeholders) with `DeltaBatch` in `rockstream-types`. Move the trait
definitions into `rockstream-types` (they are the contract) and keep only
implementations in `rockstream-connectors` / `rockstream-ops`.

### B-10 — Design-document churn vs. evidence

**Root cause.** DESIGN.md has accumulated 27 revisions (v3.1 … v3.27)
covering ~360 lines of changelog *before* the IVM kernel exists. ROADMAP
says "Design freeze after v0.10"; we are at v0.4 and design is still
growing faster than code.

**Refactor.**

1. Move the v3.x revision history out of DESIGN.md into `CHANGELOG.md`.
2. Adopt a discipline rule: **no DESIGN.md or IVM.md change without an
   accompanying code or test change in the same PR**. This is the single
   most effective antidote to "the design ran ahead of the code".
3. Track open design questions as GitHub issues labelled `design-debt`,
   not as new DESIGN.md sections.
4. Cap the `ideas/` directory at "explored, not promised" — the
   current `optimistic-locking-crdts.md` (919 lines) reads as a v0.55
   design when its target is post-1.0.

### B-11 — Repository identity drift

**Fix (one PR).** Set `repository = "https://github.com/trickle-labs/rockstream"`
in `Cargo.toml`. Update README and any internal links. Decide where the
canonical home is.

### B-12 — CI does not exercise simulation, denylist, or property tests

**Refactor.** Extend [.github/workflows/ci.yml](.github/workflows/ci.yml)
to:

- `cargo deny check` (the file exists; the gate doesn't).
- `cargo test --workspace --features simulation` once the feature lands.
- A daily scheduled job that runs the future simulation soak with a
  random seed.

---

## Recommended Sequencing (next ~10 person-weeks)

This is what I would do in the *exact* order before adding any new feature.
Each item is hours-to-days, not weeks.

1. **B-11**: fix repository URL (10 minutes).
2. **B-07**: populate `rockstream-types` shells (1 day).
3. **B-08, B-09**: move `AuditEvent` and batch types so the dependency
   graph stops being upside-down (½ day).
4. **B-02**: make `SimObjectStoreHandle` implement
   `object_store::ObjectStore` and route `TokioRuntime` to a real backend
   (1 day).
5. **B-01**: turn `SimRuntime` into a real deterministic executor (3–5
   days). This is the keystone — every later test depends on it.
6. **B-04**: port pipeline + connectors + ops + main to async,
   `#[tokio::main(current_thread)]` (1 day).
7. **B-06**: stream-based `scan_prefix` (½ day).
8. **B-05**: `EpochCommit` + shard-level coordinator skeleton (2 days).
9. **B-03**: `MergeLaw` / `LawBundle` catalog with `WeightAdd/v1`,
   `SumCount/v1`, fail-closed merge, property-test harness (3–4 days).
10. **B-10**: split DESIGN.md changelog out; adopt the
    "no doc change without a code change" rule (½ day plus discipline).
11. **B-12**: extend CI with `cargo deny` and a `simulation` feature
    job (½ day).

After step 9 the repository is genuinely at v0.5 / IVM-1 ready, and the
next operator (filter / project / map) lands on solid ground instead of
on the current synchronous record-count scaffold.

---

## What the Project Is Doing Right

It is worth being explicit, because the criticisms above are heavy:

- The **design is good**. DESIGN.md and IVM.md are coherent, internally
  consistent, and honest about non-goals (no global LSN, no `SERIALIZABLE`,
  no active-active multi-region). The principles (P1–P18) are the right
  principles.
- The **MergeLaw / LawBundle** abstraction, the **deterministic simulator**
  commitment, the **bounded-everything** rule, and the **single-binary
  embedded profile** are exactly the four bets a cloud-native IVM engine
  should make.
- **`ROADMAP.md`'s "10 person-weeks per version" and "evidence over dates"**
  is the right cadence. The job now is to live by it.
- The test density inside the crates that do exist
  ([storage/src/tests.rs](crates/rockstream-storage/src/tests.rs) is 545
  lines for a 1,346-LOC crate;
  [sim/src/tests.rs](crates/rockstream-sim/src/tests.rs) is 233 lines)
  is genuinely above industry norm for this stage.
- The **error-code-first** discipline is wired in spirit
  ([error_code.rs](crates/rockstream-types/src/error_code.rs)) even if the
  surface is currently 13 codes and missing `next_steps`.
- The audit-log skeleton is small, correct, and tested — a good model for
  every other subsystem.

The biggest single thing that will determine whether RockStream becomes
"the absolute best cloud-native IVM engine in existence" is **whether the
implementation discipline catches up to the design discipline in the next
two milestones**. The design has earned the right to be ambitious. The
code has not yet earned the right to claim the design.

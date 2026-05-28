# Multi-Shard Optimistic Locking for Transactions with CRDT Columns

Status: research report / candidate RFC

Date: 2026-05-28

Audience: implementers of `rockstream-gateway`, `rockstream-types`,
`rockstream-storage`, `rockstream-runtime`, `rockstream-plan`, and
`rockstream-sim`.

Related documents:

- [DESIGN.md](../DESIGN.md) sections 1.1, 6.11, 9, 12.6, 13.5.
- [IMPLEMENTATION_PLAN.md](../IMPLEMENTATION_PLAN.md) Phase 8 through Phase 12.
- [ideas/crdts.md](crdts.md).

## 1. Executive Summary

RockStream can combine optimistic locking with CRDT columns, but it should not
claim that this yields general cross-shard `SERIALIZABLE` isolation. The useful
pre-1.0 opportunity is narrower and stronger:

1. **Blind CRDT writes do not need conflict validation.** If a transaction only
   appends registered `MergeLaw` operands whose operations commute, the write
   conflict set is empty by construction. The system still needs idempotency
   keys, operation IDs, and a visibility rule, but it does not need read-write
   conflict detection.
2. **Non-CRDT writes can use optimistic guards.** If a transaction reads exact
   primary keys and writes non-CRDT columns, the gateway can validate those keys
   against per-shard versions before commit. This prevents stale overwrites
   without locks.
3. **Single-shard `SERIALIZABLE` is feasible pre-1.0.** If the planner proves
   every read and write touches one base-table shard, RockStream can delegate to
   SlateDB transactions and per-shard version checks.
4. **Mixed multi-shard transactions are possible as an experimental subset, not
   as full SQL serializability.** CRDT writes can skip validation; non-CRDT
   exact-key writes validate per shard. Predicate reads, range reads, negative
   constraints, uniqueness, foreign keys, and write-skew-sensitive business
   rules must be rejected or deferred.
5. **Atomic visibility is the real hard part.** CRDTs make write order
   irrelevant, but they do not by themselves make a multi-shard SQL transaction
   visible all-or-nothing. A multi-shard CRDT transaction needs either a
   transaction-envelope visibility protocol or a documented weaker surface such
   as "atomic accept, eventually convergent application."

Recommendation: do the pre-1.0 work in layers. Add row/version metadata and
optimistic guarded writes with the v0.43 direct-write surface; add
single-shard `SERIALIZABLE` and CRDT-only transaction envelopes behind a flag in
v0.50; use v0.52-v0.54 to prove mixed exact-key optimistic validation in
simulation. Keep general cross-shard `SERIALIZABLE` out of 1.0.

## 2. What Problem Are We Solving?

The current design correctly says that `SERIALIZABLE` isolation is out of scope
for cross-shard transactions. True serializability requires preventing write
skew and other read-write dependency cycles. In a sharded database, that usually
means one of the following:

- a global timestamp or global write sequence;
- replicated locks or write intents;
- predicate locks;
- a global conflict detector;
- per-shard dependency tracking plus a cross-shard cycle detector.

RockStream deliberately avoids those hot-path costs. The design uses:

- causal vector frontiers, not a global LSN;
- one writer per shard, fenced by SlateDB;
- atomic per-shard `WriteBatch` commits;
- exactly-once source epochs;
- a database-wide `MergeLaw` catalog for commutative and CRDT state.

The research question is therefore not "can optimistic locking make arbitrary
SQL serializable without coordination?" It cannot. The better question is:

> Which useful transaction shapes can RockStream support by combining
> optimistic version checks for non-commutative state with CRDT merge laws for
> commutative state?

That reframing is important. It lets RockStream ship valuable behavior without
smuggling a transaction manager into the design.

## 3. Existing RockStream Ingredients

The design already has most of the machinery needed for a conservative
optimistic-locking extension.

### 3.1 Vector-frontier isolation

`READ COMMITTED` pins each statement to the latest published vector frontier.
`REPEATABLE READ` pins a transaction at `BEGIN`. This gives the gateway a
natural read timestamp without inventing a global LSN.

This is good for optimistic locking because every read can record:

```text
ReadStamp = {
  statement_id,
  transaction_id,
  pinned_vector_frontier,
  per_shard_observed_frontier,
}
```

It is not enough for full serializability because a vector frontier is a
partial order, not a total order over every write.

### 3.2 Direct-write source connector

DESIGN.md section 13.5 already buffers pgwire `INSERT`, `UPDATE`, and `DELETE`
inside the gateway. `COMMIT` flushes the buffer as a Z-set delta to the
base-table shard via `WriteBatch`; `ROLLBACK` drops the buffer.

This is the natural interception point for optimistic locking:

- track read footprints while the transaction runs;
- classify write operations by merge law;
- validate non-CRDT writes at commit;
- attach idempotency keys or CRDT operation IDs;
- emit a clear error for unsupported transaction shapes.

### 3.3 Per-shard atomicity

Each shard commits an epoch as one or more coalesced SlateDB `WriteBatch`es.
That gives RockStream atomic multi-key mutation inside one shard. It does not
give atomic all-or-nothing visibility across shards.

The distinction matters:

- **Per-shard transaction:** SlateDB can make it atomic.
- **Multi-shard transaction:** RockStream needs an explicit transaction-envelope
  or must document weaker visibility.

### 3.4 MergeLaw catalog

The CRDT strategy makes every algebraic operation explicit:

- `DuplicatePolicy`: `RequireExactlyOnce`, `DedupeByOpId`, or `Idempotent`.
- `CompactionPolicy`: `FrontierFold`, `TombstoneGc`, or `NeverFold`.
- `FrontierPolicy`: exact only or monotone partial allowed.
- `LawBundle::merge_fn`: the shared storage/runtime/gateway merge behavior.

This is exactly what optimistic locking needs to know. A planner can ask:

```text
Does this write commute with concurrent writes to the same logical value?
Can duplicate replay be ignored, deduped, or rejected?
Can a read-dependent guard be skipped, or must it validate?
```

### 3.5 Idempotency-key enforcement

The v0.43 CRDT surface already requires idempotency keys for non-idempotent
direct writes without an exactly-once source envelope. That prevents duplicate
application on retry. Optimistic locking should reuse this table instead of
inventing a second dedupe mechanism.

## 4. Lessons From Prior Systems

### 4.1 DynamoDB optimistic locking

DynamoDB-style optimistic locking uses a per-item version attribute. A client
reads version `v`, sends an update with condition `version == v`, and retries if
the condition fails.

Lesson for RockStream:

- This is excellent stale-overwrite protection for one item/key.
- It does not detect cross-item write skew.
- It does not make global-table last-writer-wins reconciliation serializable.

RockStream should adopt the version-check idea for exact-key non-CRDT writes,
not as a claim of global serializability.

### 4.2 PostgreSQL Serializable Snapshot Isolation

PostgreSQL's `SERIALIZABLE` mode is snapshot isolation plus monitoring for
dangerous read-write dependency structures. It uses predicate locks to discover
when a write could have changed a concurrent read result. Transactions can fail
with serialization errors and must be retried.

Lesson for RockStream:

- Snapshot isolation alone is not enough.
- Predicate/range reads are the hard part.
- Any real serializable extension needs dependency tracking and retry handling.

### 4.3 CockroachDB transactions

CockroachDB uses MVCC timestamps, write intents, transaction records, latches,
lock tables, timestamp caches, read refreshing, and parallel commit machinery.
It can make cross-range transactions serializable, but it pays for distributed
conflict management and replicated state.

Lesson for RockStream:

- Full serializability is possible, but it is a transaction subsystem.
- A transaction record is the usual answer to multi-shard atomic visibility.
- Conflict-free CRDT writes can avoid much of the conflict machinery, but they
  still need durability, dedupe, and visibility semantics.

### 4.4 CALM / highly available transaction intuition

The CALM intuition is useful here: monotone programs do not require coordination
for consistency, while non-monotone decisions usually do. CRDT writes are a
practical instance of this. Appending `+1` to a counter, adding a tag to a
grow-only set, or taking a register-wise maximum does not depend on seeing the
latest global state.

Lesson for RockStream:

- Blind commutative updates can avoid coordination.
- Read-dependent guards such as "only increment if count < 10" are
  non-monotone and need validation or escrow.

## 5. Semantics We Should Name

The report recommends adding names before adding features. Ambiguous names are
how transactional systems get users hurt.

| Name | Meaning | Cross-shard? | Pre-1.0? |
|---|---|---:|---:|
| `READ COMMITTED` | Statement pins latest vector frontier. | Yes | Already planned |
| `REPEATABLE READ` | Transaction pins vector frontier at `BEGIN`. | Yes | Already planned |
| `SERIALIZABLE LOCAL` | True serializable if planner proves one shard. | No | Candidate v0.50 |
| Optimistic guarded write | Exact-key version check prevents stale overwrite. | Yes, per key | Candidate v0.43+ |
| Commutative transaction envelope | Multi-shard CRDT writes with op-id dedupe and explicit visibility. | Yes | Candidate v0.50 flag |
| Mixed optimistic transaction | CRDT writes skip validation; non-CRDT exact-key writes validate. | Yes, restricted | Candidate v0.54 experiment |
| Cross-shard `SERIALIZABLE` | General SQL serializability with predicate/range dependencies. | Yes | No |

Do not call the middle rows `SERIALIZABLE`. They are useful, but they are not
the ANSI guarantee.

## 6. The Core Insight

Optimistic locking asks: "Did something I depended on change before I wrote?"

CRDTs change the question: "Does this write depend on the old value at all?"

For many CRDT writes, the answer is no.

```sql
UPDATE balances SET amount = amount + 5 WHERE account = 'alice';
UPDATE balances SET amount = amount - 2 WHERE account = 'alice';
```

Both operations can be represented as operands. If they are applied in either
order, the final folded value is the same, assuming duplicate handling is
correct.

But this query is different:

```sql
UPDATE balances
SET amount = amount - 5
WHERE account = 'alice' AND amount >= 5;
```

The write is a CRDT delta, but the predicate is a non-monotone guard over the
current value. Two concurrent transactions can both observe `amount = 5` and
both subtract 5. The CRDT does not protect the invariant. This shape requires
optimistic validation, escrow, or rejection.

Therefore the planner must classify transactions by dependency, not just by
column type.

## 7. Proposed Transaction Shape Classifier

Add a gateway/planner classifier for direct-write transactions:

```rust
pub enum TxnShape {
    ShardLocalSerializable {
        shard: ShardId,
    },
    BlindCommutative {
        participants: Vec<ShardId>,
    },
    OptimisticExactKey {
        participants: Vec<ShardId>,
        read_footprint: ReadFootprint,
    },
    MixedCrdtAndOptimisticExactKey {
        participants: Vec<ShardId>,
        crdt_ops: Vec<CrdtOp>,
        guarded_ops: Vec<GuardedOp>,
        read_footprint: ReadFootprint,
    },
    Unsupported {
        reason: UnsupportedTxnReason,
    },
}
```

`UnsupportedTxnReason` should be a closed enum, similar to
`not_merge_safe_reason`:

```rust
pub enum UnsupportedTxnReason {
    CrossShardSerializableRequested,
    PredicateReadRequiresSerializable,
    RangeReadRequiresPredicateValidation,
    NegativeConstraintRequiresEscrow,
    UniqueConstraintRequiresGlobalIndex,
    ForeignKeyRequiresCrossShardCheck,
    NonCrdtWriteWithoutVersionGuard,
    ReadDependentCrdtWriteRequiresValidation,
    UnknownMergeLaw,
}
```

This gives users and operators an honest `EXPLAIN` surface.

## 8. Metadata Needed

### 8.1 Row and column version metadata

Each base-table row should carry a small metadata record:

```rust
pub struct RowVersionMeta {
    pub shard_id: ShardId,
    pub table_id: TableId,
    pub primary_key_hash: [u8; 16],
    pub row_version: u64,
    pub last_modified_frontier: EncodedFrontier,
    pub last_writer_txn: Option<TxnId>,
    pub law_bitmap: LawBitmap,
}
```

For wide rows, column-group versions may be worth adding later, but start with
row-level versions. Row-level conflict false positives are acceptable; false
negatives are not.

Suggested key space:

```text
op_state/txn_meta/table/{table_id}/pk/{primary_key_hash} -> RowVersionMeta
```

If this lives in `view_output/` rather than `op_state/`, keep the prefix
explicit and document it in the storage key reference. Do not hide it inside the
CRDT operand encoding.

### 8.2 Read footprints

Track what a transaction depended on:

```rust
pub enum ReadFootprintEntry {
    ExactKey {
        shard_id: ShardId,
        table_id: TableId,
        primary_key_hash: [u8; 16],
        observed_row_version: u64,
        observed_frontier: EncodedFrontier,
    },
    KeyRange {
        shard_id: ShardId,
        table_id: TableId,
        range: EncodedKeyRange,
        observed_range_version: Option<u64>,
    },
    Predicate {
        table_id: TableId,
        normalized_predicate_hash: [u8; 16],
    },
}
```

Pre-1.0 validation should accept only `ExactKey`. `KeyRange` and `Predicate`
entries should force `UnsupportedTxnReason` unless a future range-summary or
predicate-lock design exists.

### 8.3 CRDT operation IDs

Every CRDT operand written through a transaction gets a stable operation ID:

```text
op_id = hash(namespace_id, txn_id, statement_index, operation_index, law_id)
```

This plugs into `DuplicatePolicy`:

- `Idempotent`: duplicate op is harmless, but still useful for observability.
- `DedupeByOpId`: duplicate op must be dropped.
- `RequireExactlyOnce`: require an idempotency key or exact-once source epoch;
  otherwise return `RS-2007`.

### 8.4 Transaction envelope

Multi-shard CRDT transactions need a durable envelope if RockStream wants to
offer all-or-nothing *visibility* rather than merely eventual convergence.

```rust
pub struct TxnEnvelope {
    pub txn_id: TxnId,
    pub namespace_id: NamespaceId,
    pub home_shard: ShardId,
    pub participants: Vec<ShardId>,
    pub state: TxnEnvelopeState,
    pub op_hashes: Vec<[u8; 32]>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
}

pub enum TxnEnvelopeState {
    Pending,
    ParticipantApplied { shard_id: ShardId },
    Committed,
    Aborted,
}
```

This resembles a transaction record, but its job is narrower than CockroachDB's:

- not conflict detection;
- not predicate locking;
- not timestamp pushing;
- only durable identity, retry, dedupe, and visibility for commutative writes.

If RockStream does not want this complexity pre-1.0, then multi-shard CRDT
transactions should be documented as **idempotent multi-shard write batches**,
not as SQL transactions.

## 9. Commit Protocol Options

### 9.1 Option A: Scatter-gather, visible immediately

Flow:

1. Gateway classifies transaction as `BlindCommutative`.
2. Gateway sends CRDT operands to participant shards in parallel.
3. Each shard applies its operands in a local `WriteBatch`.
4. Gateway returns success after all participants acknowledge.
5. If the gateway crashes mid-flight, retry by `txn_id` completes missing
   participants and dedupes already-applied operands.

Pros:

- simplest;
- no participant waits on a global lock;
- no conflict detection;
- good fit for CRDT semantics.

Cons:

- concurrent readers can observe partial application while the gateway is
  still waiting for all acknowledgements;
- a failed client may not know whether the transaction fully applied;
- this is not SQL transaction atomicity.

Verdict: useful as an ingest batch primitive, not enough for a pgwire
transaction promise.

### 9.2 Option B: Transaction envelope with visibility marker

Flow:

1. Gateway writes `TxnEnvelope(Pending)` to a home shard or control shard.
2. Gateway sends participant operands tagged with `txn_id` and `op_id`.
3. Participant shards persist operands as pending or invisible.
4. Each participant records an apply acknowledgement.
5. Gateway or recovery worker marks envelope `Committed` after all participants
   are durable.
6. Reads include only committed envelopes, or materializers promote pending ops
   to visible ops after observing the commit marker.

Pros:

- all-or-nothing visibility is possible;
- retry and recovery are explicit;
- no read-write conflict detector is needed for blind CRDT writes.

Cons:

- adds a transaction-record-like subsystem;
- read paths must avoid exposing pending operands;
- compaction must not fold pending operands into visible state;
- recovery has to finish or abort old envelopes.

Verdict: the right design if RockStream wants to call this a multi-shard
transaction pre-1.0. It is not full serializability, but it can be atomic and
coordination-light.

### 9.3 Option C: Frontier-gated visibility

Flow:

1. Participant shards write operands normally.
2. Frontier aggregator withholds a transaction visibility frontier until all
   participant shards report the transaction's epoch.
3. Gateways reading at published transaction frontiers do not see partial
   transactions.

Pros:

- reuses frontier machinery;
- avoids per-read transaction-record lookups;
- natural for materialized view freshness.

Cons:

- harder for single-shard point reads, which may otherwise be tempted to read
  a newer local frontier;
- needs participant metadata in frontier reports;
- still needs a durable retry record for crash recovery;
- can delay unrelated reads if implemented too coarsely.

Verdict: promising for view visibility, but not sufficient by itself. It pairs
well with Option B.

## 10. Optimistic Validation Protocol

This is for non-CRDT exact-key writes and mixed transactions.

### 10.1 Read phase

When a transaction reads an exact primary key, record:

```text
(shard_id, table_id, primary_key_hash, observed_row_version, observed_frontier)
```

If a transaction reads a range or predicate, record it too, but mark the
transaction unsupported for optimistic validation unless the query is read-only.

### 10.2 Write buffering

For each write, classify:

```rust
pub enum BufferedWrite {
    CrdtOperand {
        target: RowColumnRef,
        law_id: MergeLawId,
        law_version: u16,
        op_id: OpId,
        operand: Bytes,
        read_dependent: bool,
    },
    GuardedPut {
        target: RowRef,
        expected_row_version: u64,
        value: Bytes,
    },
    GuardedDelete {
        target: RowRef,
        expected_row_version: u64,
    },
}
```

Set `read_dependent = true` if the write is guarded by a predicate over a value
read inside the transaction, even if the target column is a CRDT.

### 10.3 Validation phase

At `COMMIT`, the gateway sends validation requests to participant shards in
parallel:

```text
ValidateExactKeys(txn_id, entries[])
```

Each shard answers:

```text
Ok
Conflict { key, observed_version, current_version, last_writer_txn }
UnknownVersion { key }
UnsupportedRangeOrPredicate
```

Rules:

1. Blind CRDT writes with `read_dependent = false` do not validate.
2. Read-dependent CRDT writes validate the reads they depended on.
3. Non-CRDT writes require exact-key validation.
4. Any range or predicate footprint rejects the transaction pre-1.0.
5. Any missing row-version metadata rejects fail-closed.

### 10.4 Apply phase

Apply writes in one of three ways:

- **Shard-local:** one SlateDB transaction / `WriteBatch` with version check.
- **Blind CRDT multi-shard:** transaction envelope or documented ingest batch.
- **Mixed exact-key:** validate all participants, then apply guarded writes and
  CRDT operands. If any participant apply fails after another succeeds, recovery
  must retry idempotently by `txn_id` until all participants reach the same
  terminal state.

Do not call the mixed path `SERIALIZABLE`. It prevents stale overwrites on
tracked exact keys; it does not detect all dependency cycles.

## 11. Why Per-Shard Validation Is Not Enough for Serializable

Consider two transactions:

```text
T1: read X on shard A; write Y on shard B
T2: read Y on shard B; write X on shard A
```

If both validate only the keys they read before either write becomes visible,
both can pass. The committed history may have no serial order. Detecting this
requires a dependency graph or predicate/read-write conflict tracking across
participants.

CRDT writes avoid this only when the transaction is not making a non-monotone
decision based on the read. If `write Y` is a blind add to a CRDT, then its
order relative to another blind add does not matter. If `write Y` depends on
the value of `X`, the dependency matters again.

That is the line RockStream must enforce.

## 12. Invariants and Escrow

Some business rules look like counters but are not coordination-free:

- inventory cannot go below zero;
- account balance cannot go negative;
- at most N users may hold a seat;
- only one active primary owner may exist.

A plain `COUNTER` is not enough. Options:

1. **Reject pre-1.0.** If the write has a non-monotone guard, return an
   unsupported transaction-shape error.
2. **Optimistic guard.** Validate the exact key/version read by the guard. This
   works when all relevant state is a single key.
3. **Escrow CRDT.** Pre-allocate rights or tokens to shards. A shard can spend
   its local allocation without coordination; replenishing allocation requires
   coordination. This is the right future direction for bounded counters.

Escrow is worth a separate post-1.0 RFC. Do not mix it into the first
optimistic-locking deliverable.

## 13. Error Codes and User Surface

Proposed new codes, if this becomes implementation work:

```text
RS-2008  transaction.optimistic_conflict
RS-2009  transaction.unsupported_shape
RS-2010  transaction.visibility_pending
RS-2011  transaction.ambiguous_commit_retry_with_idempotency_key
```

Suggested SQL behavior:

```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- If planner proves single-shard:
COMMIT; -- allowed

-- If planner cannot prove single-shard:
COMMIT; -- RS-2003 isolation.serializable_not_supported
```

For optimistic guarded writes, avoid pretending this is an isolation level:

```sql
UPDATE accounts
SET email = $1
WHERE id = $2
  AND rockstream_row_version() = $3;
```

or, more ergonomically:

```sql
UPDATE accounts
SET email = $1
WHERE id = $2
WITH (optimistic = true);
```

For CRDT-only multi-shard batches, require an idempotency key:

```sql
BEGIN WITH (idempotency_key = 'client-123:txn-456');
UPDATE balances SET amount = amount + 10 WHERE account = 'alice';
UPDATE balances SET amount = amount - 10 WHERE account = 'bob';
COMMIT;
```

If the implementation lacks a transaction envelope, call this a commutative
write batch in documentation, not an atomic transaction.

## 14. Observability

Metrics:

```text
optimistic_validation_attempt_total{shape}
optimistic_validation_conflict_total{table, shard}
optimistic_validation_latency_ms{shape}
txn_shape_rejected_total{reason}
crdt_txn_envelope_created_total
crdt_txn_envelope_committed_total
crdt_txn_envelope_recovered_total
crdt_txn_partial_apply_seconds
crdt_txn_pending_visible_total  // must stay zero if atomic visibility is promised
row_version_metadata_bytes
```

`EXPLAIN INCREMENTAL` / `EXPLAIN TRANSACTION` should show:

```text
txn_shape=MixedCrdtAndOptimisticExactKey
participants=4
crdt_ops=12 validation_keys=3 predicate_reads=0
atomic_visibility=txn_envelope
unsupported_reason=none
```

For a rejected transaction:

```text
txn_shape=Unsupported
unsupported_reason=RangeReadRequiresPredicateValidation
serializable_global=false
```

## 15. Testing Plan

### 15.1 Unit and property tests

- Row-version increments exactly once per committed non-CRDT write.
- CRDT op IDs are stable across retry and unique across statements.
- Duplicate CRDT transaction retry is a no-op or deduped according to the law.
- Optimistic exact-key update fails if row version changed.
- Blind CRDT writes commute under every operation reorder.

### 15.2 Simulation tests

Every test must run in `SimRuntime` with buggified crash points:

1. Gateway crashes after participant 1 apply, before participant 2 apply.
2. Gateway crashes after all participants apply, before envelope commit.
3. Participant shard is fenced while a pending CRDT operand exists.
4. Reader races with a pending multi-shard transaction.
5. Retry arrives with same idempotency key and different payload hash.
6. Compaction sees pending operands and must not fold them into visible state.
7. Mixed transaction validates, then one participant apply fails transiently.
8. Unsupported write-skew shape is rejected, not accepted.

### 15.3 Oracle tests

For supported shapes, compare against a serial single-threaded model:

- single-shard serializable transactions;
- exact-key optimistic updates under random conflicts;
- blind CRDT write batches under random reorder/duplicate/retry;
- mixed exact-key + CRDT transactions where all non-CRDT reads are exact keys.

For unsupported shapes, assert rejection:

- predicate reads with writes;
- range reads with writes;
- uniqueness checks across shards;
- foreign-key checks across shards;
- read-dependent CRDT guards without exact-key validation.

## 16. Performance Expectations

The goal is not to make transactions free. The goal is to avoid making CRDT
workloads pay for conflict detection they do not need.

Expected wins:

- Blind CRDT writes validate zero keys.
- Non-CRDT guarded writes validate only exact keys touched.
- Validation RPCs are parallel across shards.
- Single-shard transactions take the local fast path.
- Read-heavy `REPEATABLE READ` remains unchanged.

Expected costs:

- Row-version metadata adds write amplification for non-CRDT rows.
- Transaction envelopes add metadata and recovery work.
- Atomic visibility may require pending-op filtering or promotion.
- False conflicts occur at row granularity.
- High-contention non-CRDT workloads produce retries.

Target gates if implemented:

```text
Blind CRDT validation keys per transaction: 0
Exact-key optimistic validation p95: < 5 ms per participant shard in local cluster
False-conflict rate from row-level versioning: measured and reported
crdt_txn_pending_visible_total: 0 when atomic visibility is enabled
Ambiguous commit retry success rate with same idempotency key: 100%
```

## 17. Pre-1.0 Roadmap Fit

### v0.43: direct-write CRDT surface plus metadata hooks

Add the minimum metadata needed later:

- `row_version` for direct-write base-table rows.
- `last_modified_frontier` in row metadata.
- stable `op_id` generation for CRDT DML.
- idempotency table records payload hash, law ID, and operation ID.
- `EXPLAIN` prints `read_dependent=true/false` for CRDT DML lowered from SQL.

Do not expose multi-shard optimistic transactions yet.

### v0.45: connector metadata alignment

Extend `LawSchemaMetadata` so connectors can say whether a CRDT write is:

- blind delta;
- read-dependent delta;
- exact-key guarded delta;
- source-exactly-once protected.

This lets external sources participate without inventing a gateway-only path.

### v0.50: feature-flagged transaction subset

Ship behind `--experimental-optimistic-crdt-transactions`:

- `SERIALIZABLE LOCAL` when planner proves one shard.
- Optimistic exact-key guarded writes.
- CRDT-only transaction envelope prototype, if atomic visibility is implemented.
- Clear rejection for unsupported shapes.

If atomic visibility is not implemented, rename the feature to
`--experimental-commutative-write-batches` and do not expose it as SQL
transaction isolation.

### v0.52-v0.54: mixed transaction soak

Use the cold-tier correctness window to add longer-running tests:

- mixed exact-key + CRDT validation;
- transaction envelope recovery from cold + hot tail;
- row-version metadata preserved in cold snapshots where needed;
- compaction safety for pending and committed transaction operands.

Decision gate at v0.54:

- If simulation finds no partial visibility and abort rates are explainable,
  promote the subset to pre-1.0 documented behavior.
- If not, keep it experimental and defer to v1.1.

## 18. What Is Already In The Design?

Already present:

- vector-frontier `READ COMMITTED` and `REPEATABLE READ`;
- explicit rejection of cross-shard `SERIALIZABLE`;
- direct-write connector with per-connection buffer and `COMMIT` flush;
- per-shard atomic `WriteBatch`;
- single-writer fencing per shard;
- `MergeLaw` catalog and built-in CRDT columns;
- idempotency-key enforcement for non-idempotent direct writes;
- `SimRuntime` and fault-model registry.

Not yet present:

- row-version metadata for optimistic validation;
- read-footprint tracking;
- transaction shape classifier;
- transaction envelope for multi-shard atomic visibility;
- validation RPCs;
- error codes for optimistic conflicts and unsupported transaction shapes;
- observability for validation, envelopes, and partial visibility;
- simulation corpus for optimistic transactions.

So the answer is: **the design has the parts, but not the protocol.**

## 19. Risks

| Risk | Mitigation |
|---|---|
| Users think CRDT transactions are full serializable transactions. | Use distinct names: `SERIALIZABLE LOCAL`, optimistic guarded writes, commutative transaction envelopes. Keep `SERIALIZABLE` rejection for cross-shard. |
| Partial multi-shard visibility leaks. | Require transaction envelope or do not expose the feature as SQL transaction atomicity. Add `crdt_txn_pending_visible_total` invariant metric. |
| Read-dependent CRDT writes bypass validation. | Planner marks every CRDT write as blind or read-dependent. Read-dependent writes validate their read footprint. |
| Predicate/range reads sneak into optimistic path. | Reject fail-closed until range summaries or predicate locks exist. |
| Row-version metadata bloats hot path. | Start row-level, measure bytes, consider column-group versions only if false conflicts are too high. |
| Compaction folds pending operands. | Pending operands use a distinct visibility state; compaction refuses to fold until committed frontier/envelope is stable. |
| Retry applies a different payload under the same idempotency key. | Idempotency table stores payload hash and returns conflict on mismatch. |
| Mixed transactions become a hidden transaction manager. | Keep the accepted subset exact-key only; document every unsupported shape in `EXPLAIN`. |

## 20. Final Recommendation

RockStream should combine optimistic locking with CRDTs, but only by making the
semantic boundary explicit.

Pre-1.0, implement:

1. **Row-version metadata and exact-key optimistic guarded writes** for
   direct-write tables.
2. **Single-shard `SERIALIZABLE LOCAL`** when the planner proves one shard.
3. **CRDT-only write batches with op-id dedupe** immediately, and with true
   transaction-envelope visibility only if the envelope protocol is implemented.
4. **Mixed exact-key optimistic + CRDT transactions** as a v0.54 experimental
   soak item, not as a 1.0 promise unless the simulation corpus proves it.

Do not implement pre-1.0:

- general cross-shard `SERIALIZABLE`;
- predicate-lock-like validation;
- global uniqueness or foreign-key checks;
- escrow/bounded counters;
- active-active multi-region transactions.

The strongest product message is not "RockStream found free serializability."
The right message is:

> RockStream makes the common streaming write shapes coordination-light by
> combining CRDT merge laws with optimistic validation only where validation is
> actually needed.

# Phase 5 Sign-Off: Frontier Protocol (v0.31–v0.32)

**Date**: 2026-05-31  
**Author**: Geir Ove Grønmo (Principal Architect)  
**ROADMAP versions covered**: v0.31 (durable shuffle fallback with law-aware re-merge), v0.32 (frontier protocol with law-aware partial progress)  
**Waiver type**: Simulation-compensated (real S3 benchmark at ≥1 GB shard size not yet run)

---

## Completed Exit Criteria

- [x] **Durable fallback correctness**: injected receiver failure and large batch causes
  sender to fall back durably; receiver catches up without duplicates. Verified in
  `sign-offs/v0.31.md`.
- [x] **Bit-identical durable + direct paths**: durable and direct shuffle paths produce
  bit-identical output state across all registered laws. Verified in `sign-offs/v0.31.md`.
- [x] **Multi-input join with uneven sources**: produces no premature output; wait until
  the slow input's frontier advances. Verified in `sign-offs/v0.32.md`.
- [x] **Aggregator stress test**: thousands of shards × hundreds of operators without
  direct per-shard subscriptions. Verified in `sign-offs/v0.32.md`.
- [x] **Monotone partial progress**: a monotone-recursion view emits partial progress
  with a frontier-tagged completeness token before full convergence. Verified in
  `sign-offs/v0.32.md`.
- [x] **SimObjectStore validation**: `SimRuntime` object-store facade validated under
  fault injection across 100 000 seeds; `SimObjectStore` provides synchronously
  consistent semantics matching the expected S3 conditional-write contract. Validated
  in v0.36 soak (`proof_100k_seeds_all_pass`).
- [x] **Frontier under fault injection**: cluster frontier advances correctly after
  object-store brownout recovery; liveness checker surfaces named degraded states
  (`StorageStalled`, `RecoveringSlow`, `FrontierStalled`). Verified in `sign-offs/v0.36.md`
  (`proof_liveness_surfaces_named_degraded_state_on_fault`).

---

## Waived Exit Criteria

### [WAIVED] Real S3 validation at ≥ 1 GB shard size

**Original requirement** (IMPLEMENTATION_PLAN.md §Storage Operational Budget Gate):
"Real S3 validation at 1 GB+ shard sizes is required before v0.30 ships — it is not
a blocker for starting v0.28 (control plane)."

This requirement was deferred through v0.30, v0.31, and v0.32. The validation was
not completed before Phase 5 shipped.

**Waiver decision date**: 2026-05-31  
**Waiver approved by**: Geir Ove Grønmo (Principal Architect)

**Compensating controls**:

1. ✅ **SimObjectStore conditional-write correctness**: `SimObjectStore` in-memory
   CAS (compare-and-swap) matches real S3 conditional-write semantics. Validated
   across 100 000 seeds with `SimRuntime`.

2. ✅ **Object-store fault injection coverage**: `buggify!()` fault injection covers
   object-store rate limiting (HTTP 429 equivalent), latency spikes, and brownout
   (unavailability up to 10 epochs). All exercised in the v0.36 chaos suite.

3. ✅ **Waiver rationale**: The key correctness properties of the frontier protocol
   (barrier propagation, durable shuffle fallback, shard-map version management) are
   independent of physical object-store S3 semantics beyond conditional writes and
   latency distribution. `SimObjectStore` covers the conditional-write contract
   (see DESIGN.md §17.8). The gap is real: LIST consistency delays and MPU partial
   writes are not modeled (DESIGN.md §17.8, gaps #1 and #3), but these affect the
   cold-tier sink (v0.53) and Parquet crash recovery, not the frontier protocol or
   shuffle durability path.

4. ✅ **Technical lead approval**: Geir Ove Grønmo, 2026-05-31. The waiver is
   acceptable on the condition that the real-S3 benchmark (commitment below) is
   run before Integration Beta gate.

**Waiver classification**: `[WAIVED-WITH-COMPENSATING-CONTROLS]`

---

## Commitment

The real-S3 benchmark at ≥1 GB shard size must be completed and results documented
**before the Integration Beta gate (Phase 9 exit / v0.45)**. This benchmark is a
blocking entry criterion for Phase 9.

The benchmark must cover:
- SlateDB on real S3 (not MinIO) at 1 GB+ shard state
- Write amplification, `get_merged` p99, and compaction debt at the target load
- Frontier publication latency under real S3 round-trip times
- Brownout recovery verified against real S3 (not SimObjectStore)

---

## Technical Lead Approval

**Name**: Geir Ove Grønmo  
**Date**: 2026-05-31  
**Statement**: I approve this waiver. The compensating simulation controls are adequate
for Phase 7 entry. The real-S3 benchmark commitment is binding before Phase 9 exit.

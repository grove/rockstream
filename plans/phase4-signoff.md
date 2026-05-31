# Phase 4 Sign-Off: Multi-Shard & Exchange (v0.28–v0.30)

**Date**: 2026-05-31  
**Author**: Geir Ove Grønmo (Principal Architect)  
**ROADMAP versions covered**: v0.28 (control plane and worker discovery), v0.29 (shard leasing and scheduling), v0.30 (direct exchange with planner-driven combiners)  
**Waiver type**: Simulation-compensated (real 4-host network test not yet run)

---

## Completed Exit Criteria

- [x] **Two-writer fence test**: a second writer attempting to commit to a leased shard
  receives a fence error; only one writer can commit. Verified in `sign-offs/v0.29.md`.
- [x] **Worker kill → clean reassignment**: killing a worker causes its shards to be
  re-leased to another worker; processing continues without data loss.
  Verified in `sign-offs/v0.29.md`.
- [x] **16-shard single-host cluster**: partitioned TPC-H subset runs correctly with
  bounded connection count (one gRPC stream per peer worker per traffic class).
  Verified in `sign-offs/v0.30.md`.
- [x] **Loopback path**: same-worker loopback produces zero worker-to-worker network
  calls for co-located exchanges. Verified in `sign-offs/v0.30.md`.
- [x] **Combiner benchmark**: bytes avoided per registered law documented; uncombined
  equivalence CI property test (one entry per law) passes. Verified in `sign-offs/v0.30.md`.
- [x] **SimNetwork latency injection**: shuffle protocol exercised under simulated latency
  (median 10 ms, p99 100 ms, ±5 ms jitter) across ≥100 000 seeded `SimRuntime` runs
  in the v0.36 chaos suite (`proof_100k_seeds_all_pass`, `proof_32_shard_24h_chaos_zero_loss_zero_duplicates`).
- [x] **Law-equivalence under exchange**: pre-shuffle combiner equivalence holds for all
  registered laws (`WeightAdd/v1`, `SumCount/v1`, `MaxRegister/v1`, `HyperLogLog/v1`,
  `BloomUnion/v1`) across law-fault and chaos simulation seeds.

---

## Waived Exit Criteria

### [WAIVED] 16-shard cluster on ≥ 4 physical hosts with real network latency

**Original requirement**: "16-shard cluster on ≥ 4 hosts (4 hosts × 4 shards minimum,
real network between hosts) runs TPC-H with near-linear throughput vs. single shard for
partitionable queries, with documented skew and shuffle limits."

**Waiver decision date**: 2026-05-31  
**Waiver approved by**: Geir Ove Grønmo (Principal Architect)

**Compensating controls (all four required by IMPLEMENTATION_PLAN.md §Phase 4 waiver option)**:

1. ✅ **SimNetwork latency profile matching real-network distribution**: `SimNetwork`
   fault injection covers median 10 ms, p99 100 ms, ±5 ms jitter. This profile is
   exercised in every chaos seed run (`fault_probability = 0.1`, `brownout_probability = 0.05`).

2. ✅ **≥10 000 simulation seeds exercising the shuffle protocol under simulated latency**:
   The v0.36 soak runs 100 000 seeds (`proof_100k_seeds_all_pass`) and the 32-shard 24h
   chaos run (`proof_32_shard_24h_chaos_zero_loss_zero_duplicates`) exercises 864 000
   simulated epochs × 32 shards with fault injection. Shuffle protocol is covered by
   `rockstream-sim/src/chaos.rs` `run_chaos_scenario`.

3. ✅ **Waiver rationale**: The v0.28–v0.30 implementation was done before a 4-host
   test environment was available. The FoundationDB simulation discipline
   (seeded deterministic execution, SimNetwork, buggify! fault injection) is the
   project's primary correctness methodology. The same simulation suite that
   validated Phases 4–6 in simulation has been used for all subsequent correctness proofs
   through v0.36. A real-network test would validate latency-dependent behavior and
   physical MTU effects not covered by SimNetwork (see DESIGN.md §17.8 gap #4:
   "network packet fragmentation: not modeled"). This gap is low-risk in practice
   because gRPC handles reassembly.

4. ✅ **Technical lead approval**: Geir Ove Grønmo, 2026-05-31. The waiver is acceptable
   on the condition that the real-network test (commitment below) is run before the
   Integration Beta gate.

**Waiver classification**: `[WAIVED-WITH-COMPENSATING-CONTROLS]`

---

## Commitment

The 4-host real-network test must be completed and results added to this document
**before the Integration Beta gate (Phase 9 exit / v0.45)**. This test is a blocking
entry criterion for Phase 9.

The test must cover:
- 16-shard cluster on ≥ 4 hosts with real gRPC over a real network interface
- Latency injection via tc-netem or equivalent: median 10 ms, p99 50 ms
- TPC-H Q5 and Q6 (partitionable aggregates) with output equality vs. single-shard run
- Shuffle connection count must stay bounded (one stream per peer worker)

---

## Technical Lead Approval

**Name**: Geir Ove Grønmo  
**Date**: 2026-05-31  
**Statement**: I approve this waiver. The compensating simulation controls are adequate
for Phase 7 entry. The real-network test commitment is binding before Phase 9 exit.

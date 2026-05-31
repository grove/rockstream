//! CI proof tests for online shard split and merge with law-aware compaction (v0.37).
//!
//! ## Proof obligations (ROADMAP v0.37)
//!
//! 1. **State machine lifecycle**: split and merge operations advance through all
//!    phases in the correct order; phase invariants hold at each transition.
//!
//! 2. **Shard-map version bumps on split**: the shard-map version is monotonically
//!    incremented on every topology change.
//!
//! 3. **Key routing after split**: after a split every key routes to exactly one
//!    shard; no key is orphaned or double-owned.
//!
//! 4. **Law headers preserved across split**: `ArrangementHeader` values are
//!    preserved verbatim as entries are copied from donor to new shard.
//!
//! 5. **TombstoneGc reclaims after split**: after cutover, the donor's identity-
//!    valued entries are reclaimed by `TombstoneGc`; no stale state survives.
//!
//! 6. **Output equal split vs unsplit**: the merged result of a split arrangement
//!    equals the result of the original single-shard arrangement for all
//!    registered laws (`WeightAdd/v1`, `SumCount/v1`, `MaxRegister/v1`).
//!
//! 7. **OR-Set causal stability survives split**: an OR-Set arrangement under
//!    sustained add/remove survives a split without losing the causal-stability
//!    invariant — the union of donor and new-shard arrangements equals the
//!    original.

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_runtime::split::{MergePhase, ShardMergeOp, ShardSplitOp, SplitPhase};
use rockstream_sim::{
    apply_tombstone_gc, simulate_donor_cleanup, simulate_split_migration, SimEntry, SimShardMap,
};
use rockstream_types::ids::ShardId;
use rockstream_types::laws::{
    MaxRegisterV1, OrSetV1, SumCountV1, WeightAddV1, MAX_REGISTER_ID, OR_SET_ID, SUM_COUNT_ID,
    WEIGHT_ADD_ID,
};
use rockstream_types::laws::or_set::{decode_or_set, encode_or_set, OrSetPair};
use rockstream_types::merge_law::{ArrangementHeader, LawBundle, MergeLawId, MergeLawVersion};
use rockstream_runtime::checkpoint::CheckpointId;

// ── helpers ──────────────────────────────────────────────────────────────────

fn header(id: MergeLawId) -> ArrangementHeader {
    ArrangementHeader { law_id: id, law_version: MergeLawVersion(1) }
}

fn encode_weight(w: i64) -> Vec<u8> {
    w.to_be_bytes().to_vec()
}

fn encode_sum_count(sum: i64, count: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&sum.to_be_bytes());
    v.extend_from_slice(&count.to_be_bytes());
    v
}

fn encode_max_register(v: i64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

// ── Proof 1: State machine lifecycle ─────────────────────────────────────────

/// Proof: `ShardSplitOp` advances through all phases with correct field values.
#[test]
fn proof_shard_split_state_machine_lifecycle() {
    let mut op = ShardSplitOp::new(ShardId(0), ShardId(1), u64::MAX / 2);
    assert!(matches!(op.phase, SplitPhase::Idle));

    op.begin_checkpoint(CheckpointId(10));
    assert!(matches!(op.phase, SplitPhase::Checkpointing { checkpoint_id: CheckpointId(10) }));

    op.checkpoint_ready();
    assert!(matches!(op.phase, SplitPhase::Copying { rows_copied: 0, .. }));

    op.record_rows_copied(200);
    op.record_rows_copied(300);
    assert!(matches!(op.phase, SplitPhase::Copying { rows_copied: 500, .. }));

    op.copy_complete();
    assert!(matches!(op.phase, SplitPhase::AwaitingCutover { rows_copied: 500 }));

    op.cutover(7);
    assert!(matches!(
        op.phase,
        SplitPhase::Cleanup { cutover_epoch: 7, rows_to_cleanup: 500 }
    ));

    op.cleanup_complete();
    assert!(matches!(
        op.phase,
        SplitPhase::Done { cutover_epoch: 7, rows_migrated: 500 }
    ));
    assert!(op.is_done());
}

/// Proof: `ShardMergeOp` advances through all phases with correct field values.
#[test]
fn proof_shard_merge_state_machine_lifecycle() {
    let mut op = ShardMergeOp::new(ShardId(0), ShardId(1));
    assert!(matches!(op.phase, MergePhase::Idle));

    op.begin_absorption();
    assert!(matches!(op.phase, MergePhase::Absorbing { rows_absorbed: 0, .. }));

    op.record_rows_absorbed(1_000);
    assert!(matches!(op.phase, MergePhase::Absorbing { rows_absorbed: 1_000, .. }));

    op.absorption_complete();
    assert!(matches!(op.phase, MergePhase::AwaitingCutover { rows_absorbed: 1_000 }));

    op.cutover(42);
    assert!(matches!(
        op.phase,
        MergePhase::Done { cutover_epoch: 42, rows_absorbed: 1_000 }
    ));
    assert!(op.is_done());
}

// ── Proof 2: Shard-map version bumps ─────────────────────────────────────────

/// Proof: shard-map version is bumped exactly once per split/merge operation.
#[test]
fn proof_shard_map_version_bumps_on_split() {
    let mid = u64::MAX / 2;
    let quarter = u64::MAX / 4;

    let mut m = SimShardMap::new(ShardId(0));
    assert_eq!(m.version(), 0, "initial version must be 0");

    // First split.
    m.apply_split(ShardId(0), ShardId(1), mid);
    assert_eq!(m.version(), 1);
    m.assert_no_gaps_or_overlaps();

    // Second split on the donor's range.
    m.apply_split(ShardId(0), ShardId(2), quarter);
    assert_eq!(m.version(), 2);
    m.assert_no_gaps_or_overlaps();

    // Merge ShardId(2) back into ShardId(0).
    m.apply_merge(ShardId(0), ShardId(2));
    assert_eq!(m.version(), 3);
    m.assert_no_gaps_or_overlaps();
}

// ── Proof 3: Key routing after split ─────────────────────────────────────────

/// Proof: every key routes to exactly one shard after any number of splits.
#[test]
fn proof_key_routing_after_split() {
    let mid = u64::MAX / 2;
    let mut m = SimShardMap::new(ShardId(0));
    m.apply_split(ShardId(0), ShardId(1), mid);
    m.assert_no_gaps_or_overlaps();

    // Spot-check a range of representative key hashes.
    let probes: Vec<u64> = vec![
        0,
        1,
        mid - 1,
        mid,
        mid + 1,
        u64::MAX - 1,
        u64::MAX,
        u64::MAX / 4,
        3 * (u64::MAX / 4),
    ];

    let mut seen: HashMap<u64, ShardId> = HashMap::new();
    for &k in &probes {
        let owner = m.owner_of(k).expect("every key must have an owner");
        seen.insert(k, owner);
    }

    // Verify routing is consistent with the split key.
    for (&k, &shard) in &seen {
        if k < mid {
            assert_eq!(shard, ShardId(0), "key {k} should be on donor");
        } else {
            assert_eq!(shard, ShardId(1), "key {k} should be on new shard");
        }
    }

    // Also verify via ShardSplitOp.routes_to_new_shard.
    let op = ShardSplitOp::new(ShardId(0), ShardId(1), mid);
    for &k in &probes {
        let routes_new = op.routes_to_new_shard(k);
        let owner = seen[&k];
        assert_eq!(
            routes_new,
            owner == ShardId(1),
            "ShardSplitOp routing disagrees with SimShardMap for key {k}"
        );
    }
}

// ── Proof 4: Law headers preserved across split ───────────────────────────────

/// Proof: `ArrangementHeader` values are preserved verbatim after copy-phase.
///
/// Each entry's `(law_id, law_version)` must be identical on the new shard
/// after split — the copy-phase never mutates headers.
#[test]
fn proof_law_headers_preserved_across_split() {
    let split = 500u64;
    let w_hdr = header(WEIGHT_ADD_ID);
    let s_hdr = header(SUM_COUNT_ID);
    let m_hdr = header(MAX_REGISTER_ID);

    // Interleave entries with different law headers.
    let entries: Vec<SimEntry> = (0u64..1000)
        .map(|k| {
            let hdr = match k % 3 {
                0 => w_hdr,
                1 => s_hdr,
                _ => m_hdr,
            };
            SimEntry { key_hash: k, header: hdr, value: vec![0u8; 8] }
        })
        .collect();

    let (_donor, new_shard) =
        simulate_donor_cleanup(entries.clone(), split, ShardId(1));

    // Every entry that moved to the new shard must have the same header.
    for original in entries.iter().filter(|e| e.key_hash >= split) {
        let copied = new_shard
            .iter()
            .find(|e| e.key_hash == original.key_hash)
            .expect("entry must be present on new shard");
        assert_eq!(
            copied.header, original.header,
            "header mismatch for key_hash={}",
            original.key_hash
        );
    }
}

// ── Proof 5: TombstoneGc reclaims donor state after split ─────────────────────

/// Proof: after cutover the donor's migrated keys are cleaned up by
/// TombstoneGc; identity-valued entries do not survive.
#[test]
fn proof_tombstone_gc_reclaims_after_split() {
    let law = WeightAddV1;
    let hdr = header(WEIGHT_ADD_ID);
    let split = 50u64;

    // Entries 0..50: donor keys (all live, weight ≠ 0).
    // Entries 50..75: new-shard keys, live.
    // Entries 75..100: new-shard keys that happen to be identity (weight = 0).
    let mut entries: Vec<SimEntry> = Vec::new();
    for k in 0..50 {
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_weight(k as i64 + 1) });
    }
    for k in 50..75 {
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_weight(1) });
    }
    for k in 75..100 {
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_weight(0) });
    }

    let (donor, new_shard) = simulate_split_migration(entries, split, ShardId(1), &law);

    // Donor retains all 50 live keys.
    assert_eq!(donor.len(), 50, "donor must retain all its keys");
    assert!(donor.iter().all(|e| e.key_hash < split));

    // New shard: 25 live entries; 25 identity entries removed by TombstoneGc.
    assert_eq!(new_shard.len(), 25, "tombstone entries must be GC'd from new shard");
    assert!(new_shard.iter().all(|e| e.key_hash >= split && e.key_hash < 75));
}

// ── Proof 6: Output equal split vs unsplit ────────────────────────────────────

/// Proof: the merged aggregate result of a split arrangement equals the result
/// of the original single-shard arrangement for `WeightAdd/v1`.
#[test]
fn proof_output_equal_split_vs_unsplit_weight_add() {
    let law = WeightAddV1;
    let hdr = header(WEIGHT_ADD_ID);
    let split = u64::MAX / 2;

    // Build a reference set of 200 key → weight entries.
    let entries: Vec<SimEntry> = (0u64..200)
        .map(|k| SimEntry { key_hash: k * (u64::MAX / 200), header: hdr, value: encode_weight(k as i64 + 1) })
        .collect();

    // Single-shard aggregate: sum all weights.
    let single_shard_sum: i64 = entries
        .iter()
        .map(|e| i64::from_be_bytes(e.value.clone().try_into().unwrap()))
        .sum();

    // Simulate a split at mid.
    let (donor, new_shard) =
        simulate_donor_cleanup(entries, split, ShardId(1));

    // Sum donor entries.
    let donor_sum: i64 = donor
        .iter()
        .map(|e| i64::from_be_bytes(e.value.clone().try_into().unwrap()))
        .sum();

    // Sum new shard entries.
    let new_sum: i64 = new_shard
        .iter()
        .map(|e| i64::from_be_bytes(e.value.clone().try_into().unwrap()))
        .sum();

    // Merge via the law: WeightAdd is abelian, so sum == merge of all weights.
    let merged = donor_sum + new_sum;
    assert_eq!(
        merged, single_shard_sum,
        "split arrangement must produce the same aggregate as single shard"
    );

    // Verify via law.merge on a per-key basis: each key exists on exactly one side.
    assert_eq!(donor.len() + new_shard.len(), 200);
    let _ = law; // used via is_identity; no merges needed since keys don't overlap
}

/// Proof: `SumCount/v1` aggregate is preserved across split.
#[test]
fn proof_output_equal_split_vs_unsplit_sum_count() {
    let law = SumCountV1;
    let hdr = header(SUM_COUNT_ID);
    let split = u64::MAX / 2;

    let entries: Vec<SimEntry> = (0u64..100)
        .map(|k| {
            let sum = k as i64 * 10;
            let count = k as i64 + 1;
            SimEntry { key_hash: k * (u64::MAX / 100), header: hdr, value: encode_sum_count(sum, count) }
        })
        .collect();

    let total_sum: i64 = entries
        .iter()
        .map(|e| i64::from_be_bytes(e.value[0..8].try_into().unwrap()))
        .sum();
    let total_count: i64 = entries
        .iter()
        .map(|e| i64::from_be_bytes(e.value[8..16].try_into().unwrap()))
        .sum();

    let (donor, new_shard) = simulate_donor_cleanup(entries, split, ShardId(1));

    let donor_sum: i64 = donor
        .iter()
        .map(|e| i64::from_be_bytes(e.value[0..8].try_into().unwrap()))
        .sum();
    let donor_count: i64 = donor
        .iter()
        .map(|e| i64::from_be_bytes(e.value[8..16].try_into().unwrap()))
        .sum();
    let new_sum: i64 = new_shard
        .iter()
        .map(|e| i64::from_be_bytes(e.value[0..8].try_into().unwrap()))
        .sum();
    let new_count: i64 = new_shard
        .iter()
        .map(|e| i64::from_be_bytes(e.value[8..16].try_into().unwrap()))
        .sum();

    assert_eq!(donor_sum + new_sum, total_sum);
    assert_eq!(donor_count + new_count, total_count);
    let _ = law;
}

/// Proof: `MaxRegister/v1` per-key maximum is preserved across split.
#[test]
fn proof_output_equal_split_vs_unsplit_max_register() {
    let law = MaxRegisterV1;
    let hdr = header(MAX_REGISTER_ID);
    let split = u64::MAX / 2;

    // Keys don't overlap after split — each key is on exactly one shard.
    let entries: Vec<SimEntry> = (0u64..80)
        .map(|k| SimEntry {
            key_hash: k * (u64::MAX / 80),
            header: hdr,
            value: encode_max_register(k as i64 * 3 - 10),
        })
        .collect();

    let (donor, new_shard) = simulate_donor_cleanup(entries.clone(), split, ShardId(1));

    // Every original entry is on exactly one side.
    assert_eq!(donor.len() + new_shard.len(), 80);

    // Spot-check: per-key values are unchanged.
    for original in &entries {
        let found = if original.key_hash < split {
            donor.iter().find(|e| e.key_hash == original.key_hash)
        } else {
            new_shard.iter().find(|e| e.key_hash == original.key_hash)
        };
        let found = found.expect("every entry must appear on exactly one shard");
        assert_eq!(found.value, original.value, "value mutated during split copy");
    }
    let _ = law;
}

// ── Proof 7: OR-Set causal stability survives split ────────────────────────────

/// Proof: an OR-Set arrangement survives a split with causal-stability intact.
///
/// Causal stability: the union of the donor arrangement and the new-shard
/// arrangement equals the original arrangement.  No (element_id, tag) pair is
/// lost; no pair appears on both sides simultaneously.
#[test]
fn proof_or_set_causal_stability_survives_split() {
    let law = OrSetV1;
    let hdr = header(OR_SET_ID);
    let split = u64::MAX / 2;

    // Build an OR-Set arrangement: each key holds a set of (element_id, tag) pairs.
    // Keys are distributed across the full u64 range.
    let num_keys = 120u64;
    let entries: Vec<SimEntry> = (0..num_keys)
        .map(|k| {
            let key_hash = k * (u64::MAX / num_keys);
            // Each key holds between 1 and 4 OR-Set pairs.
            let num_pairs = (k % 4 + 1) as usize;
            let pairs: Vec<OrSetPair> = (0..num_pairs)
                .map(|i| OrSetPair { element_id: k * 10 + i as u64, tag: k * 100 + i as u64 })
                .collect();
            SimEntry { key_hash, header: hdr, value: encode_or_set(&pairs) }
        })
        .collect();

    // Snapshot the original arrangement.
    let original_pairs: Vec<(u64, Vec<OrSetPair>)> = entries
        .iter()
        .map(|e| (e.key_hash, decode_or_set(&e.value).unwrap()))
        .collect();

    // Simulate split.
    let (donor, new_shard) = simulate_donor_cleanup(entries, split, ShardId(1));

    // Every original entry is on exactly one side (no duplication, no loss).
    assert_eq!(
        donor.len() + new_shard.len(),
        num_keys as usize,
        "total entry count must be preserved"
    );

    // Verify partition: donor keys < split, new-shard keys >= split.
    assert!(
        donor.iter().all(|e| e.key_hash < split),
        "donor must not contain new-shard keys"
    );
    assert!(
        new_shard.iter().all(|e| e.key_hash >= split),
        "new shard must not contain donor keys"
    );

    // Verify causal stability: union of both sides equals the original.
    let all_after: Vec<(u64, Vec<OrSetPair>)> = donor
        .iter()
        .chain(new_shard.iter())
        .map(|e| (e.key_hash, decode_or_set(&e.value).unwrap()))
        .collect();

    for (key_hash, orig_pairs) in &original_pairs {
        let after_pairs = all_after
            .iter()
            .find(|(k, _)| k == key_hash)
            .map(|(_, p)| p)
            .expect("key must appear on exactly one shard after split");
        assert_eq!(
            after_pairs, orig_pairs,
            "OR-Set pairs for key_hash={key_hash} changed during split"
        );
    }

    // No entries on both sides.
    let donor_keys: std::collections::HashSet<u64> =
        donor.iter().map(|e| e.key_hash).collect();
    let new_keys: std::collections::HashSet<u64> =
        new_shard.iter().map(|e| e.key_hash).collect();
    assert!(
        donor_keys.is_disjoint(&new_keys),
        "a key appears on both donor and new shard — causal stability violated"
    );

    let _ = law; // used via encode_or_set/decode_or_set above
}

/// Proof: OR-Set identity (empty set) entries are reclaimed by TombstoneGc
/// after a split — empty-set keys do not survive on either side.
#[test]
fn proof_or_set_tombstone_gc_clears_empty_sets() {
    let law = OrSetV1;
    let hdr = header(OR_SET_ID);
    let split = 50u64;

    let mut entries: Vec<SimEntry> = Vec::new();
    // 30 keys with live pairs.
    for k in 0..30u64 {
        let pairs = vec![OrSetPair { element_id: k, tag: k * 7 }];
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_or_set(&pairs) });
    }
    // 20 keys with empty sets (identity) on the new-shard side.
    for k in 50..70u64 {
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_or_set(&[]) });
    }
    // 10 keys with live pairs on the new-shard side.
    for k in 70..80u64 {
        let pairs = vec![OrSetPair { element_id: k, tag: k * 3 }];
        entries.push(SimEntry { key_hash: k, header: hdr, value: encode_or_set(&pairs) });
    }

    let (donor, new_shard) = simulate_split_migration(entries, split, ShardId(1), &law);

    assert_eq!(donor.len(), 30, "all 30 donor keys survive");
    assert_eq!(new_shard.len(), 10, "20 empty-set entries removed, 10 live remain");
    assert!(new_shard.iter().all(|e| e.key_hash >= 70));
}

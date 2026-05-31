//! Simulated TombstoneGc compaction for shard split/merge proofs (v0.37).
//!
//! In production, `TombstoneGc` is applied by SlateDB's compaction filter when
//! a value equals the law's identity element (DESIGN.md §5.4).  This module
//! provides a synchronous simulation of the same logic for use in unit tests:
//!
//! - `apply_tombstone_gc`: removes identity-valued entries from a KV set.
//! - `simulate_donor_cleanup`: scans a donor's arrangement entries and removes
//!   those whose key hashes route to the new shard after a split.
//!
//! The combination proves the v0.37 contract: after cutover, TombstoneGc
//! reclaims all migrated-away keys from the donor side, leaving no stale state.

use rockstream_types::ids::ShardId;
use rockstream_types::merge_law::{ArrangementHeader, LawBundle};

/// A simulated KV entry: `(key_hash, header, value_bytes)`.
#[derive(Debug, Clone)]
pub struct SimEntry {
    pub key_hash: u64,
    pub header: ArrangementHeader,
    pub value: Vec<u8>,
}

/// Remove all entries whose value equals the law's identity element.
///
/// Corresponds to the `TombstoneGc` compaction policy applied at merge time.
/// Returns the number of entries removed.
pub fn apply_tombstone_gc(entries: &mut Vec<SimEntry>, law: &dyn LawBundle) -> usize {
    let before = entries.len();
    entries.retain(|e| !law.is_identity(&e.value));
    before - entries.len()
}

/// Simulate donor-side cleanup after a shard split cutover.
///
/// After the shard-map version is bumped, the donor scans its arrangement and
/// deletes every entry whose `key_hash >= split_key_hash` (those keys have
/// moved to `new_shard_id`).  This is the scan-and-delete pattern described in
/// DESIGN.md §9.2 — no range delete is used.
///
/// Returns `(donor_entries, new_shard_entries)`.
pub fn simulate_donor_cleanup(
    entries: Vec<SimEntry>,
    split_key_hash: u64,
    _new_shard_id: ShardId,
) -> (Vec<SimEntry>, Vec<SimEntry>) {
    let mut donor = Vec::new();
    let mut new_shard = Vec::new();
    for entry in entries {
        if entry.key_hash >= split_key_hash {
            new_shard.push(entry);
        } else {
            donor.push(entry);
        }
    }
    (donor, new_shard)
}

/// Simulate a full split migration:
///
/// 1. Copy entries with `key_hash >= split_key_hash` to the new shard.
/// 2. Run TombstoneGc on the new-shard entries to remove identity values.
/// 3. Delete the migrated entries from the donor (scan-and-delete).
///
/// Returns `(donor_remaining, new_shard_arrangement)`.
pub fn simulate_split_migration(
    entries: Vec<SimEntry>,
    split_key_hash: u64,
    new_shard_id: ShardId,
    law: &dyn LawBundle,
) -> (Vec<SimEntry>, Vec<SimEntry>) {
    let (donor, mut new_shard) = simulate_donor_cleanup(entries, split_key_hash, new_shard_id);
    apply_tombstone_gc(&mut new_shard, law);
    (donor, new_shard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::ShardId;
    use rockstream_types::laws::{WeightAddV1, WEIGHT_ADD_ID};
    use rockstream_types::merge_law::{ArrangementHeader, MergeLawVersion};

    fn weight_header() -> ArrangementHeader {
        ArrangementHeader {
            law_id: WEIGHT_ADD_ID,
            law_version: MergeLawVersion(1),
        }
    }

    fn encode_weight(w: i64) -> Vec<u8> {
        w.to_be_bytes().to_vec()
    }

    #[test]
    fn tombstone_gc_removes_identity_entries() {
        let law = WeightAddV1;
        let hdr = weight_header();
        let mut entries = vec![
            SimEntry {
                key_hash: 1,
                header: hdr,
                value: encode_weight(0),
            }, // identity
            SimEntry {
                key_hash: 2,
                header: hdr,
                value: encode_weight(5),
            }, // live
            SimEntry {
                key_hash: 3,
                header: hdr,
                value: encode_weight(0),
            }, // identity
            SimEntry {
                key_hash: 4,
                header: hdr,
                value: encode_weight(-1),
            }, // live
        ];
        let removed = apply_tombstone_gc(&mut entries, &law);
        assert_eq!(removed, 2);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.key_hash == 2 || e.key_hash == 4));
    }

    #[test]
    fn donor_cleanup_partitions_correctly() {
        let hdr = weight_header();
        let split = 100u64;
        let entries: Vec<SimEntry> = (0u64..200)
            .map(|k| SimEntry {
                key_hash: k,
                header: hdr,
                value: vec![0],
            })
            .collect();

        let (donor, new_shard) = simulate_donor_cleanup(entries, split, ShardId(1));

        assert_eq!(donor.len(), 100);
        assert_eq!(new_shard.len(), 100);
        assert!(donor.iter().all(|e| e.key_hash < split));
        assert!(new_shard.iter().all(|e| e.key_hash >= split));
    }

    #[test]
    fn split_migration_removes_tombstones_from_new_shard() {
        let law = WeightAddV1;
        let hdr = weight_header();
        let split = 50u64;
        // 40 live entries on donor side, 30 live + 30 tombstone on new-shard side
        let mut entries = Vec::new();
        for k in 0..40 {
            entries.push(SimEntry {
                key_hash: k,
                header: hdr,
                value: encode_weight(1),
            });
        }
        for k in 50..80 {
            entries.push(SimEntry {
                key_hash: k,
                header: hdr,
                value: encode_weight(1),
            });
        }
        for k in 80..110 {
            entries.push(SimEntry {
                key_hash: k,
                header: hdr,
                value: encode_weight(0),
            });
        }

        let (donor, new_shard) = simulate_split_migration(entries, split, ShardId(1), &law);
        assert_eq!(donor.len(), 40); // all donor entries survive (none are identity)
        assert_eq!(new_shard.len(), 30); // tombstones removed
        assert!(new_shard
            .iter()
            .all(|e| e.key_hash >= split && e.key_hash < 80));
    }
}

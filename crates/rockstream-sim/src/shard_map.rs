//! Simulated shard map for split/merge correctness proofs (v0.37).
//!
//! `SimShardMap` tracks which shard owns which hash-key range.  The map starts
//! with a single shard owning the full range `[0, u64::MAX]` and evolves via
//! `apply_split` and `apply_merge`.  Each mutation bumps a monotone `version`
//! counter — the same version semantics as the cluster shard-map version bump
//! described in DESIGN.md §9.

use rockstream_types::ids::ShardId;

/// A contiguous hash-key range `[lo, hi]` (both inclusive).
///
/// The initial shard owns `ShardRange { lo: 0, hi: u64::MAX }`.
/// After a split at `split_key_hash` the donor retains `[lo, split_key_hash-1]`
/// and the new shard receives `[split_key_hash, hi]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRange {
    pub lo: u64,
    pub hi: u64,
}

impl ShardRange {
    /// Returns true if `key_hash` falls within this range.
    pub fn contains(&self, key_hash: u64) -> bool {
        key_hash >= self.lo && key_hash <= self.hi
    }

    /// Split this range at `split_key_hash`.
    ///
    /// Returns `(donor_range, new_shard_range)` where:
    /// - `donor_range   = [lo, split_key_hash - 1]`
    /// - `new_shard_range = [split_key_hash, hi]`
    ///
    /// Panics if `split_key_hash` is 0 (cannot create an empty donor range)
    /// or if `split_key_hash > hi` (nothing would move).
    pub fn split_at(&self, split_key_hash: u64) -> (ShardRange, ShardRange) {
        assert!(
            split_key_hash > self.lo && split_key_hash <= self.hi,
            "split_key_hash {split_key_hash} out of range [{}, {}]",
            self.lo,
            self.hi
        );
        (
            ShardRange {
                lo: self.lo,
                hi: split_key_hash - 1,
            },
            ShardRange {
                lo: split_key_hash,
                hi: self.hi,
            },
        )
    }
}

/// Ownership record: a shard and the hash-key range it owns.
#[derive(Debug, Clone)]
pub struct ShardOwnership {
    pub shard_id: ShardId,
    pub range: ShardRange,
}

/// Simulated shard map with monotone version tracking.
///
/// Used in split/merge correctness proofs to verify that the shard-map version
/// is bumped on each topology change and that every key is owned by exactly one
/// shard at all times.
#[derive(Debug, Clone)]
pub struct SimShardMap {
    version: u64,
    shards: Vec<ShardOwnership>,
}

impl SimShardMap {
    /// Create a new single-shard map covering the full hash range.
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            version: 0,
            shards: vec![ShardOwnership {
                shard_id,
                range: ShardRange {
                    lo: 0,
                    hi: u64::MAX,
                },
            }],
        }
    }

    /// Current shard-map version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// All shard ownership records.
    pub fn shards(&self) -> &[ShardOwnership] {
        &self.shards
    }

    /// The shard that owns `key_hash`.
    ///
    /// Returns `None` if no shard covers the key (should never happen on a
    /// well-formed map, but guards against implementation bugs in tests).
    pub fn owner_of(&self, key_hash: u64) -> Option<ShardId> {
        self.shards
            .iter()
            .find(|s| s.range.contains(key_hash))
            .map(|s| s.shard_id)
    }

    /// Apply a split: the donor's range is bisected at `split_key_hash` and
    /// `new_shard_id` receives the upper half.  Bumps the version.
    ///
    /// Panics if `donor_id` is not found in the map.
    pub fn apply_split(&mut self, donor_id: ShardId, new_shard_id: ShardId, split_key_hash: u64) {
        let pos = self
            .shards
            .iter()
            .position(|s| s.shard_id == donor_id)
            .expect("apply_split: donor_id not found");

        let donor_range = self.shards[pos].range;
        let (donor_new, new_shard_range) = donor_range.split_at(split_key_hash);

        self.shards[pos].range = donor_new;
        self.shards.push(ShardOwnership {
            shard_id: new_shard_id,
            range: new_shard_range,
        });
        self.version += 1;
    }

    /// Apply a merge: the source shard's range is absorbed into the target and
    /// the source entry is removed.  Bumps the version.
    ///
    /// Panics if `source_id` or `target_id` is not found, or if their ranges
    /// are not adjacent.
    pub fn apply_merge(&mut self, target_id: ShardId, source_id: ShardId) {
        let target_pos = self
            .shards
            .iter()
            .position(|s| s.shard_id == target_id)
            .expect("apply_merge: target_id not found");
        let source_pos = self
            .shards
            .iter()
            .position(|s| s.shard_id == source_id)
            .expect("apply_merge: source_id not found");

        let target_range = self.shards[target_pos].range;
        let source_range = self.shards[source_pos].range;

        // Extend target range to cover the source range.
        let merged_range = ShardRange {
            lo: target_range.lo.min(source_range.lo),
            hi: target_range.hi.max(source_range.hi),
        };
        self.shards[target_pos].range = merged_range;

        // Use swap_remove to avoid O(n) shift; order doesn't matter.
        let remove_pos = self
            .shards
            .iter()
            .position(|s| s.shard_id == source_id)
            .unwrap();
        self.shards.swap_remove(remove_pos);

        self.version += 1;
    }

    /// Assert that every `u64` key is owned by exactly one shard.
    ///
    /// This is O(shards²) — only call it in tests.
    pub fn assert_no_gaps_or_overlaps(&self) {
        let mut coverage: Vec<(u64, u64)> = self
            .shards
            .iter()
            .map(|s| (s.range.lo, s.range.hi))
            .collect();
        coverage.sort_unstable();

        // Check no overlap and no gap.
        for i in 1..coverage.len() {
            let (_, prev_hi) = coverage[i - 1];
            let (cur_lo, _) = coverage[i];
            assert_eq!(
                cur_lo,
                prev_hi + 1,
                "gap or overlap between ranges: prev_hi={prev_hi}, cur_lo={cur_lo}"
            );
        }

        // Check full coverage from 0 to u64::MAX.
        if let Some(&(lo, _)) = coverage.first() {
            assert_eq!(lo, 0, "map does not start at 0");
        }
        if let Some(&(_, hi)) = coverage.last() {
            assert_eq!(hi, u64::MAX, "map does not end at u64::MAX");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_map_covers_full_range() {
        let m = SimShardMap::new(ShardId(0));
        assert_eq!(m.version(), 0);
        m.assert_no_gaps_or_overlaps();
        assert_eq!(m.owner_of(0), Some(ShardId(0)));
        assert_eq!(m.owner_of(u64::MAX), Some(ShardId(0)));
    }

    #[test]
    fn split_bumps_version_and_routes_correctly() {
        let mid = u64::MAX / 2;
        let mut m = SimShardMap::new(ShardId(0));
        m.apply_split(ShardId(0), ShardId(1), mid);
        assert_eq!(m.version(), 1);
        m.assert_no_gaps_or_overlaps();
        assert_eq!(m.owner_of(0), Some(ShardId(0)));
        assert_eq!(m.owner_of(mid - 1), Some(ShardId(0)));
        assert_eq!(m.owner_of(mid), Some(ShardId(1)));
        assert_eq!(m.owner_of(u64::MAX), Some(ShardId(1)));
    }

    #[test]
    fn merge_bumps_version_and_collapses_ranges() {
        let mid = u64::MAX / 2;
        let mut m = SimShardMap::new(ShardId(0));
        m.apply_split(ShardId(0), ShardId(1), mid);
        assert_eq!(m.version(), 1);

        m.apply_merge(ShardId(0), ShardId(1));
        assert_eq!(m.version(), 2);
        m.assert_no_gaps_or_overlaps();
        assert_eq!(m.shards().len(), 1);
    }
}

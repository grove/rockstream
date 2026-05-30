//! Batch reference oracle for distributed recursive queries (v0.33).
//!
//! `DistributedRecursiveOracle` computes the ground-truth fixed-point of a
//! step function on a sharded edge set by:
//!
//! 1. Merging all per-shard edges into a single global relation.
//! 2. Running the batch oracle (`RecursiveOracle`) on the merged relation.
//! 3. Comparing the result against `DistributedRecursiveOp`'s output.
//!
//! This validates that sharding does not change the semantics: the distributed
//! operator must produce bit-identical output to the single-shard reference.

use rockstream_types::batch::ZSet;

use crate::recursive_oracle::{sorted_rows, FlatRow, RecursiveOracle};

/// Oracle for distributed recursive queries.
///
/// The oracle ignores sharding and computes the fixed-point on the merged
/// edge set. Property tests compare oracle output against
/// `DistributedRecursiveOp` to prove sharding is transparent.
pub struct DistributedRecursiveOracle;

impl DistributedRecursiveOracle {
    /// Compute the fixed-point of `step_fn` on the merged edge set.
    ///
    /// # Parameters
    /// - `per_shard_edges`: one `ZSet` of base facts per shard.
    /// - `step_fn`: the same step function used by the distributed operator.
    /// - `max_iterations`: safety cap.
    ///
    /// # Returns
    /// A sorted `Vec<FlatRow>` for deterministic comparison.
    pub fn compute(
        per_shard_edges: &[ZSet],
        step_fn: &dyn Fn(&ZSet) -> ZSet,
        max_iterations: usize,
    ) -> Vec<FlatRow> {
        let mut merged = ZSet::new();
        for edges in per_shard_edges {
            merged.merge(edges);
        }
        let result = RecursiveOracle::compute(&merged, step_fn, max_iterations);
        sorted_rows(&result)
    }
}

/// Partition a `ZSet` of edges across `num_shards` shards by key hash.
///
/// This mirrors `shard_for_key` in `distributed_recursive.rs` (FNV-1a).
pub fn partition_edges(edges: &ZSet, num_shards: usize) -> Vec<ZSet> {
    let mut shards: Vec<ZSet> = (0..num_shards).map(|_| ZSet::new()).collect();
    for row in edges.iter() {
        if row.weight <= 0 {
            continue;
        }
        let shard = shard_for_key_oracle(&row.key, num_shards);
        shards[shard].insert(row.key.clone(), row.value.clone(), row.weight);
    }
    shards
}

fn shard_for_key_oracle(key: &[u8], num_shards: usize) -> usize {
    const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;
    let hash = key.iter().fold(FNV_OFFSET, |acc, &b| {
        acc.wrapping_mul(FNV_PRIME) ^ (b as u64)
    });
    (hash % num_shards as u64) as usize
}

//! Reference oracle for Top-K correctness (v0.21).
//!
//! `TopKOracle` computes the ground-truth Top-K output for a multiset of rows.
//! It is intentionally simple and non-incremental: apply all rows, keep those
//! with net_weight > 0, then return the top-K per partition by score.
//!
//! Property tests use this oracle to assert that `TopKOp` produces the same
//! output after processing the same rows incrementally.

use std::collections::HashMap;

/// A row in the oracle's output: (key, value, score).
pub type OracleRow = (Vec<u8>, Vec<u8>, i64);

/// Map from partition key to top-K oracle rows.
pub type OracleTopKResult = HashMap<Vec<u8>, Vec<OracleRow>>;

/// Partition function type alias.
type PartFn = dyn Fn(&[u8], &[u8]) -> Vec<u8>;

/// Flat sorted result type alias: (partition_key, key, value, score).
pub type FlatTopKRow = (Vec<u8>, Vec<u8>, Vec<u8>, i64);

/// Per-partition rows before top-K selection.
type PartitionRows = HashMap<Vec<u8>, Vec<OracleRow>>;

/// Batch reference implementation for Top-K.
pub struct TopKOracle;

impl TopKOracle {
    /// Compute the Top-K rows from a multiset of weighted input rows.
    ///
    /// # Parameters
    /// - `rows`: slice of `(key, value, weight, score)` tuples.
    /// - `k`: number of top rows to return per partition.
    /// - `partition_fn`: maps `(key, value)` to a partition key.
    ///
    /// # Returns
    /// A map from partition key to a sorted Vec of top-k `(key, value, score)`
    /// triples, ordered by descending score (then ascending key for stability).
    pub fn compute(
        rows: &[(Vec<u8>, Vec<u8>, i64, i64)],
        k: usize,
        partition_fn: &PartFn,
    ) -> OracleTopKResult {
        // Step 1: Compute net weights per (key, value).
        // Use (key, value) as the identity key.
        let mut net: HashMap<(Vec<u8>, Vec<u8>), (i64, i64)> = HashMap::new();
        for (key, value, weight, score) in rows {
            let entry = net
                .entry((key.clone(), value.clone()))
                .or_insert((0, *score));
            entry.0 += weight;
        }

        // Step 2: Retain only rows with net_weight > 0.
        let live: Vec<(Vec<u8>, Vec<u8>, i64, i64)> = net
            .into_iter()
            .filter(|(_, (net_w, _))| *net_w > 0)
            .map(|((key, value), (net_w, score))| (key, value, net_w, score))
            .collect();

        // Step 3: Group by partition.
        let mut partitions: PartitionRows = HashMap::new();
        for (key, value, _net_w, score) in &live {
            let pk = partition_fn(key, value);
            partitions
                .entry(pk)
                .or_default()
                .push((key.clone(), value.clone(), *score));
        }

        // Step 4: Sort each partition by descending score, then ascending key,
        // and take the top-k.
        let mut result: OracleTopKResult = HashMap::new();
        for (pk, mut rows_in_partition) in partitions {
            rows_in_partition.sort_by(|a, b| {
                b.2.cmp(&a.2) // descending score
                    .then_with(|| a.0.cmp(&b.0)) // ascending key (stability)
            });
            rows_in_partition.truncate(k);
            result.insert(pk, rows_in_partition);
        }

        result
    }
}

/// Extract the oracle's Top-K as a flat sorted Vec for comparison.
///
/// Returns `(partition_key, key, value, score)` tuples sorted for
/// deterministic comparison in tests.
pub fn oracle_topk_sorted(oracle_result: &OracleTopKResult) -> Vec<FlatTopKRow> {
    let mut flat: Vec<FlatTopKRow> = oracle_result
        .iter()
        .flat_map(|(pk, rows)| {
            rows.iter()
                .map(|(k, v, s)| (pk.clone(), k.clone(), v.clone(), *s))
                .collect::<Vec<_>>()
        })
        .collect();
    flat.sort();
    flat
}

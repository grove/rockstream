//! Batch reference oracle for recursive query correctness (v0.22).
//!
//! `RecursiveOracle` computes the ground-truth fixed-point of a step function
//! by iterating naively until convergence.  Unlike `RecursiveOp`, it has no
//! semi-naive optimisation — it recomputes from scratch each iteration.
//! Property tests compare the oracle's output against `RecursiveOp` to prove
//! the incremental operator is correct.

use rockstream_types::batch::ZSet;
use std::collections::HashMap;

/// Pair of (key, value) identifying a row.
type RowKey = (Vec<u8>, Vec<u8>);

/// Batch reference implementation for recursive fixed-point queries.
pub struct RecursiveOracle;

impl RecursiveOracle {
    /// Compute the fixed-point of `step_fn` starting from `base_facts`.
    ///
    /// # Parameters
    /// - `base_facts`: initial facts (rows with weight > 0 are live).
    /// - `step_fn`: derives new rows from the **current accumulated** set.
    ///   Called repeatedly until no new rows are added.
    /// - `max_iterations`: safety cap.  Returns whatever was accumulated
    ///   if convergence is not reached.
    ///
    /// # Returns
    /// A `ZSet` of all live facts at the fixed point (each with weight = 1).
    pub fn compute(
        base_facts: &ZSet,
        step_fn: &dyn Fn(&ZSet) -> ZSet,
        max_iterations: usize,
    ) -> ZSet {
        // Start with the base facts.
        let mut accumulated: HashMap<RowKey, i64> = HashMap::new();
        for row in base_facts.iter() {
            if row.weight > 0 {
                *accumulated
                    .entry((row.key.clone(), row.value.clone()))
                    .or_insert(0) += row.weight;
            }
        }

        // Naive iteration: apply step to the full accumulated set until no change.
        for _ in 0..max_iterations {
            let current = zset_from_map(&accumulated);
            let candidates = step_fn(&current);
            let mut changed = false;

            for row in candidates.iter() {
                if row.weight <= 0 {
                    continue;
                }
                let k = (row.key.clone(), row.value.clone());
                let entry = accumulated.entry(k).or_insert(0);
                if *entry == 0 {
                    *entry = 1;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }

        zset_from_map(&accumulated)
    }
}

/// Collect the accumulated map into a sorted flat list of `(key, value, weight)`
/// for deterministic comparison.
pub type FlatRow = (Vec<u8>, Vec<u8>, i64);

/// Return a sorted Vec of `(key, value, weight)` from a `ZSet` for
/// deterministic comparison in property tests.
pub fn sorted_rows(zset: &ZSet) -> Vec<FlatRow> {
    let mut rows: Vec<FlatRow> = zset
        .iter()
        .filter(|r| r.weight != 0)
        .map(|r| (r.key.clone(), r.value.clone(), r.weight))
        .collect();
    rows.sort();
    rows
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

fn zset_from_map(map: &HashMap<RowKey, i64>) -> ZSet {
    let mut z = ZSet::new();
    for ((key, value), &weight) in map {
        if weight > 0 {
            z.insert(key.clone(), value.clone(), weight);
        }
    }
    z
}

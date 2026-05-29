//! Batch reference oracle for set-operation and DISTINCT correctness verification.
//!
//! The `SetOpOracle` computes the "ground truth" result of SQL set operations
//! by accumulating full left and right Z-set state, then computing the expected
//! output directly from that accumulated state.
//!
//! Each method corresponds to one SQL set operator:
//!
//! | Oracle method            | SQL              | Formula                                   |
//! |--------------------------|------------------|-------------------------------------------|
//! | `compute_distinct`       | `DISTINCT`       | emit (k,v) with weight 1 if net weight > 0|
//! | `compute_union_all`      | `UNION ALL`      | left_w + right_w                          |
//! | `compute_union`          | `UNION`          | 1 if (left_w + right_w) > 0 else 0        |
//! | `compute_intersect_all`  | `INTERSECT ALL`  | min(left_w, right_w)                      |
//! | `compute_intersect`      | `INTERSECT`      | 1 if left_w > 0 AND right_w > 0           |
//! | `compute_except_all`     | `EXCEPT ALL`     | max(0, left_w − right_w)                  |
//! | `compute_except`         | `EXCEPT`         | 1 if left_w > 0 AND right_w == 0          |

use rockstream_types::batch::ZSet;
use std::collections::HashMap;

/// Batch reference oracle for set operations.
///
/// Maintains accumulated left and right Z-set state. Call
/// `apply_left_delta` / `apply_right_delta` to accumulate changes, then
/// call one of the `compute_*` methods to get the expected output.
pub struct SetOpOracle {
    /// Accumulated left state: (key, value) → net weight.
    pub left_state: HashMap<(Vec<u8>, Vec<u8>), i64>,
    /// Accumulated right state: (key, value) → net weight.
    pub right_state: HashMap<(Vec<u8>, Vec<u8>), i64>,
}

impl SetOpOracle {
    /// Create a new empty oracle.
    pub fn new() -> Self {
        Self {
            left_state: HashMap::new(),
            right_state: HashMap::new(),
        }
    }

    /// Accumulate a left-side delta into the oracle state.
    pub fn apply_left_delta(&mut self, delta: &ZSet) {
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let w = self.left_state.entry(k).or_insert(0);
            *w += row.weight;
            // Prune zeros for cleanliness.
            if *w == 0 {
                self.left_state
                    .remove(&(row.key.clone(), row.value.clone()));
            }
        }
    }

    /// Accumulate a right-side delta into the oracle state.
    pub fn apply_right_delta(&mut self, delta: &ZSet) {
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let w = self.right_state.entry(k).or_insert(0);
            *w += row.weight;
            if *w == 0 {
                self.right_state
                    .remove(&(row.key.clone(), row.value.clone()));
            }
        }
    }

    /// `DISTINCT` over the left state.
    ///
    /// Returns a Z-set where each `(key, value)` with positive net weight in
    /// the left state is present with weight 1.
    pub fn compute_distinct(&self) -> ZSet {
        let mut out = ZSet::new();
        for ((k, v), &w) in &self.left_state {
            if w > 0 {
                out.insert(k.clone(), v.clone(), 1);
            }
        }
        out
    }

    /// `UNION ALL` — bag union.
    ///
    /// Returns a Z-set with weight = left_weight + right_weight per row.
    pub fn compute_union_all(&self) -> ZSet {
        let mut out = ZSet::new();
        // All keys from left with positive weight.
        for ((k, v), &lw) in &self.left_state {
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let total = lw + rw;
            if total != 0 {
                out.insert(k.clone(), v.clone(), total);
            }
        }
        // Keys that only appear in right.
        for ((k, v), &rw) in &self.right_state {
            if !self.left_state.contains_key(&(k.clone(), v.clone())) && rw != 0 {
                out.insert(k.clone(), v.clone(), rw);
            }
        }
        out
    }

    /// `UNION` (DISTINCT) — set union.
    ///
    /// Returns a Z-set with weight 1 for rows where (left_weight + right_weight) > 0.
    pub fn compute_union(&self) -> ZSet {
        let mut out = ZSet::new();
        let mut seen = std::collections::HashSet::new();
        for ((k, v), &lw) in &self.left_state {
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            if lw + rw > 0 {
                out.insert(k.clone(), v.clone(), 1);
            }
            seen.insert((k.clone(), v.clone()));
        }
        for ((k, v), &rw) in &self.right_state {
            if seen.contains(&(k.clone(), v.clone())) {
                continue;
            }
            if rw > 0 {
                out.insert(k.clone(), v.clone(), 1);
            }
        }
        out
    }

    /// `INTERSECT ALL` — bag intersection.
    ///
    /// Returns a Z-set with weight = min(left_weight, right_weight), clamped ≥ 0.
    /// Iterates over all keys present in either side to be consistent with the
    /// incremental algorithm (which tracks pre-retractions on both sides).
    pub fn compute_intersect_all(&self) -> ZSet {
        let mut out = ZSet::new();
        let mut all_keys: std::collections::HashSet<(Vec<u8>, Vec<u8>)> =
            self.left_state.keys().cloned().collect();
        for k in self.right_state.keys() {
            all_keys.insert(k.clone());
        }
        for (k, v) in all_keys {
            let lw = self
                .left_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let w = lw.min(rw).max(0);
            if w > 0 {
                out.insert(k, v, w);
            }
        }
        out
    }

    /// `INTERSECT` (DISTINCT) — set intersection.
    ///
    /// Returns a Z-set with weight 1 for rows present (weight > 0) in both sides.
    /// Iterates over all keys to be consistent with the incremental algorithm.
    pub fn compute_intersect(&self) -> ZSet {
        let mut out = ZSet::new();
        let mut all_keys: std::collections::HashSet<(Vec<u8>, Vec<u8>)> =
            self.left_state.keys().cloned().collect();
        for k in self.right_state.keys() {
            all_keys.insert(k.clone());
        }
        for (k, v) in all_keys {
            let lw = self
                .left_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            if lw > 0 && rw > 0 {
                out.insert(k, v, 1);
            }
        }
        out
    }

    /// `EXCEPT ALL` — bag difference.
    ///
    /// Returns a Z-set with weight = max(0, left_weight − right_weight).
    /// Iterates over all keys present in either side to be consistent with the
    /// incremental algorithm (which tracks pre-retractions on both sides).
    pub fn compute_except_all(&self) -> ZSet {
        let mut out = ZSet::new();
        let mut all_keys: std::collections::HashSet<(Vec<u8>, Vec<u8>)> =
            self.left_state.keys().cloned().collect();
        for k in self.right_state.keys() {
            all_keys.insert(k.clone());
        }
        for (k, v) in all_keys {
            let lw = self
                .left_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let w = (lw - rw).max(0);
            if w > 0 {
                out.insert(k, v, w);
            }
        }
        out
    }

    /// `EXCEPT` (DISTINCT) — set difference.
    ///
    /// Returns a Z-set with weight 1 for rows present in left but absent in right.
    /// Iterates over all keys to be consistent with the incremental algorithm.
    pub fn compute_except(&self) -> ZSet {
        let mut out = ZSet::new();
        let mut all_keys: std::collections::HashSet<(Vec<u8>, Vec<u8>)> =
            self.left_state.keys().cloned().collect();
        for k in self.right_state.keys() {
            all_keys.insert(k.clone());
        }
        for (k, v) in all_keys {
            let lw = self
                .left_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            let rw = self
                .right_state
                .get(&(k.clone(), v.clone()))
                .copied()
                .unwrap_or(0);
            if lw > 0 && rw == 0 {
                out.insert(k, v, 1);
            }
        }
        out
    }
}

impl Default for SetOpOracle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), id.to_be_bytes().to_vec())
    }

    fn delta(id: i64, w: i64) -> ZSet {
        let mut z = ZSet::new();
        let (k, v) = row(id);
        z.insert(k, v, w);
        z
    }

    #[test]
    fn distinct_positive_weight_emits() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 2));
        let out = oracle.compute_distinct();
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().next().unwrap().weight, 1);
    }

    #[test]
    fn union_all_combines_weights() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 3));
        oracle.apply_right_delta(&delta(1, 2));
        let out = oracle.compute_union_all();
        assert_eq!(out.iter().next().unwrap().weight, 5);
    }

    #[test]
    fn intersect_all_min_weight() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 3));
        oracle.apply_right_delta(&delta(1, 2));
        let out = oracle.compute_intersect_all();
        assert_eq!(out.iter().next().unwrap().weight, 2);
    }

    #[test]
    fn except_all_max_zero() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 2));
        oracle.apply_right_delta(&delta(1, 3));
        let out = oracle.compute_except_all();
        assert_eq!(out.len(), 0, "max(0, 2-3) = 0");
    }

    #[test]
    fn except_row_not_in_right() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 1));
        let out = oracle.compute_except();
        assert_eq!(out.iter().next().unwrap().weight, 1);
    }

    #[test]
    fn except_row_also_in_right_excluded() {
        let mut oracle = SetOpOracle::new();
        oracle.apply_left_delta(&delta(1, 1));
        oracle.apply_right_delta(&delta(1, 1));
        let out = oracle.compute_except();
        assert_eq!(out.len(), 0);
    }
}

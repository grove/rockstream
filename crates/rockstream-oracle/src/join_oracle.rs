//! Batch reference oracle for inner-join correctness verification.
//!
//! `JoinOracle` accumulates left and right Z-set deltas and computes the
//! "ground truth" materialized join result by applying the standard bilinear
//! join formula:
//!
//! ```text
//! join(L, R) = { (combine(l, r), w_l * w_r) | l ∈ L, r ∈ R, key_fn(l) == key_fn(r) }
//! ```
//!
//! Property tests compare the accumulated incremental output of `HashJoinOp`
//! against `JoinOracle::compute_join()` to prove DBSP soundness:
//!
//! ```text
//! ∑ incremental_outputs == batch_join(accumulated_left, accumulated_right)
//! ```
//!
//! # Three-way join oracle
//!
//! For A ⊗ B ⊗ C tests, use two `JoinOracle` instances:
//! - `oracle_ab`: accumulates A and B, computes AB
//! - `oracle_abc`: accumulates AB and C, computes ABC

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_types::batch::ZSet;

// ─── Public types (re-exported for test crates) ───────────────────────────────

/// Extract the join key bytes from a `(row_key, row_value)` pair.
pub type JoinKeyFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Build an output `(key, value)` from a matched left + right pair.
pub type CombineFn =
    Arc<dyn Fn(&[u8], &[u8], &[u8], &[u8]) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static>;

// ─── Schema helpers ───────────────────────────────────────────────────────────

/// Schema for a two-column row: `{ id: i64, join_key: i64 }`.
///
/// - `key`   = 8-byte big-endian `id`
/// - `value` = 8-byte big-endian `join_key`
pub struct JoinRowSchema;

impl JoinRowSchema {
    /// Encode `(id, join_key)` → `(key_bytes, value_bytes)`.
    pub fn encode(id: i64, join_key: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), join_key.to_be_bytes().to_vec())
    }

    /// Decode `(key_bytes, value_bytes)` → `(id, join_key)`.
    pub fn decode(key: &[u8], value: &[u8]) -> (i64, i64) {
        let id = decode_i64(key);
        let join_key = decode_i64(value);
        (id, join_key)
    }

    /// Join key extractor: first 8 bytes of `value`.
    pub fn key_fn() -> JoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| value[..8.min(value.len())].to_vec())
    }
}

/// Schema for a three-column row: `{ id: i64, join_key: i64, val: i64 }`.
///
/// - `key`   = 8-byte big-endian `id`
/// - `value` = 16-byte `join_key || val`
pub struct JoinRowWithValSchema;

impl JoinRowWithValSchema {
    /// Encode `(id, join_key, val)` → `(key_bytes, value_bytes)`.
    pub fn encode(id: i64, join_key: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(&join_key.to_be_bytes());
        value.extend_from_slice(&val.to_be_bytes());
        (id.to_be_bytes().to_vec(), value)
    }

    /// Decode `(key_bytes, value_bytes)` → `(id, join_key, val)`.
    pub fn decode(key: &[u8], value: &[u8]) -> (i64, i64, i64) {
        let id = decode_i64(key);
        let join_key = decode_i64(&value[..8.min(value.len())]);
        let val = if value.len() >= 16 {
            decode_i64(&value[8..])
        } else {
            0
        };
        (id, join_key, val)
    }

    /// Join key extractor: first 8 bytes of `value`.
    pub fn key_fn() -> JoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| value[..8.min(value.len())].to_vec())
    }

    /// Combine left + right into output `{ left_id || right_id, left_val || right_val }`.
    pub fn combine_fn() -> CombineFn {
        Arc::new(|lk: &[u8], lv: &[u8], rk: &[u8], rv: &[u8]| {
            // Output key = left_id || right_id (16 bytes)
            let mut out_key = lk[..8.min(lk.len())].to_vec();
            out_key.extend_from_slice(&rk[..8.min(rk.len())]);
            // Output value = left_val (bytes 8-15 of lv) || right_val (bytes 8-15 of rv)
            let left_val_bytes = if lv.len() >= 16 {
                &lv[8..16]
            } else {
                &[0u8; 8]
            };
            let right_val_bytes = if rv.len() >= 16 {
                &rv[8..16]
            } else {
                &[0u8; 8]
            };
            let mut out_val = left_val_bytes.to_vec();
            out_val.extend_from_slice(right_val_bytes);
            (out_key, out_val)
        })
    }
}

fn decode_i64(bytes: &[u8]) -> i64 {
    if bytes.len() >= 8 {
        i64::from_be_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
    } else {
        0
    }
}

// ─── JoinOracle ───────────────────────────────────────────────────────────────

/// Batch reference oracle for inner-join correctness.
///
/// Accumulates left and right Z-set state and computes the materialized join
/// result via the bilinear join formula. Used in property tests as ground truth.
pub struct JoinOracle {
    /// Left accumulated state: (key, value) → cumulative weight.
    left_state: HashMap<(Vec<u8>, Vec<u8>), i64>,
    /// Right accumulated state: (key, value) → cumulative weight.
    right_state: HashMap<(Vec<u8>, Vec<u8>), i64>,
    left_key_fn: JoinKeyFn,
    right_key_fn: JoinKeyFn,
    combine_fn: CombineFn,
}

impl JoinOracle {
    /// Create a new `JoinOracle`.
    pub fn new(left_key_fn: JoinKeyFn, right_key_fn: JoinKeyFn, combine_fn: CombineFn) -> Self {
        Self {
            left_state: HashMap::new(),
            right_state: HashMap::new(),
            left_key_fn,
            right_key_fn,
            combine_fn,
        }
    }

    /// Apply a left-side delta to the accumulated state.
    pub fn apply_left_delta(&mut self, delta: &ZSet) {
        for row in delta.iter() {
            *self
                .left_state
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }
    }

    /// Apply a right-side delta to the accumulated state.
    pub fn apply_right_delta(&mut self, delta: &ZSet) {
        for row in delta.iter() {
            *self
                .right_state
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }
    }

    /// Compute the full materialized inner-join result.
    ///
    /// Returns a Z-set where each entry's weight is `left_weight * right_weight`.
    /// In the standard SQL (set-semantics) case this is always 0 or 1.
    pub fn compute_join(&self) -> ZSet {
        type RightEntry<'a> = (&'a Vec<u8>, &'a Vec<u8>, i64);
        // Build a right-side index: join_key → [(row_key, row_value, weight)]
        let mut right_index: HashMap<Vec<u8>, Vec<RightEntry<'_>>> = HashMap::new();
        for ((rk, rv), rw) in &self.right_state {
            if *rw == 0 {
                continue;
            }
            let jk = (self.right_key_fn)(rk, rv);
            right_index.entry(jk).or_default().push((rk, rv, *rw));
        }

        let mut result = ZSet::new();
        for ((lk, lv), lw) in &self.left_state {
            if *lw == 0 {
                continue;
            }
            let jk = (self.left_key_fn)(lk, lv);
            if let Some(right_rows) = right_index.get(&jk) {
                for (rk, rv, rw) in right_rows {
                    let w = lw * rw;
                    if w != 0 {
                        let (ok, ov) = (self.combine_fn)(lk, lv, rk, rv);
                        result.insert(ok, ov, w);
                    }
                }
            }
        }
        result
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_join_returns_empty() {
        let oracle = JoinOracle::new(
            JoinRowSchema::key_fn(),
            JoinRowSchema::key_fn(),
            Arc::new(|lk, lv, rk, rv| {
                let mut k = lk.to_vec();
                k.extend_from_slice(rk);
                let mut v = lv.to_vec();
                v.extend_from_slice(rv);
                (k, v)
            }),
        );
        assert_eq!(oracle.compute_join().len(), 0);
    }

    #[test]
    fn matching_rows_appear_in_join() {
        let mut oracle = JoinOracle::new(
            JoinRowSchema::key_fn(),
            JoinRowSchema::key_fn(),
            Arc::new(|lk, lv, rk, rv| {
                let mut k = lk.to_vec();
                k.extend_from_slice(rk);
                let mut v = lv.to_vec();
                v.extend_from_slice(rv);
                (k, v)
            }),
        );

        let (lk, lv) = JoinRowSchema::encode(1, 42);
        let (rk, rv) = JoinRowSchema::encode(10, 42);
        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);

        oracle.apply_left_delta(&left);
        oracle.apply_right_delta(&right);
        assert_eq!(oracle.compute_join().len(), 1);
    }

    #[test]
    fn retraction_removes_row_from_join() {
        let mut oracle = JoinOracle::new(
            JoinRowSchema::key_fn(),
            JoinRowSchema::key_fn(),
            Arc::new(|lk, lv, rk, rv| {
                let mut k = lk.to_vec();
                k.extend_from_slice(rk);
                let mut v = lv.to_vec();
                v.extend_from_slice(rv);
                (k, v)
            }),
        );

        let (lk, lv) = JoinRowSchema::encode(1, 42);
        let (rk, rv) = JoinRowSchema::encode(10, 42);
        let mut left = ZSet::new();
        left.insert(lk.clone(), lv.clone(), 1);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);

        oracle.apply_left_delta(&left);
        oracle.apply_right_delta(&right);
        assert_eq!(oracle.compute_join().len(), 1);

        // Retract left row
        let mut retract = ZSet::new();
        retract.insert(lk, lv, -1);
        oracle.apply_left_delta(&retract);
        assert_eq!(oracle.compute_join().len(), 0);
    }
}

//! DataFusion-based window function oracle for correctness verification (v0.19).
//!
//! The `WindowOracle` computes the "ground truth" for window functions
//! (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD, SlidingSum, SlidingAvg)
//! by implementing them in pure Rust over a sorted snapshot.
//!
//! Property tests compare `WindowOp` incremental results against the oracle
//! to prove that our window operator is correct.
//!
//! # DBSP soundness assertion for window operators
//!
//! ```text
//! window(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ) == oracle_window(accumulated_state)
//! ```

use std::collections::BTreeMap;
use rockstream_plan::WindowFunc;

/// Compute the reference window function output for a Z-set snapshot.
///
/// The snapshot is a sorted list of `(order_key_bytes, row_id, row_value_bytes)`.
/// For each row, returns the window function output as `i64`.
///
/// This mirrors the partition-recomputation approach: for each partition,
/// sort by order_key and apply the window function.
pub struct WindowOracle;

impl WindowOracle {
    /// Compute window function outputs for a set of rows in one partition.
    ///
    /// `rows` must be sorted by `(order_key, row_id)` (ascending).
    /// Returns a `Vec<i64>` of window function outputs, one per row, in the
    /// same order as `rows`.
    ///
    /// For SlidingSum/SlidingAvg, `value_fn` must be provided.
    pub fn compute(
        func: &WindowFunc,
        rows: &[(&[u8], i64)], // (order_key, value)
    ) -> Vec<i64> {
        let n = rows.len();
        (0..n)
            .map(|pos| Self::compute_one(func, pos, n, rows))
            .collect()
    }

    fn compute_one(
        func: &WindowFunc,
        pos: usize,
        n: usize,
        rows: &[(&[u8], i64)],
    ) -> i64 {
        match func {
            WindowFunc::RowNumber => pos as i64 + 1,
            WindowFunc::Rank => {
                let my_order = rows[pos].0;
                rows.iter().take_while(|(ok, _)| *ok < my_order).count() as i64 + 1
            }
            WindowFunc::DenseRank => {
                let my_order = rows[pos].0;
                let mut distinct = BTreeMap::<&[u8], ()>::new();
                for (ok, _) in rows.iter() {
                    if *ok <= my_order {
                        distinct.insert(ok, ());
                    }
                }
                distinct.len() as i64
            }
            WindowFunc::Ntile(buckets) => {
                let bucket_count = (*buckets as usize).max(1);
                if n == 0 {
                    1
                } else {
                    ((pos * bucket_count) / n) as i64 + 1
                }
            }
            WindowFunc::Lag { offset } => {
                if pos >= *offset {
                    rows[pos - offset].1
                } else {
                    0
                }
            }
            WindowFunc::Lead { offset } => {
                if pos + offset < n {
                    rows[pos + offset].1
                } else {
                    0
                }
            }
            WindowFunc::SlidingSum { frame_rows } => {
                let start = pos.saturating_sub(*frame_rows);
                rows[start..=pos].iter().map(|(_, v)| v).sum()
            }
            WindowFunc::SlidingAvg { frame_rows } => {
                let start = pos.saturating_sub(*frame_rows);
                let count = (pos - start + 1) as i64;
                if count == 0 {
                    0
                } else {
                    rows[start..=pos].iter().map(|(_, v)| v).sum::<i64>() / count
                }
            }
        }
    }
}

/// Encode a row for the window oracle test harness.
///
/// Returns `(order_key_bytes, value_i64)`.
pub fn make_oracle_row(order_val: i64, value: i64) -> (Vec<u8>, i64) {
    (order_val.to_be_bytes().to_vec(), value)
}

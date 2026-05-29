//! `OuterJoinOp` — incremental outer-join, semi-join, and anti-join operator.
//!
//! Supports `LEFT OUTER`, `RIGHT OUTER`, `FULL OUTER`, `LEFT SEMI`, `LEFT ANTI`,
//! `RIGHT SEMI`, and `RIGHT ANTI` join types.
//!
//! # Algorithm
//!
//! The core data structure is a pair of Z-set arrangements plus per-join-key
//! right/left weight sums that track "matched" vs "unmatched" status:
//!
//! ```text
//! right_weight_by_jk[jk] = Σ w_r  for all r in right_arr with join_key == jk
//! left_weight_by_jk[jk]  = Σ w_l  for all l in left_arr  with join_key == jk
//! ```
//!
//! When `right_weight_by_jk[jk]` crosses zero (0→nonzero or nonzero→0), the
//! unmatched status of every left row at `jk` changes and the corresponding
//! output adjustments are emitted. The same logic applies to `left_weight_by_jk`
//! for right-outer cases.
//!
//! # Correctness
//!
//! The algorithm maintains the DBSP bilinear identity for the matched (inner
//! join) part and independently tracks the unmatched-row outputs via weight-sum
//! zero-crossing detection. The result is that at every epoch boundary:
//!
//! ```text
//! Σ incremental_outputs == batch_outer_join(accumulated_left, accumulated_right)
//! ```
//!
//! This invariant is verified by `OuterJoinOracle` in `rockstream-oracle`.

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_types::batch::{ZSet, ZSetBatch};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Extract the join key bytes from a `(row_key, row_value)` pair.
pub type JoinKeyFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Combine a matched left + right row pair into an output `(key, value)`.
pub type CombineFn =
    Arc<dyn Fn(&[u8], &[u8], &[u8], &[u8]) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static>;

/// Produce an output `(key, value)` from an unmatched row (null-padded).
///
/// Used for unmatched left rows in LEFT/FULL outer joins and for
/// unmatched right rows in RIGHT/FULL outer joins.
pub type NullCombineFn = Arc<dyn Fn(&[u8], &[u8]) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static>;

/// The join semantics for `OuterJoinOp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// SQL `LEFT OUTER JOIN`: unmatched left rows are emitted with null right.
    LeftOuter,
    /// SQL `RIGHT OUTER JOIN`: unmatched right rows are emitted with null left.
    RightOuter,
    /// SQL `FULL OUTER JOIN`: unmatched rows on both sides are emitted.
    FullOuter,
    /// SQL semi-join (`WHERE EXISTS ...`): emit left row iff ≥1 right match.
    LeftSemi,
    /// SQL anti-join (`WHERE NOT EXISTS ...`): emit left row iff 0 right matches.
    LeftAnti,
    /// Right semi-join: emit right row iff ≥1 left match.
    RightSemi,
    /// Right anti-join: emit right row iff 0 left matches.
    RightAnti,
}

/// Arrangement type: join_key → { (row_key, row_value) → cumulative_weight }.
type Arrangement = HashMap<Vec<u8>, HashMap<(Vec<u8>, Vec<u8>), i64>>;

// ─── OuterJoinMeta ────────────────────────────────────────────────────────────

/// Runtime metadata returned by `OuterJoinOp::metadata()` for `EXPLAIN INCREMENTAL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterJoinMeta {
    /// Operator name.
    pub name: String,
    /// The join type.
    pub join_type: JoinType,
    /// Number of distinct join keys in the left arrangement.
    pub left_join_keys: usize,
    /// Total non-zero rows in the left arrangement.
    pub left_rows: usize,
    /// Number of distinct join keys in the right arrangement.
    pub right_join_keys: usize,
    /// Total non-zero rows in the right arrangement.
    pub right_rows: usize,
    /// Number of join keys currently with zero right-side weight (left unmatched).
    pub left_unmatched_keys: usize,
    /// Number of join keys currently with zero left-side weight (right unmatched).
    pub right_unmatched_keys: usize,
}

// ─── OuterJoinOp ──────────────────────────────────────────────────────────────

/// Incremental outer-join, semi-join, and anti-join operator.
pub struct OuterJoinOp {
    name: String,
    join_type: JoinType,
    left_key_fn: JoinKeyFn,
    right_key_fn: JoinKeyFn,
    /// Used for matched-row output in outer and non-semi/anti joins.
    combine_fn: CombineFn,
    /// For LEFT/FULL: produces unmatched-left output (right columns are null).
    null_right_fn: Option<NullCombineFn>,
    /// For RIGHT/FULL: produces unmatched-right output (left columns are null).
    null_left_fn: Option<NullCombineFn>,
    /// Left arrangement: join_key → (row_key, row_value) → weight.
    left_arr: Arrangement,
    /// Right arrangement: join_key → (row_key, row_value) → weight.
    right_arr: Arrangement,
    /// Sum of right weights per join_key (positive = at least one match exists).
    right_weight_by_jk: HashMap<Vec<u8>, i64>,
    /// Sum of left weights per join_key (positive = at least one match exists).
    left_weight_by_jk: HashMap<Vec<u8>, i64>,
}

impl OuterJoinOp {
    /// Create a new `OuterJoinOp`.
    ///
    /// - `null_right_fn`: required for `LeftOuter` and `FullOuter`.
    /// - `null_left_fn`: required for `RightOuter` and `FullOuter`.
    /// - For `LeftSemi`, `LeftAnti`, `RightSemi`, `RightAnti`, `combine_fn` is
    ///   only used when the join type also emits matched rows (not applicable
    ///   for pure semi/anti); it can be a no-op closure in that case.
    pub fn new(
        name: impl Into<String>,
        join_type: JoinType,
        left_key_fn: JoinKeyFn,
        right_key_fn: JoinKeyFn,
        combine_fn: CombineFn,
        null_right_fn: Option<NullCombineFn>,
        null_left_fn: Option<NullCombineFn>,
    ) -> Self {
        Self {
            name: name.into(),
            join_type,
            left_key_fn,
            right_key_fn,
            combine_fn,
            null_right_fn,
            null_left_fn,
            left_arr: HashMap::new(),
            right_arr: HashMap::new(),
            right_weight_by_jk: HashMap::new(),
            left_weight_by_jk: HashMap::new(),
        }
    }

    /// Process a left-side delta.
    ///
    /// Joins every row in `left_delta` against the current right arrangement
    /// (pre-change snapshot) and emits the appropriate output based on join type.
    /// Then updates the left arrangement.
    pub fn process_left_delta(&mut self, left_delta: &ZSet) -> ZSet {
        let mut output = ZSet::new();

        for row in left_delta.iter() {
            let jk = (self.left_key_fn)(&row.key, &row.value);
            let right_total = self.right_weight_by_jk.get(&jk).copied().unwrap_or(0);

            match self.join_type {
                JoinType::LeftOuter => {
                    // Emit inner join products.
                    if let Some(right_bucket) = self.right_arr.get(&jk) {
                        for ((rk, rv), rw) in right_bucket {
                            if *rw == 0 {
                                continue;
                            }
                            let out_w = row.weight.saturating_mul(*rw);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(&row.key, &row.value, rk, rv);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // If no right match, emit null-padded row.
                    if right_total == 0 {
                        if let Some(ref null_fn) = self.null_right_fn {
                            let (ok, ov) = null_fn(&row.key, &row.value);
                            output.insert(ok, ov, row.weight);
                        }
                    }
                }

                JoinType::RightOuter => {
                    // Emit inner join products from left delta.
                    if let Some(right_bucket) = self.right_arr.get(&jk) {
                        for ((rk, rv), rw) in right_bucket {
                            if *rw == 0 {
                                continue;
                            }
                            let out_w = row.weight.saturating_mul(*rw);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(&row.key, &row.value, rk, rv);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // Retract/emit unmatched-right outputs when left side crosses zero.
                    let old_left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    let new_left_total = old_left_total + row.weight;
                    if old_left_total == 0 && new_left_total != 0 {
                        // Right rows at jk were unmatched; now they have a left match.
                        if let (Some(right_bucket), Some(ref null_fn)) =
                            (self.right_arr.get(&jk), &self.null_left_fn)
                        {
                            for ((rk, rv), rw) in right_bucket {
                                if *rw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(rk, rv);
                                output.insert(ok, ov, -rw);
                            }
                        }
                    } else if old_left_total != 0 && new_left_total == 0 {
                        // Left side dropped to zero: right rows become unmatched again.
                        if let (Some(right_bucket), Some(ref null_fn)) =
                            (self.right_arr.get(&jk), &self.null_left_fn)
                        {
                            for ((rk, rv), rw) in right_bucket {
                                if *rw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(rk, rv);
                                output.insert(ok, ov, *rw);
                            }
                        }
                    }
                }

                JoinType::FullOuter => {
                    // Emit inner join products.
                    if let Some(right_bucket) = self.right_arr.get(&jk) {
                        for ((rk, rv), rw) in right_bucket {
                            if *rw == 0 {
                                continue;
                            }
                            let out_w = row.weight.saturating_mul(*rw);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(&row.key, &row.value, rk, rv);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // Left-outer part: if no right match, emit null-padded left row.
                    if right_total == 0 {
                        if let Some(ref null_fn) = self.null_right_fn {
                            let (ok, ov) = null_fn(&row.key, &row.value);
                            output.insert(ok, ov, row.weight);
                        }
                    }
                    // Right-outer part: retract/emit unmatched-right outputs when
                    // left side crosses zero at jk.
                    let old_left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    let new_left_total = old_left_total + row.weight;
                    if old_left_total == 0 && new_left_total != 0 {
                        if let (Some(right_bucket), Some(ref null_fn)) =
                            (self.right_arr.get(&jk), &self.null_left_fn)
                        {
                            for ((rk, rv), rw) in right_bucket {
                                if *rw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(rk, rv);
                                output.insert(ok, ov, -rw);
                            }
                        }
                    } else if old_left_total != 0 && new_left_total == 0 {
                        if let (Some(right_bucket), Some(ref null_fn)) =
                            (self.right_arr.get(&jk), &self.null_left_fn)
                        {
                            for ((rk, rv), rw) in right_bucket {
                                if *rw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(rk, rv);
                                output.insert(ok, ov, *rw);
                            }
                        }
                    }
                }

                JoinType::LeftSemi => {
                    // Emit left row iff right side has matches.
                    if right_total != 0 {
                        output.insert(row.key.clone(), row.value.clone(), row.weight);
                    }
                }

                JoinType::LeftAnti => {
                    // Emit left row iff right side has NO matches.
                    if right_total == 0 {
                        output.insert(row.key.clone(), row.value.clone(), row.weight);
                    }
                }

                JoinType::RightSemi | JoinType::RightAnti => {
                    // Right semi/anti only emit from the right side; nothing to emit
                    // when left delta arrives (but we still need to track left state
                    // for right-side unmatched status).
                    let old_left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    let new_left_total = old_left_total + row.weight;

                    let crossed = (old_left_total == 0 && new_left_total != 0)
                        || (old_left_total != 0 && new_left_total == 0);
                    if crossed {
                        if let Some(right_bucket) = self.right_arr.get(&jk) {
                            for ((rk, rv), rw) in right_bucket {
                                if *rw == 0 {
                                    continue;
                                }
                                match self.join_type {
                                    JoinType::RightSemi => {
                                        // Emit right rows when left gains first match.
                                        if old_left_total == 0 {
                                            output.insert(rk.clone(), rv.clone(), *rw);
                                        } else {
                                            // Retract right rows when left drops to zero.
                                            output.insert(rk.clone(), rv.clone(), -rw);
                                        }
                                    }
                                    JoinType::RightAnti => {
                                        // Retract right rows when left gains first match.
                                        if old_left_total == 0 {
                                            output.insert(rk.clone(), rv.clone(), -rw);
                                        } else {
                                            // Emit right rows when left drops to zero.
                                            output.insert(rk.clone(), rv.clone(), *rw);
                                        }
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }
                }
            }

            // Update left arrangement weight sum.
            *self.left_weight_by_jk.entry(jk.clone()).or_insert(0) += row.weight;

            // Update left arrangement.
            let bucket = self.left_arr.entry(jk).or_default();
            *bucket
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }

        output
    }

    /// Process a right-side delta.
    ///
    /// Joins every row in `right_delta` against the current left arrangement
    /// (already updated from `process_left_delta`), then updates the right
    /// arrangement and emits unmatched-row adjustments based on join type.
    pub fn process_right_delta(&mut self, right_delta: &ZSet) -> ZSet {
        let mut output = ZSet::new();

        for row in right_delta.iter() {
            let jk = (self.right_key_fn)(&row.key, &row.value);
            let old_right_total = self.right_weight_by_jk.get(&jk).copied().unwrap_or(0);
            let new_right_total = old_right_total + row.weight;

            match self.join_type {
                JoinType::LeftOuter => {
                    // Inner join products (right against current left arrangement).
                    if let Some(left_bucket) = self.left_arr.get(&jk) {
                        for ((lk, lv), lw) in left_bucket {
                            if *lw == 0 {
                                continue;
                            }
                            let out_w = (*lw).saturating_mul(row.weight);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(lk, lv, &row.key, &row.value);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // Unmatched-left adjustment on zero-crossing.
                    if old_right_total == 0 && new_right_total != 0 {
                        // Left rows at jk were unmatched → retract null-padded outputs.
                        if let (Some(left_bucket), Some(ref null_fn)) =
                            (self.left_arr.get(&jk), &self.null_right_fn)
                        {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(lk, lv);
                                output.insert(ok, ov, -lw);
                            }
                        }
                    } else if old_right_total != 0 && new_right_total == 0 {
                        // Right side dropped to zero → emit null-padded for left rows.
                        if let (Some(left_bucket), Some(ref null_fn)) =
                            (self.left_arr.get(&jk), &self.null_right_fn)
                        {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(lk, lv);
                                output.insert(ok, ov, *lw);
                            }
                        }
                    }
                }

                JoinType::RightOuter => {
                    // Inner join products.
                    if let Some(left_bucket) = self.left_arr.get(&jk) {
                        for ((lk, lv), lw) in left_bucket {
                            if *lw == 0 {
                                continue;
                            }
                            let out_w = (*lw).saturating_mul(row.weight);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(lk, lv, &row.key, &row.value);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // Emit null-padded for unmatched right rows.
                    let left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    if left_total == 0 {
                        if let Some(ref null_fn) = self.null_left_fn {
                            let (ok, ov) = null_fn(&row.key, &row.value);
                            output.insert(ok, ov, row.weight);
                        }
                    }
                }

                JoinType::FullOuter => {
                    // Inner join products.
                    if let Some(left_bucket) = self.left_arr.get(&jk) {
                        for ((lk, lv), lw) in left_bucket {
                            if *lw == 0 {
                                continue;
                            }
                            let out_w = (*lw).saturating_mul(row.weight);
                            if out_w != 0 {
                                let (ok, ov) = (self.combine_fn)(lk, lv, &row.key, &row.value);
                                output.insert(ok, ov, out_w);
                            }
                        }
                    }
                    // Left-outer part: unmatched-left adjustment on zero-crossing.
                    if old_right_total == 0 && new_right_total != 0 {
                        if let (Some(left_bucket), Some(ref null_fn)) =
                            (self.left_arr.get(&jk), &self.null_right_fn)
                        {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(lk, lv);
                                output.insert(ok, ov, -lw);
                            }
                        }
                    } else if old_right_total != 0 && new_right_total == 0 {
                        if let (Some(left_bucket), Some(ref null_fn)) =
                            (self.left_arr.get(&jk), &self.null_right_fn)
                        {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                let (ok, ov) = null_fn(lk, lv);
                                output.insert(ok, ov, *lw);
                            }
                        }
                    }
                    // Right-outer part: emit null-left for unmatched right rows.
                    let left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    if left_total == 0 {
                        if let Some(ref null_fn) = self.null_left_fn {
                            let (ok, ov) = null_fn(&row.key, &row.value);
                            output.insert(ok, ov, row.weight);
                        }
                    }
                }

                JoinType::LeftSemi => {
                    // When right side crosses zero, adjust left rows at jk.
                    if old_right_total == 0 && new_right_total != 0 {
                        // Left rows just gained their first match → emit them.
                        if let Some(left_bucket) = self.left_arr.get(&jk) {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                output.insert(lk.clone(), lv.clone(), *lw);
                            }
                        }
                    } else if old_right_total != 0 && new_right_total == 0 {
                        // Left rows lost all matches → retract them.
                        if let Some(left_bucket) = self.left_arr.get(&jk) {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                output.insert(lk.clone(), lv.clone(), -lw);
                            }
                        }
                    }
                }

                JoinType::LeftAnti => {
                    // When right side crosses zero, adjust left rows at jk.
                    if old_right_total == 0 && new_right_total != 0 {
                        // Left rows just got a match → retract anti outputs.
                        if let Some(left_bucket) = self.left_arr.get(&jk) {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                output.insert(lk.clone(), lv.clone(), -lw);
                            }
                        }
                    } else if old_right_total != 0 && new_right_total == 0 {
                        // Left rows lost all matches → emit anti outputs again.
                        if let Some(left_bucket) = self.left_arr.get(&jk) {
                            for ((lk, lv), lw) in left_bucket {
                                if *lw == 0 {
                                    continue;
                                }
                                output.insert(lk.clone(), lv.clone(), *lw);
                            }
                        }
                    }
                }

                JoinType::RightSemi => {
                    // Emit right row iff left side has at least one match.
                    let left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    if left_total != 0 {
                        output.insert(row.key.clone(), row.value.clone(), row.weight);
                    }
                }

                JoinType::RightAnti => {
                    // Emit right row iff left side has no matches.
                    let left_total = self.left_weight_by_jk.get(&jk).copied().unwrap_or(0);
                    if left_total == 0 {
                        output.insert(row.key.clone(), row.value.clone(), row.weight);
                    }
                }
            }

            // Update right weight sum.
            *self.right_weight_by_jk.entry(jk.clone()).or_insert(0) += row.weight;

            // Update right arrangement.
            let bucket = self.right_arr.entry(jk).or_default();
            *bucket
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }

        output
    }

    /// Process a combined epoch: left delta then right delta.
    pub fn process_epoch(&mut self, left_delta: &ZSet, right_delta: &ZSet) -> ZSet {
        let left_out = self.process_left_delta(left_delta);
        let right_out = self.process_right_delta(right_delta);
        let mut output = left_out;
        output.merge(&right_out);
        output
    }

    /// Compact arrangements by removing zero-weight entries.
    pub fn compact(&mut self) {
        compact_arr(&mut self.left_arr);
        compact_arr(&mut self.right_arr);
        self.right_weight_by_jk.retain(|_, w| *w != 0);
        self.left_weight_by_jk.retain(|_, w| *w != 0);
    }

    /// Returns runtime metadata for `EXPLAIN INCREMENTAL`.
    pub fn metadata(&self) -> OuterJoinMeta {
        let (lk, lr) = arr_stats(&self.left_arr);
        let (rk, rr) = arr_stats(&self.right_arr);
        let left_unmatched_keys = self
            .right_weight_by_jk
            .values()
            .filter(|w| **w == 0)
            .count()
            + self
                .left_arr
                .keys()
                .filter(|jk| self.right_weight_by_jk.get(*jk).copied().unwrap_or(0) == 0)
                .count();
        let right_unmatched_keys = self.left_weight_by_jk.values().filter(|w| **w == 0).count()
            + self
                .right_arr
                .keys()
                .filter(|jk| self.left_weight_by_jk.get(*jk).copied().unwrap_or(0) == 0)
                .count();
        OuterJoinMeta {
            name: self.name.clone(),
            join_type: self.join_type,
            left_join_keys: lk,
            left_rows: lr,
            right_join_keys: rk,
            right_rows: rr,
            left_unmatched_keys,
            right_unmatched_keys,
        }
    }

    /// Name of this operator.
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ─── Operator impl ────────────────────────────────────────────────────────────

#[async_trait::async_trait]
impl Operator for OuterJoinOp {
    async fn process(
        &mut self,
        input: &rockstream_types::batch::SourceBatch,
    ) -> rockstream_types::batch::SinkBatch {
        rockstream_types::batch::SinkBatch {
            record_count: input.record_count,
            epoch: input.epoch,
        }
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let out = self.process_left_delta(&input.zset);
        ZSetBatch {
            zset: out,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {
        self.compact();
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn compact_arr(arr: &mut Arrangement) {
    arr.retain(|_, bucket| {
        bucket.retain(|_, w| *w != 0);
        !bucket.is_empty()
    });
}

fn arr_stats(arr: &Arrangement) -> (usize, usize) {
    let keys = arr.len();
    let rows = arr
        .values()
        .map(|b| b.values().filter(|w| **w != 0).count())
        .sum();
    (keys, rows)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;
    use std::sync::Arc;

    // ─── Schema helpers ───────────────────────────────────────────────────────
    //
    // Row schema: key = 8-byte big-endian i64 id
    //             value = 8-byte big-endian i64 join_key
    //
    // Null-padded schema (for outer join unmatched rows):
    //   key = 8-byte left_id
    //   value = 8-byte join_key || 0xFFFF_FFFF_FFFF_FFFFi64 (sentinel for NULL)

    const NULL_SENTINEL: i64 = i64::MAX;

    fn encode(id: i64, join_key: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), join_key.to_be_bytes().to_vec())
    }

    fn key_fn() -> JoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| value[..8.min(value.len())].to_vec())
    }

    fn combine_fn() -> CombineFn {
        Arc::new(|lk: &[u8], _lv: &[u8], rk: &[u8], _rv: &[u8]| {
            // Output key = left_id || right_id
            let mut out_key = lk[..8.min(lk.len())].to_vec();
            out_key.extend_from_slice(&rk[..8.min(rk.len())]);
            (out_key, b"matched".to_vec())
        })
    }

    fn null_right_fn() -> NullCombineFn {
        Arc::new(|lk: &[u8], _lv: &[u8]| {
            // key = left_id, value = "null_right" marker
            (
                lk[..8.min(lk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    fn null_left_fn() -> NullCombineFn {
        Arc::new(|rk: &[u8], _rv: &[u8]| {
            // key = right_id, value = "null_left" marker
            (
                rk[..8.min(rk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    // ─── Left Outer Join ──────────────────────────────────────────────────────

    #[test]
    fn left_outer_unmatched_left_row_emitted() {
        let mut op = OuterJoinOp::new(
            "loj",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );

        // Insert a left row with no right match.
        let (lk, lv) = encode(1, 42);
        let mut left_delta = ZSet::new();
        left_delta.insert(lk.clone(), lv.clone(), 1);

        let out = op.process_left_delta(&left_delta);
        // Should produce one null-padded row.
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, 1);
    }

    #[test]
    fn left_outer_matched_row_emits_combined_and_retracts_null() {
        let mut op = OuterJoinOp::new(
            "loj",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Left row arrives first (unmatched).
        let mut left_delta = ZSet::new();
        left_delta.insert(lk.clone(), lv.clone(), 1);
        let out1 = op.process_left_delta(&left_delta);
        assert_eq!(out1.len(), 1, "unmatched left row expected");
        let r = out1.iter().next().unwrap();
        assert_eq!(r.weight, 1);

        // Right row arrives — matches left.
        let mut right_delta = ZSet::new();
        right_delta.insert(rk.clone(), rv.clone(), 1);
        let out2 = op.process_right_delta(&right_delta);

        // Expect: +1 matched row + (-1) retraction of null-padded row.
        let mut out2_map: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();
        for row in out2.iter() {
            *out2_map
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0) += row.weight;
        }
        // The null-padded entry should be retracted.
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        assert_eq!(
            out2_map.get(&(lk.clone(), null_val)).copied().unwrap_or(0),
            -1
        );
        // The matched entry should be inserted.
        let mut matched_key = lk.clone();
        matched_key.extend_from_slice(&rk);
        assert_eq!(
            out2_map
                .get(&(matched_key, b"matched".to_vec()))
                .copied()
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn left_outer_delete_right_emits_null_again() {
        let mut op = OuterJoinOp::new(
            "loj_delete",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Setup: left row + matched right row.
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        op.process_left_delta(&ld);

        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        op.process_right_delta(&rd);

        // Now delete the right row.
        let mut rd2 = ZSet::new();
        rd2.insert(rk, rv, -1);
        let out = op.process_right_delta(&rd2);

        // Should retract matched row and emit null-padded.
        let mut map: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();
        for row in out.iter() {
            *map.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        assert_eq!(map.get(&(lk.clone(), null_val)).copied().unwrap_or(0), 1);
    }

    // ─── Right Outer Join ─────────────────────────────────────────────────────

    #[test]
    fn right_outer_unmatched_right_row_emitted() {
        let mut op = OuterJoinOp::new(
            "roj",
            JoinType::RightOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            Some(null_left_fn()),
        );

        let (rk, rv) = encode(10, 42);
        let mut right_delta = ZSet::new();
        right_delta.insert(rk.clone(), rv.clone(), 1);

        let out = op.process_right_delta(&right_delta);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, rk);
        assert_eq!(row.weight, 1);
    }

    // ─── Full Outer Join ──────────────────────────────────────────────────────

    #[test]
    fn full_outer_both_unmatched_rows_emitted() {
        let mut op = OuterJoinOp::new(
            "foj",
            JoinType::FullOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            Some(null_left_fn()),
        );

        let (lk, lv) = encode(1, 99);
        let (rk, rv) = encode(10, 77);

        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        let out1 = op.process_left_delta(&ld);
        assert_eq!(out1.len(), 1, "unmatched left should be emitted");

        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out2 = op.process_right_delta(&rd);
        // join_key 99 != 77, so right row is unmatched
        assert_eq!(out2.len(), 1, "unmatched right should be emitted");
        let row = out2.iter().next().unwrap();
        assert_eq!(row.key, rk);
    }

    #[test]
    fn full_outer_match_retracts_both_nulls() {
        let mut op = OuterJoinOp::new(
            "foj_match",
            JoinType::FullOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            Some(null_left_fn()),
        );

        let jk = 42i64;
        let (lk, lv) = encode(1, jk);
        let (rk, rv) = encode(10, jk);

        // Insert left first (unmatched).
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        let out1 = op.process_left_delta(&ld);
        assert_eq!(out1.len(), 1);

        // Insert right — same join key → both become matched.
        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out2 = op.process_right_delta(&rd);

        let mut map: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();
        for row in out2.iter() {
            *map.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        // Null-padded left row should be retracted.
        assert_eq!(map.get(&(lk.clone(), null_val)).copied().unwrap_or(0), -1);
        // Inner join product should be emitted.
        let mut matched_key = lk.clone();
        matched_key.extend_from_slice(&rk);
        assert_eq!(
            map.get(&(matched_key, b"matched".to_vec()))
                .copied()
                .unwrap_or(0),
            1
        );
    }

    // ─── Semi join ────────────────────────────────────────────────────────────

    #[test]
    fn left_semi_emits_left_when_right_exists() {
        let mut op = OuterJoinOp::new(
            "semi",
            JoinType::LeftSemi,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Left arrives first — no right match → nothing emitted.
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        let out1 = op.process_left_delta(&ld);
        assert!(out1.is_empty(), "no right match → semi join emits nothing");

        // Right arrives — left should now appear.
        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out2 = op.process_right_delta(&rd);
        assert_eq!(out2.len(), 1);
        let row = out2.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, 1);
    }

    #[test]
    fn left_semi_retracts_when_right_deleted() {
        let mut op = OuterJoinOp::new(
            "semi_del",
            JoinType::LeftSemi,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Setup.
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        op.process_left_delta(&ld);

        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        op.process_right_delta(&rd);

        // Delete right row.
        let mut rd2 = ZSet::new();
        rd2.insert(rk, rv, -1);
        let out = op.process_right_delta(&rd2);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, -1);
    }

    // ─── Anti join ────────────────────────────────────────────────────────────

    #[test]
    fn left_anti_emits_left_when_no_right() {
        let mut op = OuterJoinOp::new(
            "anti",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        let out = op.process_left_delta(&ld);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, 1);
    }

    #[test]
    fn left_anti_retracts_when_right_arrives() {
        let mut op = OuterJoinOp::new(
            "anti_retract",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Left arrives (no right → emitted by anti).
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        op.process_left_delta(&ld);

        // Right arrives → left should be retracted from anti output.
        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out = op.process_right_delta(&rd);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, -1);
    }

    #[test]
    fn left_anti_emits_again_when_right_deleted() {
        let mut op = OuterJoinOp::new(
            "anti_delete",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Setup: left + right both present (anti output suppressed).
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        op.process_left_delta(&ld);

        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        op.process_right_delta(&rd);

        // Delete right row → left should be emitted again.
        let mut rd2 = ZSet::new();
        rd2.insert(rk, rv, -1);
        let out = op.process_right_delta(&rd2);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, lk);
        assert_eq!(row.weight, 1);
    }

    // ─── q11-style: LEFT OUTER JOIN with aggregation edge case ────────────────
    //
    // Q11 spirit: supplier LEFT JOIN partsupp — suppliers without any parts
    // should still appear in the output (with null part value).

    #[test]
    fn q11_style_left_outer_supplier_no_parts() {
        let mut op = OuterJoinOp::new(
            "q11_loj",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );

        // Suppliers (left side): s1 suppkey=1, s2 suppkey=2
        let (s1k, s1v) = encode(1, 1);
        let (s2k, s2v) = encode(2, 2);
        // Parts (right side): only part p1 has suppkey=1
        let (p1k, p1v) = encode(100, 1);

        let mut ld = ZSet::new();
        ld.insert(s1k.clone(), s1v.clone(), 1);
        ld.insert(s2k.clone(), s2v.clone(), 1);
        let out1 = op.process_left_delta(&ld);

        // Both suppliers are unmatched initially.
        assert_eq!(out1.len(), 2);
        for row in out1.iter() {
            assert_eq!(row.weight, 1);
        }

        let mut rd = ZSet::new();
        rd.insert(p1k.clone(), p1v.clone(), 1);
        let out2 = op.process_right_delta(&rd);

        let mut map: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();
        for row in out2.iter() {
            *map.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }

        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        // s1's null-padded entry should be retracted (it now has a match).
        assert_eq!(
            map.get(&(s1k.clone(), null_val.clone()))
                .copied()
                .unwrap_or(0),
            -1
        );
        // s2 is still unmatched — its null-padded entry should remain (no change here).
        assert_eq!(
            map.get(&(s2k.clone(), null_val.clone()))
                .copied()
                .unwrap_or(0),
            0
        );
        // Matched row for s1 + p1 should appear.
        let mut matched_key = s1k.clone();
        matched_key.extend_from_slice(&p1k);
        assert_eq!(
            map.get(&(matched_key, b"matched".to_vec()))
                .copied()
                .unwrap_or(0),
            1
        );
    }

    // ─── q21-style: ANTI JOIN for "suppliers with no competing late delivery" ─

    #[test]
    fn q21_style_anti_join_suppliers_without_competitors() {
        let mut op = OuterJoinOp::new(
            "q21_anti",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        // Left = lineitem rows for suppliers (suppkey as join key)
        // Right = competing late lineitems for the same orderkey
        // Q21: suppliers NOT in the right (no competitor) appear in anti-join output.
        let (l1k, l1v) = encode(1001, 7); // order 1001, suppkey 7
        let (l2k, l2v) = encode(1002, 8); // order 1002, suppkey 8
        let (l3k, l3v) = encode(1003, 7); // order 1003, suppkey 7 (competitor for suppkey 7)

        // Insert supplier orders on left side.
        let mut ld = ZSet::new();
        ld.insert(l1k.clone(), l1v.clone(), 1);
        ld.insert(l2k.clone(), l2v.clone(), 1);
        let out1 = op.process_left_delta(&ld);
        // Both should appear in anti-join output (no right rows yet).
        assert_eq!(out1.len(), 2);

        // Insert competitor for suppkey=7 on right side.
        let mut rd = ZSet::new();
        rd.insert(l3k.clone(), l3v.clone(), 1);
        let out2 = op.process_right_delta(&rd);
        // suppkey=7 left rows should be retracted from anti-join output.
        assert_eq!(out2.len(), 1);
        let row = out2.iter().next().unwrap();
        assert_eq!(row.key, l1k);
        assert_eq!(row.weight, -1);

        // Now delete the competitor.
        let mut rd2 = ZSet::new();
        rd2.insert(l3k, l3v, -1);
        let out3 = op.process_right_delta(&rd2);
        // suppkey=7 left row should re-appear.
        assert_eq!(out3.len(), 1);
        let row = out3.iter().next().unwrap();
        assert_eq!(row.key, l1k);
        assert_eq!(row.weight, 1);
    }

    // ─── NULL-heavy test: multiple join keys, mix of matched/unmatched ─────────

    #[test]
    fn null_heavy_left_outer_multiple_join_keys() {
        let mut op = OuterJoinOp::new(
            "null_heavy",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );

        // Insert 5 left rows with different join keys.
        let jks = [10i64, 20, 30, 40, 50];
        let mut ld = ZSet::new();
        for (i, &jk) in jks.iter().enumerate() {
            let (k, v) = encode(i as i64 + 1, jk);
            ld.insert(k, v, 1);
        }
        let out1 = op.process_left_delta(&ld);
        // All 5 should be unmatched.
        assert_eq!(out1.len(), 5);
        for row in out1.iter() {
            assert_eq!(row.weight, 1);
        }

        // Insert right rows for jk=10 and jk=30 only.
        let (r1k, r1v) = encode(100, 10);
        let (r2k, r2v) = encode(200, 30);
        let mut rd = ZSet::new();
        rd.insert(r1k.clone(), r1v.clone(), 1);
        rd.insert(r2k.clone(), r2v.clone(), 1);
        let out2 = op.process_right_delta(&rd);

        let mut map: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();
        for row in out2.iter() {
            *map.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }

        // jk=10 and jk=30 null entries retracted (-1 each).
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        let (l1k, _) = encode(1, 10);
        let (l3k, _) = encode(3, 30);
        assert_eq!(
            map.get(&(l1k.clone(), null_val.clone()))
                .copied()
                .unwrap_or(0),
            -1
        );
        assert_eq!(
            map.get(&(l3k.clone(), null_val.clone()))
                .copied()
                .unwrap_or(0),
            -1
        );

        // jk=20, 40, 50 remain unmatched — no change in out2.
        let (l2k, _) = encode(2, 20);
        assert_eq!(
            map.get(&(l2k.clone(), null_val.clone()))
                .copied()
                .unwrap_or(0),
            0
        );
    }

    // ─── Right semi join ─────────────────────────────────────────────────────

    #[test]
    fn right_semi_emits_right_when_left_exists() {
        let mut op = OuterJoinOp::new(
            "rsemi",
            JoinType::RightSemi,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (lk, lv) = encode(1, 42);
        let (rk, rv) = encode(10, 42);

        // Right arrives first — no left match → nothing.
        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out1 = op.process_right_delta(&rd);
        assert!(out1.is_empty());

        // Left arrives — right row should now appear.
        let mut ld = ZSet::new();
        ld.insert(lk.clone(), lv.clone(), 1);
        let out2 = op.process_left_delta(&ld);
        assert_eq!(out2.len(), 1);
        let row = out2.iter().next().unwrap();
        assert_eq!(row.key, rk);
        assert_eq!(row.weight, 1);
    }

    // ─── Right anti join ─────────────────────────────────────────────────────

    #[test]
    fn right_anti_emits_right_when_no_left() {
        let mut op = OuterJoinOp::new(
            "ranti",
            JoinType::RightAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );

        let (rk, rv) = encode(10, 42);
        let mut rd = ZSet::new();
        rd.insert(rk.clone(), rv.clone(), 1);
        let out = op.process_right_delta(&rd);
        assert_eq!(out.len(), 1);
        let row = out.iter().next().unwrap();
        assert_eq!(row.key, rk);
        assert_eq!(row.weight, 1);
    }
}

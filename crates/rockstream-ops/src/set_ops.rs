//! Set-operation operators for RockStream IVM.
//!
//! Implements SQL `UNION ALL`, `UNION`, `INTERSECT ALL`, `INTERSECT`,
//! `EXCEPT ALL`, and `EXCEPT` as incremental Z-set operators driven by
//! `WeightAdd/v1`.
//!
//! # Operator summary
//!
//! | Operator        | SQL form          | Semantics                                       | Law-safe? |
//! |-----------------|-------------------|-------------------------------------------------|-----------|
//! | `UnionAllOp`    | `UNION ALL`       | output weight = left_w + right_w (linear)       | ✅ abelian group |
//! | `UnionOp`       | `UNION`           | `DISTINCT(UNION ALL)` — zero-crossing state     | ✅ abelian group |
//! | `IntersectAllOp`| `INTERSECT ALL`   | output weight = min(left_w, right_w)            | ❌ clamp_not_a_law |
//! | `IntersectOp`   | `INTERSECT`       | present iff in both; zero-crossing state        | ❌ clamp_not_a_law |
//! | `ExceptAllOp`   | `EXCEPT ALL`      | output weight = max(0, left_w − right_w)        | ❌ clamp_not_a_law |
//! | `ExceptOp`      | `EXCEPT`          | present iff in left but not right               | ❌ clamp_not_a_law |
//!
//! # `not_merge_safe_reason=clamp_not_a_law`
//!
//! INTERSECT and EXCEPT use min- or max-clamp operations that cannot be
//! expressed as an abelian-group law. Specifically:
//!
//! - `min(a, b)` is not invertible: given the output and one input you
//!   cannot recover the other.
//! - `max(0, a − b)` similarly loses information.
//!
//! Therefore the arrangement state for these operators is **not** driven by
//! a registered merge law; it uses raw `WeightAdd/v1` tracking on the
//! individual left and right weight accumulators, but the *output* is derived
//! via clamping. Compaction of the output arrangement is disabled until a
//! safety proof exists.
//!
//! # Compaction
//!
//! `UnionAllOp` and `UnionOp` are safe to compact (abelian group law).
//! `IntersectAllOp`, `IntersectOp`, `ExceptAllOp`, `ExceptOp` disable
//! compaction on their output arrangements; internal left/right weight
//! accumulators do use `WeightAdd/v1`.
//!
//! # Algorithm
//!
//! All operators store per-`(key, value)` weights using `WeightAdd/v1`
//! encoding. On each left-side or right-side delta:
//! 1. Update the relevant weight accumulator.
//! 2. Compute the old and new output weight.
//! 3. Emit `(key, value, new_output − old_output)` if different.

use std::collections::HashMap;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet};
use rockstream_types::laws::weight_add::{decode_weight, encode_weight, WEIGHT_ADD_ID};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

// ─── not_merge_safe_reason constant ──────────────────────────────────────────

/// The `not_merge_safe_reason` string for clamped set operations.
///
/// Both INTERSECT and EXCEPT apply min/max clamping that is not invertible
/// and therefore cannot be represented as a registered merge law.
pub const CLAMP_NOT_A_LAW: &str = "clamp_not_a_law";

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Decode a stored `WeightAdd/v1` state entry, returning 0 for absent keys.
fn load_weight(map: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>, key: &(Vec<u8>, Vec<u8>)) -> i64 {
    map.get(key)
        .and_then(|b| decode_weight(b).ok())
        .unwrap_or(0)
}

/// Store a weight, removing the entry if it is zero.
fn store_weight(map: &mut HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>, key: (Vec<u8>, Vec<u8>), w: i64) {
    if w == 0 {
        map.remove(&key);
    } else {
        map.insert(key, encode_weight(w));
    }
}

// ─── UnionAllOp ───────────────────────────────────────────────────────────────

/// `UNION ALL` — bag union.
///
/// A purely linear operator: output weight = left_weight + right_weight.
/// No state is required — any delta from either side passes through directly.
///
/// The arrangement is backed by `WeightAdd/v1`.
pub struct UnionAllOp {
    name: String,
}

impl UnionAllOp {
    /// Create a new `UnionAllOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Apply a left-side delta.  Returns it unchanged (linear passthrough).
    pub fn process_left(&mut self, delta: &ZSet) -> ZSet {
        delta.clone()
    }

    /// Apply a right-side delta.  Returns it unchanged (linear passthrough).
    pub fn process_right(&mut self, delta: &ZSet) -> ZSet {
        delta.clone()
    }
}

#[async_trait]
impl Operator for UnionAllOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(WEIGHT_ADD_ID)
    }
}

// ─── UnionOp ──────────────────────────────────────────────────────────────────

/// `UNION` (DISTINCT) — set union.
///
/// Equivalent to `DISTINCT(UNION ALL)`.
/// Maintains per-`(key, value)` total weight from both sides combined.
/// Zero-crossing detection emits `+1` / `−1` when a row transitions between
/// absent and present.
///
/// The arrangement is backed by `WeightAdd/v1`.
pub struct UnionOp {
    name: String,
    /// Combined weight from both sides.
    weight_state: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
}

impl UnionOp {
    /// Create a new `UnionOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            weight_state: HashMap::new(),
        }
    }

    /// Apply a delta from either side and emit the distinct output delta.
    pub fn process_delta(&mut self, delta: &ZSet) -> ZSet {
        let mut output = ZSet::new();
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_w = load_weight(&self.weight_state, &k);
            let new_w = old_w + row.weight;
            store_weight(&mut self.weight_state, k, new_w);
            match (old_w > 0, new_w > 0) {
                (false, true) => output.insert(row.key.clone(), row.value.clone(), 1),
                (true, false) => output.insert(row.key.clone(), row.value.clone(), -1),
                _ => {}
            }
        }
        output
    }
}

#[async_trait]
impl Operator for UnionOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(WEIGHT_ADD_ID)
    }
}

// ─── IntersectAllOp ───────────────────────────────────────────────────────────

/// `INTERSECT ALL` — bag intersection.
///
/// Output weight = `min(left_weight, right_weight)` per `(key, value)`.
///
/// The clamped output is **not** law-safe (`not_merge_safe_reason = clamp_not_a_law`).
/// Internal per-side weight accumulators use `WeightAdd/v1` encoding.
/// Compaction on the output arrangement is disabled.
pub struct IntersectAllOp {
    name: String,
    /// Per-row weight from the left side.
    left_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    /// Per-row weight from the right side.
    right_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    /// Last emitted output weight per row (for computing output deltas).
    last_emitted: HashMap<(Vec<u8>, Vec<u8>), i64>,
}

impl IntersectAllOp {
    /// Create a new `IntersectAllOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            left_weights: HashMap::new(),
            right_weights: HashMap::new(),
            last_emitted: HashMap::new(),
        }
    }

    fn compute_output_weight(left_w: i64, right_w: i64) -> i64 {
        left_w.min(right_w).max(0)
    }

    fn process_side(
        side: &mut HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        other: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        last_emitted: &mut HashMap<(Vec<u8>, Vec<u8>), i64>,
        delta: &ZSet,
        this_is_left: bool,
    ) -> ZSet {
        let mut output = ZSet::new();
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_this = load_weight(side, &k);
            let new_this = old_this + row.weight;
            store_weight(side, k.clone(), new_this);

            let other_w = load_weight(other, &k);
            let (left_w, right_w) = if this_is_left {
                (new_this, other_w)
            } else {
                (other_w, new_this)
            };
            let new_out = Self::compute_output_weight(left_w, right_w);
            let old_out = *last_emitted.get(&k).unwrap_or(&0);
            let diff = new_out - old_out;
            if diff != 0 {
                output.insert(row.key.clone(), row.value.clone(), diff);
                if new_out == 0 {
                    last_emitted.remove(&k);
                } else {
                    last_emitted.insert(k, new_out);
                }
            }
        }
        output
    }

    /// Apply a left-side delta.
    pub fn process_left(&mut self, delta: &ZSet) -> ZSet {
        let right = &self.right_weights.clone();
        Self::process_side(
            &mut self.left_weights,
            right,
            &mut self.last_emitted,
            delta,
            true,
        )
    }

    /// Apply a right-side delta.
    pub fn process_right(&mut self, delta: &ZSet) -> ZSet {
        let left = &self.left_weights.clone();
        Self::process_side(
            &mut self.right_weights,
            left,
            &mut self.last_emitted,
            delta,
            false,
        )
    }

    /// `not_merge_safe_reason` for EXPLAIN INCREMENTAL.
    pub fn not_merge_safe_reason() -> &'static str {
        CLAMP_NOT_A_LAW
    }
}

#[async_trait]
impl Operator for IntersectAllOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    // Output arrangement is not law-backed (clamp is not a law).
    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ─── IntersectOp ──────────────────────────────────────────────────────────────

/// `INTERSECT` (DISTINCT) — set intersection.
///
/// A row is present in the output iff `left_weight > 0 AND right_weight > 0`.
/// Zero-crossing detection emits `+1` / `−1` accordingly.
///
/// The output is **not** law-safe (`not_merge_safe_reason = clamp_not_a_law`).
pub struct IntersectOp {
    name: String,
    left_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    right_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    /// Whether the row is currently emitted in the output.
    last_emitted: HashMap<(Vec<u8>, Vec<u8>), bool>,
}

impl IntersectOp {
    /// Create a new `IntersectOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            left_weights: HashMap::new(),
            right_weights: HashMap::new(),
            last_emitted: HashMap::new(),
        }
    }

    fn process_side(
        side: &mut HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        other: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        last_emitted: &mut HashMap<(Vec<u8>, Vec<u8>), bool>,
        delta: &ZSet,
        this_is_left: bool,
    ) -> ZSet {
        let mut output = ZSet::new();
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_this = load_weight(side, &k);
            let new_this = old_this + row.weight;
            store_weight(side, k.clone(), new_this);

            let other_w = load_weight(other, &k);
            let (left_w, right_w) = if this_is_left {
                (new_this, other_w)
            } else {
                (other_w, new_this)
            };
            let was_present = *last_emitted.get(&k).unwrap_or(&false);
            let now_present = left_w > 0 && right_w > 0;
            match (was_present, now_present) {
                (false, true) => {
                    output.insert(row.key.clone(), row.value.clone(), 1);
                    last_emitted.insert(k, true);
                }
                (true, false) => {
                    output.insert(row.key.clone(), row.value.clone(), -1);
                    last_emitted.remove(&k);
                }
                _ => {}
            }
        }
        output
    }

    /// Apply a left-side delta.
    pub fn process_left(&mut self, delta: &ZSet) -> ZSet {
        let right = &self.right_weights.clone();
        Self::process_side(
            &mut self.left_weights,
            right,
            &mut self.last_emitted,
            delta,
            true,
        )
    }

    /// Apply a right-side delta.
    pub fn process_right(&mut self, delta: &ZSet) -> ZSet {
        let left = &self.left_weights.clone();
        Self::process_side(
            &mut self.right_weights,
            left,
            &mut self.last_emitted,
            delta,
            false,
        )
    }

    /// `not_merge_safe_reason` for EXPLAIN INCREMENTAL.
    pub fn not_merge_safe_reason() -> &'static str {
        CLAMP_NOT_A_LAW
    }
}

#[async_trait]
impl Operator for IntersectOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ─── ExceptAllOp ──────────────────────────────────────────────────────────────

/// `EXCEPT ALL` — bag difference.
///
/// Output weight = `max(0, left_weight − right_weight)` per `(key, value)`.
///
/// The clamped output is **not** law-safe (`not_merge_safe_reason = clamp_not_a_law`).
pub struct ExceptAllOp {
    name: String,
    left_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    right_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    last_emitted: HashMap<(Vec<u8>, Vec<u8>), i64>,
}

impl ExceptAllOp {
    /// Create a new `ExceptAllOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            left_weights: HashMap::new(),
            right_weights: HashMap::new(),
            last_emitted: HashMap::new(),
        }
    }

    fn compute_output_weight(left_w: i64, right_w: i64) -> i64 {
        (left_w - right_w).max(0)
    }

    fn process_side(
        side: &mut HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        other: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        last_emitted: &mut HashMap<(Vec<u8>, Vec<u8>), i64>,
        delta: &ZSet,
        this_is_left: bool,
    ) -> ZSet {
        let mut output = ZSet::new();
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_this = load_weight(side, &k);
            let new_this = old_this + row.weight;
            store_weight(side, k.clone(), new_this);

            let other_w = load_weight(other, &k);
            let (left_w, right_w) = if this_is_left {
                (new_this, other_w)
            } else {
                (other_w, new_this)
            };
            let new_out = Self::compute_output_weight(left_w, right_w);
            let old_out = *last_emitted.get(&k).unwrap_or(&0);
            let diff = new_out - old_out;
            if diff != 0 {
                output.insert(row.key.clone(), row.value.clone(), diff);
                if new_out == 0 {
                    last_emitted.remove(&k);
                } else {
                    last_emitted.insert(k, new_out);
                }
            }
        }
        output
    }

    /// Apply a left-side delta.
    pub fn process_left(&mut self, delta: &ZSet) -> ZSet {
        let right = &self.right_weights.clone();
        Self::process_side(
            &mut self.left_weights,
            right,
            &mut self.last_emitted,
            delta,
            true,
        )
    }

    /// Apply a right-side delta.
    pub fn process_right(&mut self, delta: &ZSet) -> ZSet {
        let left = &self.left_weights.clone();
        Self::process_side(
            &mut self.right_weights,
            left,
            &mut self.last_emitted,
            delta,
            false,
        )
    }

    /// `not_merge_safe_reason` for EXPLAIN INCREMENTAL.
    pub fn not_merge_safe_reason() -> &'static str {
        CLAMP_NOT_A_LAW
    }
}

#[async_trait]
impl Operator for ExceptAllOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ─── ExceptOp ─────────────────────────────────────────────────────────────────

/// `EXCEPT` (DISTINCT) — set difference.
///
/// A row is present in the output iff `left_weight > 0 AND right_weight == 0`.
/// Zero-crossing detection emits `+1` / `−1` accordingly.
///
/// The output is **not** law-safe (`not_merge_safe_reason = clamp_not_a_law`).
pub struct ExceptOp {
    name: String,
    left_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    right_weights: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    last_emitted: HashMap<(Vec<u8>, Vec<u8>), bool>,
}

impl ExceptOp {
    /// Create a new `ExceptOp`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            left_weights: HashMap::new(),
            right_weights: HashMap::new(),
            last_emitted: HashMap::new(),
        }
    }

    fn process_side(
        side: &mut HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        other: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
        last_emitted: &mut HashMap<(Vec<u8>, Vec<u8>), bool>,
        delta: &ZSet,
        this_is_left: bool,
    ) -> ZSet {
        let mut output = ZSet::new();
        for row in delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_this = load_weight(side, &k);
            let new_this = old_this + row.weight;
            store_weight(side, k.clone(), new_this);

            let other_w = load_weight(other, &k);
            let (left_w, right_w) = if this_is_left {
                (new_this, other_w)
            } else {
                (other_w, new_this)
            };
            let was_present = *last_emitted.get(&k).unwrap_or(&false);
            let now_present = left_w > 0 && right_w == 0;
            match (was_present, now_present) {
                (false, true) => {
                    output.insert(row.key.clone(), row.value.clone(), 1);
                    last_emitted.insert(k, true);
                }
                (true, false) => {
                    output.insert(row.key.clone(), row.value.clone(), -1);
                    last_emitted.remove(&k);
                }
                _ => {}
            }
        }
        output
    }

    /// Apply a left-side delta.
    pub fn process_left(&mut self, delta: &ZSet) -> ZSet {
        let right = &self.right_weights.clone();
        Self::process_side(
            &mut self.left_weights,
            right,
            &mut self.last_emitted,
            delta,
            true,
        )
    }

    /// Apply a right-side delta.
    pub fn process_right(&mut self, delta: &ZSet) -> ZSet {
        let left = &self.left_weights.clone();
        Self::process_side(
            &mut self.right_weights,
            left,
            &mut self.last_emitted,
            delta,
            false,
        )
    }

    /// `not_merge_safe_reason` for EXPLAIN INCREMENTAL.
    pub fn not_merge_safe_reason() -> &'static str {
        CLAMP_NOT_A_LAW
    }
}

#[async_trait]
impl Operator for ExceptOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), id.to_be_bytes().to_vec())
    }

    fn zset_one(id: i64, w: i64) -> ZSet {
        let mut z = ZSet::new();
        let (k, v) = row(id);
        z.insert(k, v, w);
        z
    }

    // ─── UnionAll ─────────────────────────────────────────────────────────

    #[test]
    fn union_all_left_passthrough() {
        let mut op = UnionAllOp::new("t");
        let input = zset_one(1, 3);
        let out = op.process_left(&input);
        assert_eq!(out.len(), 1);
        let r = out.iter().next().unwrap();
        assert_eq!(r.weight, 3);
    }

    #[test]
    fn union_all_right_passthrough() {
        let mut op = UnionAllOp::new("t");
        let input = zset_one(2, 2);
        let out = op.process_right(&input);
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().next().unwrap().weight, 2);
    }

    // ─── Union ────────────────────────────────────────────────────────────

    #[test]
    fn union_emits_on_first_insert() {
        let mut op = UnionOp::new("t");
        let out = op.process_delta(&zset_one(1, 1));
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().next().unwrap().weight, 1);
    }

    #[test]
    fn union_no_duplicate_emit() {
        let mut op = UnionOp::new("t");
        op.process_delta(&zset_one(1, 1));
        let out = op.process_delta(&zset_one(1, 1));
        assert_eq!(out.len(), 0, "already present, no re-emit");
    }

    #[test]
    fn union_retract_removes() {
        let mut op = UnionOp::new("t");
        op.process_delta(&zset_one(1, 1));
        let out = op.process_delta(&zset_one(1, -1));
        assert_eq!(out.iter().next().unwrap().weight, -1);
    }

    // ─── IntersectAll ─────────────────────────────────────────────────────

    #[test]
    fn intersect_all_no_right_no_output() {
        let mut op = IntersectAllOp::new("t");
        let out = op.process_left(&zset_one(1, 3));
        assert_eq!(out.len(), 0, "no right side yet");
    }

    #[test]
    fn intersect_all_min_weight() {
        let mut op = IntersectAllOp::new("t");
        op.process_left(&zset_one(1, 3));
        let out = op.process_right(&zset_one(1, 2));
        // min(3, 2) = 2
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().next().unwrap().weight, 2);
    }

    #[test]
    fn intersect_all_right_less_than_left() {
        let mut op = IntersectAllOp::new("t");
        op.process_right(&zset_one(1, 2));
        let out = op.process_left(&zset_one(1, 5));
        // min(5, 2) = 2
        assert_eq!(out.iter().next().unwrap().weight, 2);
    }

    #[test]
    fn intersect_all_not_merge_safe_reason() {
        assert_eq!(IntersectAllOp::not_merge_safe_reason(), "clamp_not_a_law");
    }

    // ─── Intersect ────────────────────────────────────────────────────────

    #[test]
    fn intersect_not_present_until_both_sides() {
        let mut op = IntersectOp::new("t");
        let out_l = op.process_left(&zset_one(1, 1));
        assert_eq!(out_l.len(), 0, "only left, not yet in output");
        let out_r = op.process_right(&zset_one(1, 1));
        assert_eq!(out_r.iter().next().unwrap().weight, 1);
    }

    #[test]
    fn intersect_retract_right_removes_output() {
        let mut op = IntersectOp::new("t");
        op.process_left(&zset_one(1, 1));
        op.process_right(&zset_one(1, 1));
        let out = op.process_right(&zset_one(1, -1));
        assert_eq!(out.iter().next().unwrap().weight, -1);
    }

    // ─── ExceptAll ────────────────────────────────────────────────────────

    #[test]
    fn except_all_no_right_passes_left() {
        let mut op = ExceptAllOp::new("t");
        let out = op.process_left(&zset_one(1, 3));
        // max(0, 3 - 0) = 3
        assert_eq!(out.iter().next().unwrap().weight, 3);
    }

    #[test]
    fn except_all_right_subtracts() {
        let mut op = ExceptAllOp::new("t");
        op.process_left(&zset_one(1, 3));
        // Add right weight 2: max(0, 3-2)=1, delta from 3 is -2
        let out = op.process_right(&zset_one(1, 2));
        assert_eq!(out.iter().next().unwrap().weight, -2);
    }

    #[test]
    fn except_all_right_exceeds_left_clamps_to_zero() {
        let mut op = ExceptAllOp::new("t");
        op.process_left(&zset_one(1, 2));
        // right=3: max(0, 2-3)=0, delta=-2
        let out = op.process_right(&zset_one(1, 3));
        assert_eq!(out.iter().next().unwrap().weight, -2);
    }

    #[test]
    fn except_all_not_merge_safe_reason() {
        assert_eq!(ExceptAllOp::not_merge_safe_reason(), "clamp_not_a_law");
    }

    // ─── Except ───────────────────────────────────────────────────────────

    #[test]
    fn except_present_when_only_in_left() {
        let mut op = ExceptOp::new("t");
        let out = op.process_left(&zset_one(1, 1));
        assert_eq!(out.iter().next().unwrap().weight, 1);
    }

    #[test]
    fn except_not_present_when_also_in_right() {
        let mut op = ExceptOp::new("t");
        op.process_left(&zset_one(1, 1));
        let out = op.process_right(&zset_one(1, 1));
        assert_eq!(out.iter().next().unwrap().weight, -1);
    }

    #[test]
    fn except_reappears_when_right_retracted() {
        let mut op = ExceptOp::new("t");
        op.process_left(&zset_one(1, 1));
        op.process_right(&zset_one(1, 1));
        let out = op.process_right(&zset_one(1, -1));
        assert_eq!(
            out.iter().next().unwrap().weight,
            1,
            "row reappears after right retraction"
        );
    }

    #[test]
    fn except_not_merge_safe_reason() {
        assert_eq!(ExceptOp::not_merge_safe_reason(), "clamp_not_a_law");
    }
}

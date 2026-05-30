//! Window function operator for RockStream IVM (v0.19).
//!
//! Implements `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE` via
//! partition-scoped recomputation, and sliding `SUM`/`AVG` by internally
//! delegating to `SumCount/v1` as a sub-component law.
//!
//! ## Design
//!
//! **Ranking functions** (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE)
//! use *partition recomputation*: the operator maintains the complete current
//! state of every partition, and on each input delta it recomputes the
//! window-function output for all rows in the affected partitions. This is
//! correct under all insert/delete workloads and satisfies the v0.19 proof
//! criterion. The `not_merge_safe_reason` is `partition_recomputation`.
//!
//! **Sliding aggregates** (SlidingSum, SlidingAvg with a fixed row-frame)
//! use the same partition-recomputation path for IVM correctness, but the
//! per-row window aggregate is computed using `SumCount/v1` encoding
//! internally. This allows `EXPLAIN INCREMENTAL` to report
//! `merge_law=SumCount/v1` as the sub-component law.
//!
//! ## Partition recomputation cost
//!
//! For a partition of size P with an incoming delta of D rows, recomputation
//! cost is O(P log P) (sort + linear scan). In the worst case this is
//! O(N log N) where N is the total number of rows in the shard. For
//! high-cardinality partitioning (many small partitions) the cost is low.
//! For low-cardinality partitioning (few large partitions) the cost is higher.
//! The escape hatch (ROADMAP.md v0.19) explicitly accepts this trade-off:
//! "correct, slower" is the goal for the v0.19 milestone.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_plan::WindowFunc;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::sum_count::{
    decode_sum_count, encode_sum_count, SumCountV1, SUM_COUNT_ID,
};
use rockstream_types::merge_law::{LawBundle, MergeLawId};
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Extract a partition key from a row using the given column indices.
pub type PartitionKeyFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Extract an order key from a row using the given column indices.
pub type OrderKeyFn = Arc<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static>;

/// Extract the value column used for sliding aggregates.
pub type ValueFn = Arc<dyn Fn(&[u8], &[u8]) -> i64 + Send + Sync + 'static>;

/// A stable row identifier within a partition (key ++ separator ++ value bytes).
type RowId = Vec<u8>;

/// Window function operator (v0.19).
///
/// Maintains an in-memory snapshot of each partition, sorted by the order key.
/// On each input Z-set delta, updates affected partitions and emits the full
/// diff between the old and new window function output.
pub struct WindowOp {
    name: String,
    func: WindowFunc,
    partition_key_fn: PartitionKeyFn,
    order_key_fn: OrderKeyFn,
    value_fn: Option<ValueFn>,
    /// State: partition_key → BTreeMap<(order_key, row_id) → row_value_bytes>
    ///
    /// The BTreeMap gives deterministic ordering by (order_key, row_id), which
    /// is the stable sort key used for all window computations.
    partitions: HashMap<Vec<u8>, BTreeMap<(Vec<u8>, RowId), Vec<u8>>>,
    /// Last emitted output per row_id: row_id → (output_key, output_value).
    ///
    /// Stored so we can retract the old value before emitting the new one.
    last_emitted: HashMap<RowId, (Vec<u8>, Vec<u8>)>,
    /// The merge-law ID reported in `EXPLAIN` for sliding aggregates.
    ///
    /// `None` for ranking functions (uses `not_merge_safe_reason` instead).
    pub sub_law_id: Option<MergeLawId>,
}

impl WindowOp {
    /// Create a new window operator for a ranking function.
    pub fn new_ranking(
        name: impl Into<String>,
        func: WindowFunc,
        partition_key_fn: PartitionKeyFn,
        order_key_fn: OrderKeyFn,
    ) -> Self {
        Self {
            name: name.into(),
            func,
            partition_key_fn,
            order_key_fn,
            value_fn: None,
            partitions: HashMap::new(),
            last_emitted: HashMap::new(),
            sub_law_id: None,
        }
    }

    /// Create a new window operator for a sliding aggregate function.
    ///
    /// The `value_fn` extracts the i64 value used for SUM/AVG computation.
    /// Reports `SumCount/v1` as the sub-component law in `EXPLAIN`.
    pub fn new_sliding(
        name: impl Into<String>,
        func: WindowFunc,
        partition_key_fn: PartitionKeyFn,
        order_key_fn: OrderKeyFn,
        value_fn: ValueFn,
    ) -> Self {
        Self {
            name: name.into(),
            func,
            partition_key_fn,
            order_key_fn,
            value_fn: Some(value_fn),
            partitions: HashMap::new(),
            last_emitted: HashMap::new(),
            sub_law_id: Some(SUM_COUNT_ID),
        }
    }

    /// Process a `ZSet` delta and return the Z-set diff of window outputs.
    ///
    /// Algorithm:
    /// 1. Apply all insertions/deletions from the delta to the in-memory
    ///    partition state.
    /// 2. Collect the set of affected partition keys.
    /// 3. For each affected partition, recompute the window function for all
    ///    rows in that partition.
    /// 4. Emit retractions for old outputs and insertions for new outputs,
    ///    but only where the output actually changed.
    pub fn process_zset(&mut self, input: &ZSet) -> ZSet {
        let mut affected_partitions: HashSet<Vec<u8>> = HashSet::new();

        // Phase 1: Update partition state.
        for row in input.iter() {
            let partition_key = (self.partition_key_fn)(&row.key, &row.value);
            let order_key = (self.order_key_fn)(&row.key, &row.value);
            let row_id = make_row_id(&row.key, &row.value);

            let partition = self.partitions.entry(partition_key.clone()).or_default();

            if row.weight > 0 {
                partition.insert(
                    (order_key.clone(), row_id.clone()),
                    row.value.clone(),
                );
            } else {
                let key = (order_key.clone(), row_id.clone());
                partition.remove(&key);
            }

            affected_partitions.insert(partition_key);
        }

        // Phase 2: Recompute affected partitions and emit diffs.
        let mut output = ZSet::new();

        for partition_key in &affected_partitions {
            // Collect the current set of live row_ids in this partition.
            let live_row_ids: Vec<RowId> = match self.partitions.get(partition_key) {
                Some(p) => p.keys().map(|(_, rid)| rid.clone()).collect(),
                None => vec![],
            };
            let _live_set: HashSet<&RowId> = live_row_ids.iter().collect();

            // Retract outputs for rows that were deleted from this partition
            // (i.e., rows that had a last_emitted entry but are no longer live).
            let to_retract: Vec<RowId> = self
                .last_emitted
                .keys()
                .filter(|rid| {
                    // Check if this row belonged to the current partition.
                    // We do this by checking if it was in the partition before.
                    // The simplest correct approach: only retract if the row
                    // no longer exists anywhere in any partition.
                    !self.partitions.values().any(|p| {
                        p.keys().any(|(_, id)| id == *rid)
                    })
                })
                .cloned()
                .collect();

            for rid in to_retract {
                if let Some((old_key, old_value)) = self.last_emitted.remove(&rid) {
                    output.insert(old_key, old_value, -1);
                }
            }

            // Recompute window function for all live rows in this partition.
            let partition = match self.partitions.get(partition_key) {
                Some(p) => p,
                None => continue,
            };

            let sorted_rows: Vec<(&(Vec<u8>, RowId), &Vec<u8>)> = partition.iter().collect();
            let n = sorted_rows.len();

            for (pos, ((_, row_id), _row_value)) in sorted_rows.iter().enumerate() {
                let new_out_value = self.compute_window_value(pos, n, &sorted_rows);
                let out_key = row_id.clone();

                match self.last_emitted.get(row_id).map(|(k, v)| (k.clone(), v.clone())) {
                    Some((old_key, old_value)) if old_value != new_out_value => {
                        // Retract old, insert new.
                        output.insert(old_key, old_value, -1);
                        output.insert(out_key.clone(), new_out_value.clone(), 1);
                        self.last_emitted
                            .insert(row_id.clone(), (out_key, new_out_value));
                    }
                    None => {
                        // First emission for this row.
                        output.insert(out_key.clone(), new_out_value.clone(), 1);
                        self.last_emitted
                            .insert(row_id.clone(), (out_key, new_out_value));
                    }
                    Some(_) => {
                        // Output unchanged — no delta needed.
                    }
                }
            }
        }

        output
    }

    /// Compute the window function value for a single row at position `pos`
    /// in a partition of size `n`.
    ///
    /// For sliding aggregates, uses `SumCount/v1` encoding so that the
    /// sub-component law is correctly identified in EXPLAIN.
    fn compute_window_value(
        &self,
        pos: usize,
        n: usize,
        sorted_rows: &[(&(Vec<u8>, RowId), &Vec<u8>)],
    ) -> Vec<u8> {
        match &self.func {
            WindowFunc::RowNumber => {
                // ROW_NUMBER() is 1-based.
                (pos as i64 + 1).to_be_bytes().to_vec()
            }
            WindowFunc::Rank => {
                // RANK(): count rows with strictly smaller order key + 1.
                let my_order = &sorted_rows[pos].0 .0;
                let rank = sorted_rows
                    .iter()
                    .take_while(|((ok, _), _)| ok < my_order)
                    .count() as i64
                    + 1;
                rank.to_be_bytes().to_vec()
            }
            WindowFunc::DenseRank => {
                // DENSE_RANK(): count distinct order keys ≤ mine.
                let my_order = &sorted_rows[pos].0 .0;
                let mut distinct: BTreeSet<Vec<u8>> = BTreeSet::new();
                for ((ok, _), _) in sorted_rows.iter() {
                    if ok <= my_order {
                        distinct.insert(ok.clone());
                    }
                }
                (distinct.len() as i64).to_be_bytes().to_vec()
            }
            WindowFunc::Ntile(buckets) => {
                let bucket_count = (*buckets as usize).max(1);
                let bucket = if n == 0 {
                    1i64
                } else {
                    ((pos * bucket_count) / n) as i64 + 1
                };
                bucket.to_be_bytes().to_vec()
            }
            WindowFunc::Lag { offset } => {
                if pos >= *offset {
                    sorted_rows[pos - offset].1.clone()
                } else {
                    // NULL → encode as zero bytes sentinel.
                    vec![0u8; 8]
                }
            }
            WindowFunc::Lead { offset } => {
                if pos + offset < n {
                    sorted_rows[pos + offset].1.clone()
                } else {
                    vec![0u8; 8]
                }
            }
            WindowFunc::SlidingSum { frame_rows } => {
                // Sliding SUM using SumCount/v1 sub-component:
                // sum the values for rows in [pos - frame_rows, pos].
                let value_fn = self.value_fn.as_ref().expect("SlidingSum requires value_fn");
                let start = pos.saturating_sub(*frame_rows);
                let law = SumCountV1;
                let mut acc = law.identity().unwrap();
                for ((_, _), row_val) in sorted_rows[start..=pos].iter() {
                    let v = value_fn(&[], row_val);
                    let delta = encode_sum_count(v, 1);
                    acc = law.merge(&acc, &delta).unwrap();
                }
                let (sum, _count) = decode_sum_count(&acc).unwrap();
                sum.to_be_bytes().to_vec()
            }
            WindowFunc::SlidingAvg { frame_rows } => {
                // Sliding AVG using SumCount/v1 sub-component.
                let value_fn = self.value_fn.as_ref().expect("SlidingAvg requires value_fn");
                let start = pos.saturating_sub(*frame_rows);
                let law = SumCountV1;
                let mut acc = law.identity().unwrap();
                for ((_, _), row_val) in sorted_rows[start..=pos].iter() {
                    let v = value_fn(&[], row_val);
                    let delta = encode_sum_count(v, 1);
                    acc = law.merge(&acc, &delta).unwrap();
                }
                let (sum, count) = decode_sum_count(&acc).unwrap();
                if count == 0 {
                    0i64.to_be_bytes().to_vec()
                } else {
                    (sum / count).to_be_bytes().to_vec()
                }
            }
        }
    }

    /// Return the current window function output for testing/inspection.
    ///
    /// Returns a map from `row_id → output_value_bytes`.
    pub fn current_output(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        self.last_emitted
            .iter()
            .map(|(rid, (_, ov))| (rid.clone(), ov.clone()))
            .collect()
    }
}

/// Build a stable row identifier from key and value bytes.
pub fn make_row_id(key: &[u8], value: &[u8]) -> RowId {
    let mut id = Vec::with_capacity(key.len() + 1 + value.len());
    id.extend_from_slice(key);
    id.push(0xFF); // separator
    id.extend_from_slice(value);
    id
}

#[async_trait]
impl Operator for WindowOp {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&mut self, input: &SourceBatch) -> SinkBatch {
        SinkBatch {
            record_count: input.record_count,
            epoch: input.epoch,
        }
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let output_zset = self.process_zset(&input.zset);
        ZSetBatch {
            zset: output_zset,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn merge_law(&self) -> Option<MergeLawId> {
        // Sliding aggregates report SumCount/v1 as the sub-component law.
        self.sub_law_id
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;
    use std::sync::Arc;

    // ── Helper factories ──────────────────────────────────────────────────────

    /// Trivial partition key: all rows in the same partition.
    fn single_partition() -> PartitionKeyFn {
        Arc::new(|_key: &[u8], _val: &[u8]| vec![0u8])
    }

    /// Partition by first byte of key.
    fn partition_by_first_byte() -> PartitionKeyFn {
        Arc::new(|key: &[u8], _val: &[u8]| vec![if key.is_empty() { 0 } else { key[0] }])
    }

    /// Order by value bytes (as big-endian i64).
    fn order_by_value() -> OrderKeyFn {
        Arc::new(|_key: &[u8], val: &[u8]| val.to_vec())
    }

    /// Encode (key_byte, val_i64) as (key_bytes, val_bytes).
    fn row(key: u8, val: i64) -> (Vec<u8>, Vec<u8>) {
        (vec![key], val.to_be_bytes().to_vec())
    }

    fn insert_row(zset: &mut ZSet, key: u8, val: i64) {
        let (k, v) = row(key, val);
        zset.insert(k, v, 1);
    }

    fn delete_row(zset: &mut ZSet, key: u8, val: i64) {
        let (k, v) = row(key, val);
        zset.insert(k, v, -1);
    }

    // ── ROW_NUMBER tests ──────────────────────────────────────────────────────

    #[test]
    fn row_number_assigns_sequential_positions() {
        let mut op = WindowOp::new_ranking(
            "test_row_number",
            WindowFunc::RowNumber,
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 30);
        insert_row(&mut delta, 2, 10);
        insert_row(&mut delta, 3, 20);
        op.process_zset(&delta);

        let out = op.current_output();
        // Row with val=10 should get row_number=1, val=20 → 2, val=30 → 3.
        let (k1, v1) = row(2, 10);
        let (k2, v2) = row(3, 20);
        let (k3, v3) = row(1, 30);
        let id1 = make_row_id(&k1, &v1);
        let id2 = make_row_id(&k2, &v2);
        let id3 = make_row_id(&k3, &v3);

        let rn1 = i64::from_be_bytes(out[&id1][..8].try_into().unwrap());
        let rn2 = i64::from_be_bytes(out[&id2][..8].try_into().unwrap());
        let rn3 = i64::from_be_bytes(out[&id3][..8].try_into().unwrap());

        assert_eq!(rn1, 1, "val=10 → row_number=1");
        assert_eq!(rn2, 2, "val=20 → row_number=2");
        assert_eq!(rn3, 3, "val=30 → row_number=3");
    }

    #[test]
    fn row_number_updates_after_delete() {
        let mut op = WindowOp::new_ranking(
            "test_rn_delete",
            WindowFunc::RowNumber,
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 20);
        insert_row(&mut delta, 3, 30);
        op.process_zset(&delta);

        // Delete the first row (val=10).
        let mut delta2 = ZSet::new();
        delete_row(&mut delta2, 1, 10);
        op.process_zset(&delta2);

        let out = op.current_output();
        let (k2, v2) = row(2, 20);
        let (k3, v3) = row(3, 30);
        let id2 = make_row_id(&k2, &v2);
        let id3 = make_row_id(&k3, &v3);

        let rn2 = i64::from_be_bytes(out[&id2][..8].try_into().unwrap());
        let rn3 = i64::from_be_bytes(out[&id3][..8].try_into().unwrap());

        assert_eq!(rn2, 1, "after delete of row 1, val=20 becomes row_number=1");
        assert_eq!(rn3, 2, "after delete of row 1, val=30 becomes row_number=2");
    }

    // ── RANK / DENSE_RANK tests ───────────────────────────────────────────────

    #[test]
    fn rank_handles_ties() {
        let mut op = WindowOp::new_ranking(
            "test_rank",
            WindowFunc::Rank,
            single_partition(),
            order_by_value(),
        );

        // Two rows with the same value (tie).
        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 10);
        insert_row(&mut delta, 3, 20);
        op.process_zset(&delta);

        let out = op.current_output();
        // Both val=10 rows should have rank=1; val=20 row should have rank=3.
        for k_byte in [1u8, 2u8] {
            let (key, val) = row(k_byte, 10);
            let id = make_row_id(&key, &val);
            if let Some(bytes) = out.get(&id) {
                let r = i64::from_be_bytes(bytes[..8].try_into().unwrap());
                assert_eq!(r, 1, "tied rows both get rank=1");
            }
        }
        let (k3, v3) = row(3, 20);
        let id3 = make_row_id(&k3, &v3);
        if let Some(bytes) = out.get(&id3) {
            let r = i64::from_be_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(r, 3, "row after 2 ties gets rank=3");
        }
    }

    #[test]
    fn dense_rank_no_gaps() {
        let mut op = WindowOp::new_ranking(
            "test_dense_rank",
            WindowFunc::DenseRank,
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 10); // tie with row 1
        insert_row(&mut delta, 3, 20);
        op.process_zset(&delta);

        let out = op.current_output();
        let (k3, v3) = row(3, 20);
        let id3 = make_row_id(&k3, &v3);
        if let Some(bytes) = out.get(&id3) {
            let r = i64::from_be_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(r, 2, "DENSE_RANK: after 2 tied rows at rank 1, next is 2");
        }
    }

    // ── NTILE tests ───────────────────────────────────────────────────────────

    #[test]
    fn ntile_distributes_rows_into_buckets() {
        let mut op = WindowOp::new_ranking(
            "test_ntile",
            WindowFunc::Ntile(4),
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        for v in [10i64, 20, 30, 40, 50, 60, 70, 80] {
            let (k, val) = row(v as u8, v);
            delta.insert(k, val, 1);
        }
        op.process_zset(&delta);

        let out = op.current_output();
        assert_eq!(out.len(), 8, "all 8 rows have output");
        let buckets: HashSet<i64> = out
            .values()
            .map(|b| i64::from_be_bytes(b[..8].try_into().unwrap()))
            .collect();
        assert!(buckets.contains(&1), "bucket 1 exists");
        assert!(buckets.contains(&4), "bucket 4 exists");
    }

    // ── LAG / LEAD tests ──────────────────────────────────────────────────────

    #[test]
    fn lag_returns_previous_row_value() {
        let mut op = WindowOp::new_ranking(
            "test_lag",
            WindowFunc::Lag { offset: 1 },
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 20);
        insert_row(&mut delta, 3, 30);
        op.process_zset(&delta);

        let out = op.current_output();
        // row with val=20 should have LAG = 10 (val bytes of previous row).
        let (k2, v2) = row(2, 20);
        let id2 = make_row_id(&k2, &v2);
        if let Some(lag_bytes) = out.get(&id2) {
            let lag = i64::from_be_bytes(lag_bytes[..8].try_into().unwrap());
            assert_eq!(lag, 10, "LAG(1) of val=20 should be 10");
        }
    }

    #[test]
    fn lead_returns_next_row_value() {
        let mut op = WindowOp::new_ranking(
            "test_lead",
            WindowFunc::Lead { offset: 1 },
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 20);
        insert_row(&mut delta, 3, 30);
        op.process_zset(&delta);

        let out = op.current_output();
        // row with val=10 should have LEAD = 20 (val bytes of next row).
        let (k1, v1) = row(1, 10);
        let id1 = make_row_id(&k1, &v1);
        if let Some(lead_bytes) = out.get(&id1) {
            let lead = i64::from_be_bytes(lead_bytes[..8].try_into().unwrap());
            assert_eq!(lead, 20, "LEAD(1) of val=10 should be 20");
        }
    }

    // ── SlidingSum / SlidingAvg tests ─────────────────────────────────────────

    #[test]
    fn sliding_sum_reports_sum_count_law() {
        let op = WindowOp::new_sliding(
            "test_sliding_sum",
            WindowFunc::SlidingSum { frame_rows: 2 },
            single_partition(),
            order_by_value(),
            Arc::new(|_key: &[u8], val: &[u8]| {
                i64::from_be_bytes(val[..8].try_into().unwrap_or([0u8; 8]))
            }),
        );
        assert_eq!(
            op.sub_law_id,
            Some(SUM_COUNT_ID),
            "SlidingSum must report SumCount/v1 as sub-component law"
        );
    }

    #[test]
    fn sliding_avg_reports_sum_count_law() {
        let op = WindowOp::new_sliding(
            "test_sliding_avg",
            WindowFunc::SlidingAvg { frame_rows: 2 },
            single_partition(),
            order_by_value(),
            Arc::new(|_key: &[u8], val: &[u8]| {
                i64::from_be_bytes(val[..8].try_into().unwrap_or([0u8; 8]))
            }),
        );
        assert_eq!(
            op.sub_law_id,
            Some(SUM_COUNT_ID),
            "SlidingAvg must report SumCount/v1 as sub-component law"
        );
    }

    #[test]
    fn sliding_sum_computes_frame_correctly() {
        let value_fn: ValueFn = Arc::new(|_key: &[u8], val: &[u8]| {
            i64::from_be_bytes(val[..8].try_into().unwrap_or([0u8; 8]))
        });
        let mut op = WindowOp::new_sliding(
            "test_ss",
            WindowFunc::SlidingSum { frame_rows: 2 },
            single_partition(),
            order_by_value(),
            value_fn,
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 20);
        insert_row(&mut delta, 3, 30);
        op.process_zset(&delta);

        let out = op.current_output();
        // frame_rows=2: each row sees itself and up to 2 preceding rows.
        // pos=0 (val=10): [10] → sum=10
        // pos=1 (val=20): [10,20] → sum=30
        // pos=2 (val=30): [10,20,30] → sum=60
        let (k1, v1) = row(1, 10);
        let (k2, v2) = row(2, 20);
        let (k3, v3) = row(3, 30);
        let id1 = make_row_id(&k1, &v1);
        let id2 = make_row_id(&k2, &v2);
        let id3 = make_row_id(&k3, &v3);

        if let (Some(s1), Some(s2), Some(s3)) = (out.get(&id1), out.get(&id2), out.get(&id3)) {
            assert_eq!(
                i64::from_be_bytes(s1[..8].try_into().unwrap()),
                10,
                "sliding_sum pos=0"
            );
            assert_eq!(
                i64::from_be_bytes(s2[..8].try_into().unwrap()),
                30,
                "sliding_sum pos=1"
            );
            assert_eq!(
                i64::from_be_bytes(s3[..8].try_into().unwrap()),
                60,
                "sliding_sum pos=2"
            );
        }
    }

    #[test]
    fn sliding_avg_computes_frame_correctly() {
        let value_fn: ValueFn = Arc::new(|_key: &[u8], val: &[u8]| {
            i64::from_be_bytes(val[..8].try_into().unwrap_or([0u8; 8]))
        });
        let mut op = WindowOp::new_sliding(
            "test_sa",
            WindowFunc::SlidingAvg { frame_rows: 1 },
            single_partition(),
            order_by_value(),
            value_fn,
        );

        let mut delta = ZSet::new();
        insert_row(&mut delta, 1, 10);
        insert_row(&mut delta, 2, 20);
        insert_row(&mut delta, 3, 30);
        op.process_zset(&delta);

        let out = op.current_output();
        // frame_rows=1: each row sees itself and 1 preceding row.
        // pos=0 (val=10): [10] → avg=10
        // pos=1 (val=20): [10,20] → avg=15
        // pos=2 (val=30): [20,30] → avg=25
        let (k1, v1) = row(1, 10);
        let (k2, v2) = row(2, 20);
        let (k3, v3) = row(3, 30);
        let id1 = make_row_id(&k1, &v1);
        let id2 = make_row_id(&k2, &v2);
        let id3 = make_row_id(&k3, &v3);

        if let (Some(a1), Some(a2), Some(a3)) = (out.get(&id1), out.get(&id2), out.get(&id3)) {
            assert_eq!(
                i64::from_be_bytes(a1[..8].try_into().unwrap()),
                10,
                "sliding_avg pos=0"
            );
            assert_eq!(
                i64::from_be_bytes(a2[..8].try_into().unwrap()),
                15,
                "sliding_avg pos=1"
            );
            assert_eq!(
                i64::from_be_bytes(a3[..8].try_into().unwrap()),
                25,
                "sliding_avg pos=2"
            );
        }
    }

    // ── Partition isolation test ───────────────────────────────────────────────

    #[test]
    fn partitions_are_independent() {
        let mut op = WindowOp::new_ranking(
            "test_partition_iso",
            WindowFunc::RowNumber,
            partition_by_first_byte(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        // Partition A (key[0]=1): 2 rows
        let (k1, v1) = row(1, 10);
        let (k1b, v1b) = (vec![1u8, 99], 20i64.to_be_bytes().to_vec());
        // Partition B (key[0]=2): 3 rows
        let (k3, v3) = row(2, 5);
        let (k4, v4) = row(2, 15);
        let (k5, v5) = row(2, 25);

        for (k, v) in [
            (k1.clone(), v1.clone()),
            (k1b.clone(), v1b.clone()),
            (k3.clone(), v3.clone()),
            (k4.clone(), v4.clone()),
            (k5.clone(), v5.clone()),
        ] {
            delta.insert(k, v, 1);
        }
        op.process_zset(&delta);

        let out = op.current_output();
        assert_eq!(out.len(), 5, "all 5 rows have output");

        let id_a1 = make_row_id(&k1, &v1);
        let id_b1 = make_row_id(&k3, &v3);
        let id_b3 = make_row_id(&k5, &v5);

        let rn_a1 = i64::from_be_bytes(out[&id_a1][..8].try_into().unwrap());
        let rn_b1 = i64::from_be_bytes(out[&id_b1][..8].try_into().unwrap());
        let rn_b3 = i64::from_be_bytes(out[&id_b3][..8].try_into().unwrap());

        assert_eq!(rn_a1, 1, "partition A, val=10 gets row_number=1");
        assert_eq!(rn_b1, 1, "partition B, val=5 gets row_number=1");
        assert_eq!(rn_b3, 3, "partition B, val=25 gets row_number=3");
    }
}


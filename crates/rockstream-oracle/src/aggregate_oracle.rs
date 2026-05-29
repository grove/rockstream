//! DataFusion-based aggregate oracle for correctness verification.
//!
//! The `AggregateOracle` computes the "ground truth" for `SUM`, `COUNT(*)`,
//! and `AVG` aggregations using Apache DataFusion's in-memory SQL engine.
//! Property tests compare `AggregateMergeOp` incremental results against
//! the oracle to prove that our aggregate operator is correct.
//!
//! # DBSP soundness assertion for aggregate operators
//!
//! ```text
//! aggregate(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ) == SQL_aggregate(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ)
//! ```
//!
//! The oracle computes the right side via DataFusion; the `AggregateMergeOp`
//! computes the left side incrementally.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use rockstream_types::batch::ZSet;
use rockstream_types::laws::sum_count::decode_sum_count;

/// Schema helper for `{group_id: Int64, val: Int64}` rows.
///
/// - `key`   = 8-byte big-endian `group_id`
/// - `value` = 8-byte big-endian `val`
pub struct AggSchema;

impl AggSchema {
    /// Arrow schema for `{group_id: Int64, val: Int64}`.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("group_id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    /// Encode `(group_id, val)` to `(key_bytes, value_bytes)`.
    pub fn encode(group_id: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        (group_id.to_be_bytes().to_vec(), val.to_be_bytes().to_vec())
    }

    /// Decode `(key_bytes, value_bytes)` to `(group_id, val)`.
    pub fn decode(key: &[u8], value: &[u8]) -> (i64, i64) {
        let group_id = if key.len() >= 8 {
            i64::from_be_bytes(key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        let val = if value.len() >= 8 {
            i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        (group_id, val)
    }

    /// Convert a `ZSet` to an Arrow `RecordBatch` (expanding multiplicities).
    ///
    /// Rows with `weight > 0` appear `weight` times.
    /// Rows with `weight <= 0` are omitted (deletions in the accumulated state).
    pub fn zset_to_batch(zset: &ZSet) -> RecordBatch {
        let mut group_ids = Vec::new();
        let mut vals = Vec::new();
        for row in zset.iter() {
            let (group_id, val) = Self::decode(&row.key, &row.value);
            for _ in 0..row.weight.max(0) {
                group_ids.push(group_id);
                vals.push(val);
            }
        }
        let schema = Self::schema();
        let gid_arr: ArrayRef = Arc::new(Int64Array::from(group_ids));
        let val_arr: ArrayRef = Arc::new(Int64Array::from(vals));
        RecordBatch::try_new(schema, vec![gid_arr, val_arr])
            .expect("aggregate oracle batch construction")
    }
}

/// DataFusion-based aggregate oracle.
pub struct AggregateOracle {
    ctx: SessionContext,
}

impl AggregateOracle {
    /// Create a new oracle backed by a DataFusion `SessionContext`.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Compute `SELECT group_id, SUM(val), COUNT(*) FROM t GROUP BY group_id`
    /// over the accumulated state and return a map of `group_id → (sum, count)`.
    ///
    /// Only valid for states where all weights are positive (DataFusion cannot
    /// represent negative-weight rows).
    pub async fn agg_batch(&self, state: &ZSet) -> HashMap<i64, (i64, i64)> {
        let batch = AggSchema::zset_to_batch(state);
        if batch.num_rows() == 0 {
            return HashMap::new();
        }
        let mem_table =
            datafusion::datasource::MemTable::try_new(AggSchema::schema(), vec![vec![batch]])
                .expect("AggregateOracle: MemTable creation");

        self.ctx
            .deregister_table("t")
            .expect("deregister t (if exists)");
        self.ctx
            .register_table("t", Arc::new(mem_table))
            .expect("AggregateOracle: register table");

        let df = self
            .ctx
            .sql("SELECT group_id, SUM(val) AS sum_val, COUNT(*) AS cnt FROM t GROUP BY group_id")
            .await
            .expect("AggregateOracle: SQL parse");

        let batches = df.collect().await.expect("AggregateOracle: collect");

        let mut result = HashMap::new();
        for batch in &batches {
            let group_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("group_id column must be Int64");
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum_val column must be Int64");
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cnt column must be Int64");
            for i in 0..batch.num_rows() {
                result.insert(group_ids.value(i), (sums.value(i), counts.value(i)));
            }
        }
        result
    }
}

impl Default for AggregateOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a Z-set of aggregate results (key=group_id, value=SumCount bytes)
/// to a `HashMap<group_id, (sum, count)>`.
///
/// Used to compare IVM output against the oracle.
pub fn agg_zset_to_map(zset: &ZSet) -> HashMap<i64, (i64, i64)> {
    let mut result = HashMap::new();
    for row in zset.iter() {
        if row.weight <= 0 {
            continue; // skip retracted entries
        }
        let group_id = if row.key.len() >= 8 {
            i64::from_be_bytes(row.key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        if let Ok((sum, count)) = decode_sum_count(&row.value) {
            result.insert(group_id, (sum, count));
        }
    }
    result
}

/// Reference aggregate: apply `SUM(val)` and `COUNT(*)` grouped by `group_id`
/// to a `ZSet` of `(group_id_key, val_value)` rows.
///
/// Computes the ground truth without going through DataFusion.
pub fn zset_aggregate(state: &ZSet) -> HashMap<i64, (i64, i64)> {
    let mut result: HashMap<i64, (i64, i64)> = HashMap::new();
    for row in state.iter() {
        let (group_id, val) = AggSchema::decode(&row.key, &row.value);
        let entry = result.entry(group_id).or_insert((0i64, 0i64));
        entry.0 += val * row.weight;
        entry.1 += row.weight;
    }
    // Remove groups with zero count (fully deleted).
    result.retain(|_, (sum, count)| *count != 0 || *sum != 0);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::ZSet;

    #[tokio::test]
    async fn oracle_agg_matches_reference() {
        let mut state = ZSet::new();
        let (k1, v1) = AggSchema::encode(1, 10);
        let (k2, v2) = AggSchema::encode(1, 20);
        let (k3, v3) = AggSchema::encode(2, 5);
        state.insert(k1, v1, 1);
        state.insert(k2, v2, 1);
        state.insert(k3, v3, 1);

        let oracle = AggregateOracle::new();
        let result = oracle.agg_batch(&state).await;

        assert_eq!(result.get(&1), Some(&(30, 2)));
        assert_eq!(result.get(&2), Some(&(5, 1)));
    }

    #[test]
    fn reference_agg_correctness() {
        let mut state = ZSet::new();
        let (k1, v1) = AggSchema::encode(1, 10);
        let (k2, v2) = AggSchema::encode(1, 20);
        let (k3, v3) = AggSchema::encode(2, 5);
        state.insert(k1, v1, 1);
        state.insert(k2, v2, 1);
        state.insert(k3, v3, 1);

        let result = zset_aggregate(&state);
        assert_eq!(result.get(&1), Some(&(30, 2)));
        assert_eq!(result.get(&2), Some(&(5, 1)));
    }

    #[test]
    fn reference_agg_handles_deletes() {
        let mut state = ZSet::new();
        let (k, v) = AggSchema::encode(1, 10);
        state.insert(k.clone(), v.clone(), 1);
        state.insert(k, v, -1);
        state.consolidate();

        let result = zset_aggregate(&state);
        assert!(result.is_empty() || result.get(&1) == Some(&(0, 0)));
    }
}

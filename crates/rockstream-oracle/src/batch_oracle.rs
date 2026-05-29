//! Batch oracle for filter, project, and map correctness verification.
//!
//! The `BatchOracle` computes the "ground truth" result of a query on an
//! accumulated state using Apache DataFusion's in-memory execution engine.
//! Property tests compare IVM incremental results against the oracle to prove
//! that our operators are correct.
//!
//! # DBSP soundness assertion for linear operators
//!
//! For any linear operator `f` (filter, project, map):
//! ```text
//! f(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ) == f(Δ₁) ⊕ f(Δ₂) ⊕ … ⊕ f(Δₙ)
//! ```
//! The oracle computes the left side (batch); the IVM operators compute the
//! right side (incremental). They must be equal.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use rockstream_types::batch::ZSet;

/// Codec for `{id: Int64, val: Int64}` rows used in the oracle.
///
/// Each `ZSetRow` has:
/// - `key`   = 8-byte big-endian `id`
/// - `value` = 8-byte big-endian `val`
pub struct Int64Schema;

impl Int64Schema {
    /// Arrow schema for `{id: Int64, val: Int64}`.
    pub fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]))
    }

    /// Encode `(id, val)` to `(key_bytes, value_bytes)`.
    pub fn encode(id: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), val.to_be_bytes().to_vec())
    }

    /// Decode `(key_bytes, value_bytes)` to `(id, val)`.
    pub fn decode(key: &[u8], value: &[u8]) -> (i64, i64) {
        let id = if key.len() >= 8 {
            i64::from_be_bytes(key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        let val = if value.len() >= 8 {
            i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        (id, val)
    }

    /// Convert a `ZSet` to an Arrow `RecordBatch` (one row per non-zero-weight
    /// entry, weight excluded — only the current state).
    ///
    /// Rows with weight > 0 appear weight times; rows with weight < 0 are
    /// omitted (they represent deletions in the accumulated state).
    pub fn zset_to_batch(zset: &ZSet) -> RecordBatch {
        let mut ids = Vec::new();
        let mut vals = Vec::new();
        for row in zset.iter() {
            let (id, val) = Self::decode(&row.key, &row.value);
            // Positive weight = the row exists in the current state.
            for _ in 0..row.weight.max(0) {
                ids.push(id);
                vals.push(val);
            }
        }
        let schema = Self::schema();
        let id_arr: ArrayRef = Arc::new(Int64Array::from(ids));
        let val_arr: ArrayRef = Arc::new(Int64Array::from(vals));
        RecordBatch::try_new(schema, vec![id_arr, val_arr]).expect("oracle batch construction")
    }

    /// Convert an Arrow `RecordBatch` back to a `ZSet` (each row gets weight
    /// +1).
    pub fn batch_to_zset(batch: &RecordBatch) -> ZSet {
        let mut zset = ZSet::new();
        if batch.num_rows() == 0 {
            return zset;
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column must be Int64");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val column must be Int64");
        for i in 0..batch.num_rows() {
            let (key, value) = Self::encode(ids.value(i), vals.value(i));
            zset.insert(key, value, 1);
        }
        zset
    }
}

/// Batch oracle that computes filter/project/map results using DataFusion.
///
/// The oracle is used in property tests to verify IVM operator correctness.
pub struct BatchOracle {
    ctx: SessionContext,
}

impl BatchOracle {
    /// Create a new oracle backed by a fresh DataFusion `SessionContext`.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Compute the filter result on an accumulated `ZSet` using a SQL WHERE
    /// clause expression.
    ///
    /// The `ZSet` is converted to an in-memory table `t` with columns
    /// `{id: Int64, val: Int64}`. The predicate is evaluated as
    /// `SELECT id, val FROM t WHERE <predicate>`.
    ///
    /// Returns the result as a `ZSet` (each output row has weight +1).
    pub async fn filter_batch(&self, state: &ZSet, predicate: &str) -> ZSet {
        let batch = Int64Schema::zset_to_batch(state);
        if batch.num_rows() == 0 {
            return ZSet::new();
        }
        self.ctx.deregister_table("t").unwrap_or_default();
        self.ctx
            .register_batch("t", batch)
            .expect("oracle: register_batch failed");
        let sql = format!("SELECT id, val FROM t WHERE {predicate}");
        let df = self.ctx.sql(&sql).await.expect("oracle: SQL parse failed");
        let batches = df.collect().await.expect("oracle: collect failed");
        let mut result = ZSet::new();
        for b in &batches {
            let part = Int64Schema::batch_to_zset(b);
            result.merge(&part);
        }
        result
    }

    /// Compute the projection result on an accumulated `ZSet` using a SQL
    /// SELECT expression list.
    ///
    /// Example: `select_expr = "id, val * 2 AS val"`.
    pub async fn project_batch(&self, state: &ZSet, select_expr: &str) -> ZSet {
        let batch = Int64Schema::zset_to_batch(state);
        if batch.num_rows() == 0 {
            return ZSet::new();
        }
        self.ctx.deregister_table("t").unwrap_or_default();
        self.ctx
            .register_batch("t", batch)
            .expect("oracle: register_batch failed");
        let sql = format!("SELECT {select_expr} FROM t");
        let df = self.ctx.sql(&sql).await.expect("oracle: SQL parse failed");
        let batches = df.collect().await.expect("oracle: collect failed");
        let mut result = ZSet::new();
        for b in &batches {
            let part = Int64Schema::batch_to_zset(b);
            result.merge(&part);
        }
        result
    }
}

impl Default for BatchOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply a filter predicate `f: (id, val) -> bool` to a `ZSet`, returning
/// the sub-`ZSet` of rows that satisfy the predicate. Weights are preserved.
///
/// This is the reference implementation used to build IVM filter operators
/// for property tests.
pub fn zset_filter<F>(zset: &ZSet, predicate: F) -> ZSet
where
    F: Fn(i64, i64) -> bool,
{
    let mut result = ZSet::new();
    for row in zset.iter() {
        let (id, val) = Int64Schema::decode(&row.key, &row.value);
        if predicate(id, val) {
            result.insert(row.key.clone(), row.value.clone(), row.weight);
        }
    }
    result
}

/// Apply a projection `f: (id, val) -> (new_id, new_val)` to a `ZSet`.
pub fn zset_project<F>(zset: &ZSet, project: F) -> ZSet
where
    F: Fn(i64, i64) -> (i64, i64),
{
    let mut result = ZSet::new();
    for row in zset.iter() {
        let (id, val) = Int64Schema::decode(&row.key, &row.value);
        let (new_id, new_val) = project(id, val);
        let (new_key, new_value) = Int64Schema::encode(new_id, new_val);
        result.insert(new_key, new_value, row.weight);
    }
    result
}

/// Apply a map function `f: (id, val) -> (new_id, new_val)` to a `ZSet`.
///
/// Unlike `zset_project`, map is expected to be a bijection: distinct input
/// rows produce distinct output rows with the same weights.
pub fn zset_map<F>(zset: &ZSet, map_fn: F) -> ZSet
where
    F: Fn(i64, i64) -> (i64, i64),
{
    zset_project(zset, map_fn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int64_schema_encode_decode_roundtrip() {
        let (k, v) = Int64Schema::encode(42, 99);
        let (id, val) = Int64Schema::decode(&k, &v);
        assert_eq!(id, 42);
        assert_eq!(val, 99);
    }

    #[test]
    fn zset_filter_keeps_matching_rows() {
        let mut zset = ZSet::new();
        let (k1, v1) = Int64Schema::encode(1, 10);
        let (k2, v2) = Int64Schema::encode(2, 3);
        let (k3, v3) = Int64Schema::encode(3, 7);
        zset.insert(k1, v1, 1);
        zset.insert(k2, v2, 1);
        zset.insert(k3, v3, 2);

        let filtered = zset_filter(&zset, |_, val| val > 5);
        assert_eq!(filtered.len(), 2); // id=1 (val=10) and id=3 (val=7)
    }

    #[test]
    fn zset_project_transforms_rows() {
        let mut zset = ZSet::new();
        let (k, v) = Int64Schema::encode(1, 5);
        zset.insert(k, v, 3);

        let projected = zset_project(&zset, |id, val| (id, val * 2));
        let row = projected.iter().next().unwrap();
        let (_, new_val) = Int64Schema::decode(&row.key, &row.value);
        assert_eq!(new_val, 10);
        assert_eq!(row.weight, 3);
    }

    #[tokio::test]
    async fn oracle_filter_matches_manual_filter() {
        let oracle = BatchOracle::new();

        let mut state = ZSet::new();
        for i in 0i64..10 {
            let (k, v) = Int64Schema::encode(i, i * 2);
            state.insert(k, v, 1);
        }

        // DataFusion batch oracle
        let oracle_result = oracle.filter_batch(&state, "val > 10").await;

        // Manual reference filter
        let manual_result = zset_filter(&state, |_, val| val > 10);

        assert_eq!(oracle_result, manual_result);
    }
}

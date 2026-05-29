//! DataFusion-based MIN/MAX oracle for correctness verification.
//!
//! The `MinMaxOracle` computes the "ground truth" for `MIN` and `MAX`
//! aggregations using Apache DataFusion's in-memory SQL engine.
//! Property tests compare `MinMaxOp` incremental results against the oracle
//! to prove correctness of retraction-aware extremum computation.
//!
//! # DBSP soundness assertion for MIN/MAX operators
//!
//! ```text
//! min_max_op(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ) == SQL_min_max(Δ₁ ⊕ Δ₂ ⊕ … ⊕ Δₙ)
//! ```
//!
//! The oracle uses DataFusion for positive-weight states and
//! `zset_min_max` for the reference implementation over arbitrary Z-sets
//! (including retractions).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::prelude::*;
use rockstream_types::batch::ZSet;

use crate::aggregate_oracle::AggSchema;

/// Which aggregate kind to compute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleAggKind {
    /// Compute `MIN(val)` per group.
    Min,
    /// Compute `MAX(val)` per group.
    Max,
}

/// DataFusion-backed MIN/MAX oracle.
pub struct MinMaxOracle {
    ctx: SessionContext,
}

impl MinMaxOracle {
    /// Create a new oracle backed by a DataFusion `SessionContext`.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Compute `SELECT group_id, MIN/MAX(val) FROM t GROUP BY group_id` over
    /// the accumulated state and return `group_id → extremum`.
    ///
    /// Only valid for states where all weights are positive (DataFusion cannot
    /// represent negative-weight rows).
    pub async fn min_max_batch(&self, state: &ZSet, kind: OracleAggKind) -> HashMap<i64, i64> {
        let batch = AggSchema::zset_to_batch(state);
        if batch.num_rows() == 0 {
            return HashMap::new();
        }
        let mem_table =
            datafusion::datasource::MemTable::try_new(AggSchema::schema(), vec![vec![batch]])
                .expect("MinMaxOracle: MemTable creation");

        self.ctx
            .deregister_table("t")
            .expect("deregister t (if exists)");
        self.ctx
            .register_table("t", Arc::new(mem_table))
            .expect("MinMaxOracle: register table");

        let sql = match kind {
            OracleAggKind::Max => "SELECT group_id, MAX(val) AS extremum FROM t GROUP BY group_id",
            OracleAggKind::Min => "SELECT group_id, MIN(val) AS extremum FROM t GROUP BY group_id",
        };

        let df = self.ctx.sql(sql).await.expect("MinMaxOracle: SQL parse");

        let batches = df.collect().await.expect("MinMaxOracle: collect");

        let mut result = HashMap::new();
        for batch in &batches {
            let group_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("group_id column must be Int64");
            let extrema = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("extremum column must be Int64");
            for i in 0..batch.num_rows() {
                result.insert(group_ids.value(i), extrema.value(i));
            }
        }
        result
    }
}

impl Default for MinMaxOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Reference MIN/MAX implementation over an arbitrary Z-set (including
/// negative weights / retractions).
///
/// Uses the weighted multiset interpretation:
/// - Only rows with `weight > 0` are "present".
/// - Extremum is derived from those present rows.
///
/// Returns `group_id → Some(extremum)` for groups with at least one
/// positive-weight row, or `None` for empty groups.
pub fn zset_min_max(state: &ZSet, kind: OracleAggKind) -> HashMap<i64, Option<i64>> {
    // Accumulate per-group multisets.
    let mut multisets: HashMap<i64, HashMap<i64, i64>> = HashMap::new();

    for row in state.iter() {
        let group_id = if row.key.len() >= 8 {
            i64::from_be_bytes(row.key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        let val = if row.value.len() >= 8 {
            i64::from_be_bytes(row.value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        *multisets
            .entry(group_id)
            .or_default()
            .entry(val)
            .or_insert(0) += row.weight;
    }

    // Derive extremum from positive-weight entries.
    let mut result = HashMap::new();
    for (group_id, multiset) in &multisets {
        let extremum = match kind {
            OracleAggKind::Max => multiset
                .iter()
                .filter(|(_, w)| **w > 0)
                .map(|(v, _)| *v)
                .max(),
            OracleAggKind::Min => multiset
                .iter()
                .filter(|(_, w)| **w > 0)
                .map(|(v, _)| *v)
                .min(),
        };
        result.insert(*group_id, extremum);
    }
    result
}

/// Convert a `ZSet` of `MinMaxOp` output (key=group_id, value=i64 extremum)
/// to a `HashMap<group_id, i64>`.
///
/// The output Z-set from `MinMaxOp` uses `weight = 1` for current extrema and
/// `weight = -1` for retractions. Only positive-weight entries are the "live"
/// current extrema.
pub fn min_max_zset_to_map(zset: &ZSet) -> HashMap<i64, i64> {
    let mut result = HashMap::new();
    for row in zset.iter() {
        if row.weight <= 0 {
            continue;
        }
        let group_id = if row.key.len() >= 8 {
            i64::from_be_bytes(row.key[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        let val = if row.value.len() >= 8 {
            i64::from_be_bytes(row.value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        result.insert(group_id, val);
    }
    result
}

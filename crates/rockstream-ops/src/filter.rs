//! Filter operator for RockStream IVM.
//!
//! Applies a predicate to each row in a `ZSetBatch`, keeping rows whose
//! (key, value) bytes satisfy the predicate. Weights are preserved unchanged.
//!
//! Filter is a **linear** (stateless) operator in the IVM sense: it applies
//! independently to each (key, value, weight) triple. The output delta can be
//! directly accumulated downstream without any per-epoch state.
//!
//! # DataFusion expression evaluation
//!
//! `FilterOperator::with_datafusion_expr` accepts a DataFusion `PhysicalExpr`
//! plus a `RowCodec` that bridges between raw `ZSetRow` bytes and Arrow
//! `RecordBatch`. The predicate is evaluated via DataFusion's vectorised
//! expression engine on the full batch, rather than row-by-row.

use std::sync::Arc;

use rockstream_types::batch::{ZSetBatch, ZSetRow};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;
use crate::row_codec::RowCodec;

/// Predicate function type for row-level filtering.
pub type FilterFn = Arc<dyn Fn(&[u8], &[u8]) -> bool + Send + Sync + 'static>;

/// IVM filter operator: keeps rows satisfying `predicate`.
pub struct FilterOperator {
    predicate: FilterFn,
    name: String,
}

impl FilterOperator {
    /// Create a `FilterOperator` from a plain Rust closure.
    ///
    /// The closure receives `(key: &[u8], value: &[u8])` and returns `true` to
    /// keep the row, `false` to discard it.
    pub fn new(name: impl Into<String>, predicate: FilterFn) -> Self {
        Self {
            predicate,
            name: name.into(),
        }
    }

    /// Create a `FilterOperator` backed by a DataFusion `PhysicalExpr`.
    ///
    /// The expression is evaluated on Arrow `RecordBatch`es produced by
    /// `codec`. This is the primary entry-point for DataFusion expression
    /// evaluation at the IVM operator level.
    ///
    /// # Panics
    /// Panics at predicate evaluation time if `codec.encode_batch` or the
    /// DataFusion expression returns an error (operator-level errors are fatal
    /// and indicate a plan/schema mismatch).
    pub fn with_datafusion_expr(
        name: impl Into<String>,
        expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        codec: Arc<dyn RowCodec>,
    ) -> Self {
        let codec_clone = codec.clone();
        let predicate: FilterFn = Arc::new(move |key: &[u8], value: &[u8]| {
            // Build a single-row batch and evaluate the expression.
            let row = ZSetRow {
                key: key.to_vec(),
                value: value.to_vec(),
                weight: 1,
            };
            let batch = codec_clone.encode_batch(&[row]);
            let result = expr
                .evaluate(&batch)
                .expect("DataFusion expression evaluation failed");
            let array = result
                .into_array(1)
                .expect("DataFusion expression returned empty array");
            let bools = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .expect("filter expression must return Boolean");
            bools.value(0)
        });
        Self {
            predicate,
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl Operator for FilterOperator {
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
        let mut out = rockstream_types::batch::ZSet::new();
        for row in input.zset.iter() {
            if (self.predicate)(&row.key, &row.value) {
                out.insert(row.key.clone(), row.value.clone(), row.weight);
            }
        }
        ZSetBatch {
            zset: out,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

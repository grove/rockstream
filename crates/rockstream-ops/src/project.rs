//! Project operator for RockStream IVM.
//!
//! Applies a projection to each row in a `ZSetBatch`, transforming
//! (key, value) bytes according to the provided mapping function.
//!
//! Project is a **linear** (stateless) operator: it applies independently
//! to each row. If two input rows project to the same output row, their
//! weights are additive (the Z-set insert semantics handle this naturally).
//!
//! # DataFusion expression evaluation
//!
//! `ProjectOperator::with_datafusion_exprs` accepts a list of DataFusion
//! `PhysicalExpr` column expressions plus a `RowCodec`. Projection is
//! evaluated via DataFusion's vectorised expression engine.

use std::sync::Arc;

use rockstream_types::batch::{ZSetBatch, ZSetRow};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;
use crate::row_codec::RowCodec;

/// Projection function type: maps `(key, value)` bytes to an optional output
/// `(new_key, new_value)`. Returning `None` drops the row (equivalent to a
/// combined filter+project).
pub type ProjectFn =
    Arc<dyn Fn(&[u8], &[u8]) -> Option<(Vec<u8>, Vec<u8>)> + Send + Sync + 'static>;

/// IVM project operator: transforms each row's (key, value) bytes.
pub struct ProjectOperator {
    project: ProjectFn,
    name: String,
}

impl ProjectOperator {
    /// Create a `ProjectOperator` from a plain Rust closure.
    pub fn new(name: impl Into<String>, project: ProjectFn) -> Self {
        Self {
            project,
            name: name.into(),
        }
    }

    /// Create a `ProjectOperator` backed by DataFusion `PhysicalExpr` column
    /// expressions evaluated on Arrow `RecordBatch`es.
    ///
    /// `exprs` is an ordered list of expressions defining the output columns.
    /// `output_codec` encodes the resulting Arrow row back to `(key, value)`.
    pub fn with_datafusion_exprs(
        name: impl Into<String>,
        exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        input_codec: Arc<dyn RowCodec>,
        output_codec: Arc<dyn RowCodec>,
    ) -> Self {
        let project: ProjectFn = Arc::new(move |key: &[u8], value: &[u8]| {
            let row = ZSetRow {
                key: key.to_vec(),
                value: value.to_vec(),
                weight: 1,
            };
            let batch = input_codec.encode_batch(&[row]);

            // Evaluate each expression to produce output columns.
            let result_columns: Vec<_> = exprs
                .iter()
                .map(|expr| {
                    expr.evaluate(&batch)
                        .expect("DataFusion projection expression failed")
                        .into_array(1)
                        .expect("empty array from projection")
                })
                .collect();

            // Build output RecordBatch from evaluated columns.
            let out_schema = output_codec.schema();
            let out_batch = arrow::record_batch::RecordBatch::try_new(out_schema, result_columns)
                .expect("projection output RecordBatch construction failed");

            output_codec.decode_batch_single(&out_batch)
        });

        Self {
            project,
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl Operator for ProjectOperator {
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
            if let Some((new_key, new_value)) = (self.project)(&row.key, &row.value) {
                out.insert(new_key, new_value, row.weight);
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

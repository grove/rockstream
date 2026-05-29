//! Map operator for RockStream IVM.
//!
//! Applies an element-wise transformation to each row in a `ZSetBatch`.
//! Map is a **linear** (stateless) operator: it applies independently to
//! each row, preserving weights. Unlike `ProjectOperator`, the map function
//! must return exactly one output row per input row (no filtering, no
//! merging of duplicate keys).
//!
//! # DataFusion expression evaluation
//!
//! `MapOperator::with_datafusion_expr` accepts a DataFusion `PhysicalExpr`
//! that maps each row to a new row via Arrow `RecordBatch` evaluation.

use std::sync::Arc;

use rockstream_types::batch::{ZSetBatch, ZSetRow};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;
use crate::row_codec::RowCodec;

/// Map function type: transforms `(key, value)` bytes to a new `(key, value)`.
pub type MapFn = Arc<dyn Fn(&[u8], &[u8]) -> (Vec<u8>, Vec<u8>) + Send + Sync + 'static>;

/// IVM map operator: transforms each row's (key, value) bytes.
pub struct MapOperator {
    map_fn: MapFn,
    name: String,
}

impl MapOperator {
    /// Create a `MapOperator` from a plain Rust closure.
    pub fn new(name: impl Into<String>, map_fn: MapFn) -> Self {
        Self {
            map_fn,
            name: name.into(),
        }
    }

    /// Create a `MapOperator` backed by a DataFusion `PhysicalExpr` applied
    /// to Arrow `RecordBatch`es produced and consumed by `codec`.
    ///
    /// The expression must return a single column whose value, combined with
    /// the original key, forms the output row.
    pub fn with_datafusion_expr(
        name: impl Into<String>,
        expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        input_codec: Arc<dyn RowCodec>,
        output_codec: Arc<dyn RowCodec>,
    ) -> Self {
        let map_fn: MapFn = Arc::new(move |key: &[u8], value: &[u8]| {
            let row = ZSetRow {
                key: key.to_vec(),
                value: value.to_vec(),
                weight: 1,
            };
            let batch = input_codec.encode_batch(&[row]);
            let result = expr
                .evaluate(&batch)
                .expect("DataFusion map expression evaluation failed");
            let array = result
                .into_array(1)
                .expect("empty array from map expression");
            let out_schema = output_codec.schema();
            let out_batch = arrow::record_batch::RecordBatch::try_new(out_schema, vec![array])
                .expect("map output RecordBatch construction failed");
            output_codec
                .decode_batch_single(&out_batch)
                .unwrap_or_else(|| (key.to_vec(), value.to_vec()))
        });

        Self {
            map_fn,
            name: name.into(),
        }
    }
}

#[async_trait::async_trait]
impl Operator for MapOperator {
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
            let (new_key, new_value) = (self.map_fn)(&row.key, &row.value);
            out.insert(new_key, new_value, row.weight);
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

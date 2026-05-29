//! Operator trait definition.
//!
//! All IVM operators implement this trait. The trait works with `ZSetBatch`
//! (the delta unit) and carries a merge-law annotation for `EXPLAIN`.

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSetBatch};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

/// Trait that all operators must implement.
#[async_trait]
pub trait Operator: Send {
    /// Process an input batch and produce an output batch.
    async fn process(&mut self, input: &SourceBatch) -> SinkBatch;

    /// Process a Z-set delta and produce an output delta.
    ///
    /// This is the primary IVM interface. Operators receive incremental
    /// changes and produce incremental changes.
    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        // Default: pass through (override in real operators)
        input.clone()
    }

    /// Called when an epoch is complete.
    async fn epoch_complete(&mut self, epoch: Epoch);

    /// Name of this operator for diagnostics.
    fn name(&self) -> &str;

    /// The merge law this operator uses (if any).
    /// Used for `EXPLAIN INCREMENTAL` annotations.
    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

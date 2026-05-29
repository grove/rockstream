//! Operator trait definition.

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch};
use rockstream_types::timestamp::Epoch;

/// Trait that all operators must implement.
#[async_trait]
pub trait Operator: Send {
    /// Process an input batch and produce an output batch.
    async fn process(&mut self, input: &SourceBatch) -> SinkBatch;

    /// Called when an epoch is complete.
    async fn epoch_complete(&mut self, epoch: Epoch);

    /// Name of this operator for diagnostics.
    fn name(&self) -> &str;
}

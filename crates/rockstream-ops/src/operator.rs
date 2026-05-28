//! Operator trait definition.

use rockstream_connectors::sink::SinkBatch;
use rockstream_connectors::source::SourceBatch;
use rockstream_types::timestamp::Epoch;

/// Trait that all operators must implement.
pub trait Operator: Send {
    /// Process an input batch and produce an output batch.
    fn process(&mut self, input: &SourceBatch) -> SinkBatch;

    /// Called when an epoch is complete.
    fn epoch_complete(&mut self, epoch: Epoch);

    /// Name of this operator for diagnostics.
    fn name(&self) -> &str;
}

//! Sink trait for RockStream connectors.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

// Re-export SinkBatch from the canonical location in rockstream-types.
pub use rockstream_types::batch::SinkBatch;

/// Trait that all sinks must implement.
#[async_trait]
pub trait Sink: Send {
    /// Write a batch of records.
    async fn write_batch(&mut self, batch: &SinkBatch);

    /// Commit the current epoch (flush).
    async fn commit(&mut self, epoch: Epoch);

    /// Name of this sink for diagnostics.
    fn name(&self) -> &str;
}

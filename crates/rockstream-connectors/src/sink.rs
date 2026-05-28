//! Sink trait for RockStream connectors.

use rockstream_types::timestamp::Epoch;

/// A batch of records to be written by a sink.
#[derive(Debug, Clone, Default)]
pub struct SinkBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

/// Trait that all sinks must implement.
pub trait Sink: Send {
    /// Write a batch of records.
    fn write_batch(&mut self, batch: &SinkBatch);

    /// Commit the current epoch (flush).
    fn commit(&mut self, epoch: Epoch);

    /// Name of this sink for diagnostics.
    fn name(&self) -> &str;
}

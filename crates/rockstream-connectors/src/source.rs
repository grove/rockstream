//! Source trait for RockStream connectors.

use rockstream_types::timestamp::Epoch;

/// A batch of records produced by a source in one epoch.
#[derive(Debug, Clone, Default)]
pub struct SourceBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

/// Trait that all sources must implement.
pub trait Source: Send {
    /// Poll for the next batch of records. Returns `None` when the source is exhausted.
    fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch>;

    /// Name of this source for diagnostics.
    fn name(&self) -> &str;
}

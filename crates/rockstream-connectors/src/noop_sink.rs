//! No-op view sink connector.
//!
//! Consumes and discards all batches. Used in the no-op pipeline to prove
//! the system starts, runs, and shuts down cleanly.

use crate::sink::{Sink, SinkBatch};
use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

/// A sink that discards all records.
pub struct NoopSink {
    /// Count of batches consumed (for diagnostics).
    batches_written: u64,
    /// Count of epochs committed.
    epochs_committed: u64,
}

impl NoopSink {
    /// Create a new no-op sink.
    pub fn new() -> Self {
        Self {
            batches_written: 0,
            epochs_committed: 0,
        }
    }

    /// Number of batches consumed.
    pub fn batches_written(&self) -> u64 {
        self.batches_written
    }

    /// Number of epochs committed.
    pub fn epochs_committed(&self) -> u64 {
        self.epochs_committed
    }
}

impl Default for NoopSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for NoopSink {
    async fn write_batch(&mut self, batch: &SinkBatch) {
        self.batches_written += 1;
        tracing::trace!(
            epoch = batch.epoch,
            records = batch.record_count,
            "noop sink discarding batch"
        );
    }

    async fn commit(&mut self, epoch: Epoch) {
        self.epochs_committed += 1;
        tracing::trace!(epoch, "noop sink committed epoch");
    }

    fn name(&self) -> &str {
        "noop-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_sink_accepts_batches() {
        let mut sink = NoopSink::new();
        sink.write_batch(&SinkBatch {
            record_count: 0,
            epoch: 0,
        })
        .await;
        sink.write_batch(&SinkBatch {
            record_count: 0,
            epoch: 1,
        })
        .await;
        assert_eq!(sink.batches_written(), 2);
    }

    #[tokio::test]
    async fn noop_sink_commits() {
        let mut sink = NoopSink::new();
        sink.commit(0).await;
        sink.commit(1).await;
        assert_eq!(sink.epochs_committed(), 2);
    }

    #[test]
    fn noop_sink_name() {
        let sink = NoopSink::new();
        assert_eq!(sink.name(), "noop-sink");
    }

    #[test]
    fn noop_sink_default() {
        let sink = NoopSink::default();
        assert_eq!(sink.batches_written(), 0);
        assert_eq!(sink.epochs_committed(), 0);
    }
}

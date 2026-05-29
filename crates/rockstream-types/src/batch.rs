//! Batch types shared across connectors and operators.
//!
//! These live in `rockstream-types` so that both the connector layer and the
//! operator layer can depend on them without creating a circular dependency.

use crate::timestamp::Epoch;
use serde::{Deserialize, Serialize};

/// A batch of records produced by a source in one epoch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

/// A batch of records to be written by a sink.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkBatch {
    /// Number of records in this batch.
    pub record_count: usize,
    /// Epoch this batch belongs to.
    pub epoch: Epoch,
}

/// A weight value for Z-set rows (IVM delta encoding).
pub type Weight = i64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_batch_default() {
        let b = SourceBatch::default();
        assert_eq!(b.record_count, 0);
        assert_eq!(b.epoch, 0);
    }

    #[test]
    fn sink_batch_default() {
        let b = SinkBatch::default();
        assert_eq!(b.record_count, 0);
        assert_eq!(b.epoch, 0);
    }
}

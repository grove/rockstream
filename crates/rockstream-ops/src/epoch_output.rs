//! EpochOutput: the result produced by one operator for one epoch (IVM.md §8.1).
//!
//! An `EpochOutput` carries the delta batch produced by an operator after
//! processing all input for a given epoch, plus the operator ID and epoch
//! number so the shard-level epoch commit coordinator can coalesce all ready
//! fragments into a single atomic `WriteBatch`.

use rockstream_types::batch::ZSetBatch;
use rockstream_types::ids::OperatorId;
use rockstream_types::timestamp::Epoch;

/// The output fragment produced by one operator instance for one epoch.
///
/// Multiple `EpochOutput` fragments from different operators in the same shard
/// are coalesced by the epoch commit coordinator into a single atomic
/// `WriteBatch` before being durably committed.
#[derive(Debug, Clone)]
pub struct EpochOutput {
    /// The operator that produced this fragment.
    pub operator_id: OperatorId,
    /// The epoch this fragment belongs to.
    pub epoch: Epoch,
    /// The incremental delta produced by the operator.
    pub delta: ZSetBatch,
    /// Whether this is the final fragment for this operator in this epoch.
    /// An operator may emit multiple fragments per epoch (e.g., after a large
    /// input batch is processed in chunks), but must emit exactly one with
    /// `is_final = true` to signal epoch completion.
    pub is_final: bool,
}

impl EpochOutput {
    /// Create a new epoch output fragment.
    pub fn new(operator_id: OperatorId, epoch: Epoch, delta: ZSetBatch, is_final: bool) -> Self {
        Self {
            operator_id,
            epoch,
            delta,
            is_final,
        }
    }

    /// Create a final (single-fragment) epoch output.
    pub fn final_output(operator_id: OperatorId, epoch: Epoch, delta: ZSetBatch) -> Self {
        Self::new(operator_id, epoch, delta, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::batch::{ZSet, ZSetBatch};
    use rockstream_types::ids::OperatorId;

    #[test]
    fn epoch_output_final_flag() {
        let batch = ZSetBatch {
            zset: ZSet::new(),
            epoch: 1,
        };
        let out = EpochOutput::final_output(OperatorId(0), 1, batch);
        assert!(out.is_final);
        assert_eq!(out.epoch, 1);
    }

    #[test]
    fn epoch_output_non_final() {
        let batch = ZSetBatch {
            zset: ZSet::new(),
            epoch: 2,
        };
        let out = EpochOutput::new(OperatorId(1), 2, batch, false);
        assert!(!out.is_final);
    }
}

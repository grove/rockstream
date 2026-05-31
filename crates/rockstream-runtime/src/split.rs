//! Shard split and merge state machines (v0.37).
//!
//! Implements online shard split, cold shard merge, and the post-cutover
//! TombstoneGc cleanup that reclaims donor-side state after ownership transfer.
//!
//! # Split lifecycle
//!
//! ```text
//! Idle
//!   → Checkpointing { checkpoint_id }           (barrier injected)
//!   → Copying { checkpoint_id, rows_copied }     (new shard ingesting range)
//!   → AwaitingCutover { rows_copied }            (copy done; wait for epoch boundary)
//!   → Cleanup { cutover_epoch, rows_to_cleanup } (shard map bumped; donor retiring keys)
//!   → Done { cutover_epoch, rows_migrated }      (split complete)
//! ```
//!
//! # Merge lifecycle
//!
//! ```text
//! Idle
//!   → Absorbing { absorbed_shard, rows_absorbed } (target absorbing source's range)
//!   → AwaitingCutover { rows_absorbed }           (absorption done; wait for epoch boundary)
//!   → Done { cutover_epoch }                      (merge complete)
//! ```

use rockstream_types::{ids::ShardId, timestamp::Epoch};

use crate::checkpoint::CheckpointId;

// ─── Split State Machine ──────────────────────────────────────────────────────

/// Phase of a shard split operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitPhase {
    /// No split in progress.
    Idle,
    /// Checkpoint barrier injected; waiting for the donor shard to create a
    /// consistent snapshot at `checkpoint_id`.
    Checkpointing {
        /// The checkpoint that will anchor the split.
        checkpoint_id: CheckpointId,
    },
    /// Checkpoint taken; new shard is ingesting the key range from the donor's
    /// snapshot via `DbReader`.
    Copying {
        /// The checkpoint that anchors the split.
        checkpoint_id: CheckpointId,
        /// Rows copied to the new shard so far.
        rows_copied: u64,
    },
    /// Copy complete; waiting for the next epoch boundary for the atomic shard-map
    /// version bump and cutover.
    AwaitingCutover {
        /// Rows that were copied to the new shard.
        rows_copied: u64,
    },
    /// Shard map version bumped; donor is scanning and deleting migrated keys
    /// (scan-and-delete — no range delete, per storage contract).
    Cleanup {
        /// The epoch at which cutover occurred.
        cutover_epoch: Epoch,
        /// Rows remaining to be cleaned up on the donor.
        rows_to_cleanup: u64,
    },
    /// Split complete. All state has been migrated and donor has been cleaned up.
    Done {
        /// The epoch at which the cutover occurred.
        cutover_epoch: Epoch,
        /// Total rows migrated to the new shard.
        rows_migrated: u64,
    },
}

/// An in-progress shard split operation.
///
/// Tracks the full lifecycle from checkpoint injection through donor cleanup.
/// The `split_key_hash` is the midpoint of the donor's hash range: keys with
/// `hash(key) < split_key_hash` remain on the donor; keys with
/// `hash(key) >= split_key_hash` move to the new shard.
#[derive(Debug, Clone)]
pub struct ShardSplitOp {
    /// Shard being split (donor).
    pub donor_id: ShardId,
    /// New shard receiving the upper half of the key range.
    pub new_shard_id: ShardId,
    /// Hash boundary: keys with `key_hash >= split_key_hash` go to `new_shard_id`.
    pub split_key_hash: u64,
    /// Current phase of the split.
    pub phase: SplitPhase,
}

impl ShardSplitOp {
    /// Create a new split operation in the `Idle` phase.
    pub fn new(donor_id: ShardId, new_shard_id: ShardId, split_key_hash: u64) -> Self {
        Self {
            donor_id,
            new_shard_id,
            split_key_hash,
            phase: SplitPhase::Idle,
        }
    }

    /// Advance to `Checkpointing` after the control plane injects a barrier.
    pub fn begin_checkpoint(&mut self, checkpoint_id: CheckpointId) {
        assert!(
            matches!(self.phase, SplitPhase::Idle),
            "begin_checkpoint called in wrong phase: {:?}",
            self.phase
        );
        self.phase = SplitPhase::Checkpointing { checkpoint_id };
    }

    /// Advance to `Copying` once the donor's checkpoint snapshot is ready.
    pub fn checkpoint_ready(&mut self) {
        if let SplitPhase::Checkpointing { checkpoint_id } = self.phase {
            self.phase = SplitPhase::Copying {
                checkpoint_id,
                rows_copied: 0,
            };
        } else {
            panic!("checkpoint_ready called in wrong phase: {:?}", self.phase);
        }
    }

    /// Record progress as rows are copied to the new shard.
    pub fn record_rows_copied(&mut self, count: u64) {
        if let SplitPhase::Copying { rows_copied, .. } = &mut self.phase {
            *rows_copied += count;
        }
    }

    /// Advance to `AwaitingCutover` once all rows in the key range have been copied.
    pub fn copy_complete(&mut self) {
        if let SplitPhase::Copying { rows_copied, .. } = self.phase {
            self.phase = SplitPhase::AwaitingCutover { rows_copied };
        } else {
            panic!("copy_complete called in wrong phase: {:?}", self.phase);
        }
    }

    /// Advance to `Cleanup` at the epoch boundary when the shard-map version is bumped.
    ///
    /// After this point the new shard is authoritative for its key range.
    pub fn cutover(&mut self, cutover_epoch: Epoch) {
        if let SplitPhase::AwaitingCutover { rows_copied } = self.phase {
            self.phase = SplitPhase::Cleanup {
                cutover_epoch,
                rows_to_cleanup: rows_copied,
            };
        } else {
            panic!("cutover called in wrong phase: {:?}", self.phase);
        }
    }

    /// Advance to `Done` once the donor has finished scanning and deleting
    /// its migrated key range.
    pub fn cleanup_complete(&mut self) {
        if let SplitPhase::Cleanup {
            cutover_epoch,
            rows_to_cleanup,
        } = self.phase
        {
            self.phase = SplitPhase::Done {
                cutover_epoch,
                rows_migrated: rows_to_cleanup,
            };
        } else {
            panic!("cleanup_complete called in wrong phase: {:?}", self.phase);
        }
    }

    /// Whether the split has completed all phases.
    pub fn is_done(&self) -> bool {
        matches!(self.phase, SplitPhase::Done { .. })
    }

    /// Whether this key (by its `key_hash`) belongs to the new shard after cutover.
    pub fn routes_to_new_shard(&self, key_hash: u64) -> bool {
        key_hash >= self.split_key_hash
    }
}

// ─── Merge State Machine ──────────────────────────────────────────────────────

/// Phase of a shard merge operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergePhase {
    /// No merge in progress.
    Idle,
    /// Target shard is absorbing rows from the source shard's range.
    Absorbing {
        /// ID of the shard being absorbed (will be decommissioned after merge).
        absorbed_shard: ShardId,
        /// Rows absorbed from the source shard so far.
        rows_absorbed: u64,
    },
    /// Absorption complete; waiting for epoch boundary to bump the shard map.
    AwaitingCutover {
        /// Total rows absorbed.
        rows_absorbed: u64,
    },
    /// Merge complete; absorbed shard has been decommissioned.
    Done {
        /// The epoch at which the shard map version was bumped.
        cutover_epoch: Epoch,
        /// Total rows that were absorbed.
        rows_absorbed: u64,
    },
}

/// An in-progress shard merge operation.
///
/// The target shard absorbs the full state of the source shard.  Both shards
/// must be quiesced (no in-flight writes) before absorption begins.  The
/// target shard applies each key-value pair using the law's merge function
/// (`LawBundle::merge`) to correctly handle overlapping keys.
#[derive(Debug, Clone)]
pub struct ShardMergeOp {
    /// Shard that will absorb the source shard's state.
    pub target_id: ShardId,
    /// Shard being decommissioned (its state moves to `target_id`).
    pub source_id: ShardId,
    /// Current phase.
    pub phase: MergePhase,
}

impl ShardMergeOp {
    /// Create a new merge operation in the `Idle` phase.
    pub fn new(target_id: ShardId, source_id: ShardId) -> Self {
        Self {
            target_id,
            source_id,
            phase: MergePhase::Idle,
        }
    }

    /// Advance to `Absorbing` once both shards are quiesced.
    pub fn begin_absorption(&mut self) {
        assert!(
            matches!(self.phase, MergePhase::Idle),
            "begin_absorption called in wrong phase: {:?}",
            self.phase
        );
        self.phase = MergePhase::Absorbing {
            absorbed_shard: self.source_id,
            rows_absorbed: 0,
        };
    }

    /// Record progress as rows are absorbed from the source shard.
    pub fn record_rows_absorbed(&mut self, count: u64) {
        if let MergePhase::Absorbing { rows_absorbed, .. } = &mut self.phase {
            *rows_absorbed += count;
        }
    }

    /// Advance to `AwaitingCutover` once all source rows have been absorbed.
    pub fn absorption_complete(&mut self) {
        if let MergePhase::Absorbing { rows_absorbed, .. } = self.phase {
            self.phase = MergePhase::AwaitingCutover { rows_absorbed };
        } else {
            panic!("absorption_complete called in wrong phase: {:?}", self.phase);
        }
    }

    /// Advance to `Done` at the epoch boundary when the shard-map version bumps
    /// and the source shard is decommissioned.
    pub fn cutover(&mut self, cutover_epoch: Epoch) {
        if let MergePhase::AwaitingCutover { rows_absorbed } = self.phase {
            self.phase = MergePhase::Done {
                cutover_epoch,
                rows_absorbed,
            };
        } else {
            panic!("cutover called in wrong phase: {:?}", self.phase);
        }
    }

    /// Whether the merge has completed all phases.
    pub fn is_done(&self) -> bool {
        matches!(self.phase, MergePhase::Done { .. })
    }
}

// ─── Migration Statistics ─────────────────────────────────────────────────────

/// Statistics collected during a shard split or merge operation.
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// Total rows moved between shards.
    pub rows_migrated: u64,
    /// Total bytes moved between shards.
    pub bytes_migrated: u64,
    /// Rows removed by TombstoneGc during donor cleanup.
    pub tombstones_gc_d: u64,
    /// Duration of the migration in simulated milliseconds.
    pub duration_ms: u64,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_op_full_lifecycle() {
        let mut op = ShardSplitOp::new(ShardId(0), ShardId(1), u64::MAX / 2);
        assert!(matches!(op.phase, SplitPhase::Idle));

        op.begin_checkpoint(CheckpointId(1));
        assert!(matches!(op.phase, SplitPhase::Checkpointing { .. }));

        op.checkpoint_ready();
        assert!(matches!(op.phase, SplitPhase::Copying { rows_copied: 0, .. }));

        op.record_rows_copied(500);
        op.record_rows_copied(300);
        assert!(
            matches!(op.phase, SplitPhase::Copying { rows_copied: 800, .. })
        );

        op.copy_complete();
        assert!(matches!(
            op.phase,
            SplitPhase::AwaitingCutover { rows_copied: 800 }
        ));

        op.cutover(42);
        assert!(matches!(
            op.phase,
            SplitPhase::Cleanup {
                cutover_epoch: 42,
                rows_to_cleanup: 800
            }
        ));

        op.cleanup_complete();
        assert!(matches!(
            op.phase,
            SplitPhase::Done {
                cutover_epoch: 42,
                rows_migrated: 800
            }
        ));
        assert!(op.is_done());
    }

    #[test]
    fn merge_op_full_lifecycle() {
        let mut op = ShardMergeOp::new(ShardId(0), ShardId(1));
        assert!(matches!(op.phase, MergePhase::Idle));

        op.begin_absorption();
        assert!(matches!(op.phase, MergePhase::Absorbing { rows_absorbed: 0, .. }));

        op.record_rows_absorbed(1_200);
        assert!(matches!(
            op.phase,
            MergePhase::Absorbing {
                rows_absorbed: 1_200,
                ..
            }
        ));

        op.absorption_complete();
        assert!(matches!(
            op.phase,
            MergePhase::AwaitingCutover { rows_absorbed: 1_200 }
        ));

        op.cutover(99);
        assert!(matches!(
            op.phase,
            MergePhase::Done {
                cutover_epoch: 99,
                rows_absorbed: 1_200
            }
        ));
        assert!(op.is_done());
    }

    #[test]
    fn split_key_routing() {
        let mid = u64::MAX / 2;
        let op = ShardSplitOp::new(ShardId(0), ShardId(1), mid);
        assert!(!op.routes_to_new_shard(0));
        assert!(!op.routes_to_new_shard(mid - 1));
        assert!(op.routes_to_new_shard(mid));
        assert!(op.routes_to_new_shard(u64::MAX));
    }
}

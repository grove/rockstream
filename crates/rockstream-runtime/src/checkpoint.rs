//! Cluster checkpoint protocol — v0.34.
//!
//! Implements the four-component checkpoint algorithm described in DESIGN.md §11:
//!
//! ```text
//!  Source ──barrier──► [AlignmentBuffer] ──► Operator ──► Sink
//!                               │
//!                         ShardCheckpointAck
//!                               │
//!                     CheckpointCoordinator
//!                        (atomic commit)
//!                               │
//!                         CheckpointGc
//!                      (old checkpoint GC)
//! ```
//!
//! ## Barrier injection
//!
//! A `CheckpointBarrier` is stamped with a `CheckpointId` and the `barrier_epoch`
//! at which it was injected.  The barrier flows through the data path; when an
//! operator receives it, the operator takes a local snapshot and sends a
//! `ShardCheckpointAck` back to the `CheckpointCoordinator`.
//!
//! ## Bounded alignment buffers
//!
//! Multi-input operators (joins, unions) may receive the barrier on one input
//! before the other.  Data arriving from the fast input is held in an
//! `AlignmentBuffer` while the slow input catches up.  The buffer has a hard
//! `max_rows` limit.  When the limit is reached the buffer returns `RS-3601`
//! rather than growing unboundedly.
//!
//! ## Atomic cluster checkpoint commit
//!
//! `CheckpointCoordinator` collects `ShardCheckpointAck` messages.  When every
//! registered shard has acknowledged the current barrier the coordinator
//! transitions to `CheckpointStatus::Committed`.  If the coordinator is aborted
//! (e.g. a shard fails) it transitions to `CheckpointStatus::Recovering` and
//! surfaces `RS-3602`.
//!
//! ## Old checkpoint GC
//!
//! `CheckpointGc` tracks `(CheckpointId, barrier_epoch)` pairs.  When the
//! cluster frontier advances past `barrier_epoch`, the corresponding checkpoint
//! is safe to delete and `collect(frontier_epoch)` returns its ID.

use std::collections::HashSet;

use rockstream_types::ids::ShardId;
use rockstream_types::timestamp::Epoch;

// ─── CheckpointId ─────────────────────────────────────────────────────────────

/// A sequential cluster checkpoint identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CheckpointId(pub u64);

impl std::fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ckpt-{}", self.0)
    }
}

// ─── CheckpointBarrier ────────────────────────────────────────────────────────

/// A barrier injected at `barrier_epoch`.
///
/// The barrier flows through the data path.  When an operator receives it, the
/// operator drains its in-flight state, creates a per-shard checkpoint, and
/// sends a `ShardCheckpointAck` to the `CheckpointCoordinator`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointBarrier {
    /// Sequential checkpoint identifier.
    pub checkpoint_id: CheckpointId,
    /// The epoch at which the barrier was injected.  Operators complete all
    /// work for epochs strictly less than `barrier_epoch` before checkpointing.
    pub barrier_epoch: Epoch,
}

// ─── CheckpointStatus ────────────────────────────────────────────────────────

/// The status of a cluster checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointStatus {
    /// Barrier has been injected; waiting for all shard acks.
    InProgress {
        checkpoint_id: CheckpointId,
        barrier_epoch: Epoch,
        /// Number of shards that have not yet acknowledged.
        pending_shards: usize,
    },
    /// All shards acknowledged; checkpoint is durably committed.
    Committed {
        checkpoint_id: CheckpointId,
        barrier_epoch: Epoch,
        /// Number of shards that participated.
        shard_count: usize,
    },
    /// Checkpoint aborted; pipeline is in RECOVERING state (RS-3602).
    Recovering {
        checkpoint_id: CheckpointId,
        /// Human-readable reason for the failure.
        reason: String,
    },
}

// ─── ShardCheckpointAck ───────────────────────────────────────────────────────

/// Acknowledgement sent by a shard after it has written its local snapshot.
#[derive(Debug, Clone)]
pub struct ShardCheckpointAck {
    /// The shard that produced this ack.
    pub shard_id: ShardId,
    /// The checkpoint being acknowledged.
    pub checkpoint_id: CheckpointId,
    /// The epoch at which the snapshot was taken.
    pub epoch: Epoch,
    /// Approximate size of the checkpoint state in bytes.
    pub state_size_bytes: u64,
}

// ─── AlignmentBuffer ─────────────────────────────────────────────────────────

/// Bounded alignment buffer for a multi-input operator.
///
/// When a `CheckpointBarrier` arrives on one input (the fast input) before the
/// other (the slow input), the operator holds incoming fast-side rows here until
/// the barrier arrives on the slow side.  This keeps the checkpoint semantics
/// consistent: no row crosses the barrier unless it has been checkpointed on
/// all inputs.
///
/// The buffer has a hard `max_rows` capacity.  When the capacity is reached
/// `push` returns `Err("RS-3601: …")` instead of growing the buffer
/// unboundedly.  The caller is expected to surface this error as `RS-3601`
/// and stop the pipeline until the barrier is cleared.
pub struct AlignmentBuffer {
    max_rows: usize,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
}

impl AlignmentBuffer {
    /// Create a new buffer with a hard capacity of `max_rows`.
    ///
    /// # Panics
    ///
    /// Panics if `max_rows == 0`.
    pub fn new(max_rows: usize) -> Self {
        assert!(max_rows > 0, "max_rows must be > 0");
        Self {
            max_rows,
            rows: Vec::new(),
        }
    }

    /// Push a `(key, value)` row into the buffer.
    ///
    /// Returns `Err("RS-3601: …")` if the buffer is at capacity.
    pub fn push(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        if self.rows.len() >= self.max_rows {
            return Err(format!(
                "RS-3601: checkpoint alignment buffer overflowed (capacity={}, \
                 pipeline halted until barrier clears). Reduce input rate or \
                 increase alignment_buffer_max_rows.",
                self.max_rows
            ));
        }
        self.rows.push((key, value));
        Ok(())
    }

    /// Number of buffered rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if the buffer holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Drain all buffered rows, leaving the buffer empty.
    pub fn drain(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        std::mem::take(&mut self.rows)
    }

    /// Maximum number of rows the buffer will hold before returning RS-3601.
    pub fn max_rows(&self) -> usize {
        self.max_rows
    }
}

// ─── CheckpointCoordinator ───────────────────────────────────────────────────

/// Atomic cluster checkpoint coordinator.
///
/// Responsibilities:
/// - Inject barriers by calling `inject_barrier(epoch)`.
/// - Accept shard acknowledgements via `ack_shard(ack)`.
/// - When all shards have acknowledged: transition to `Committed`.
/// - When explicitly aborted: transition to `Recovering` (RS-3602).
/// - Track committed checkpoints for GC via the embedded `CheckpointGc`.
pub struct CheckpointCoordinator {
    num_shards: usize,
    next_id: u64,
    current: Option<InProgressCheckpoint>,
    gc: CheckpointGc,
}

struct InProgressCheckpoint {
    id: CheckpointId,
    barrier_epoch: Epoch,
    pending: HashSet<ShardId>,
    total_shards: usize,
}

impl CheckpointCoordinator {
    /// Create a coordinator for a cluster with `num_shards` shards.
    ///
    /// `max_retained_checkpoints` controls how many committed checkpoint IDs
    /// are tracked for GC before old entries are pruned.
    pub fn new(num_shards: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        Self {
            num_shards,
            next_id: 0,
            current: None,
            gc: CheckpointGc::new(),
        }
    }

    /// Inject a new barrier at `barrier_epoch`.
    ///
    /// Returns `Err` if a checkpoint is already in progress for this
    /// coordinator (only one in-flight checkpoint is supported at a time).
    pub fn inject_barrier(&mut self, barrier_epoch: Epoch) -> Result<CheckpointBarrier, String> {
        if self.current.is_some() {
            return Err(
                "RS-3602: cannot inject a new barrier while a checkpoint is already in \
                 progress; wait for the current checkpoint to commit or abort it first."
                    .into(),
            );
        }
        let id = CheckpointId(self.next_id);
        self.next_id += 1;

        let pending: HashSet<ShardId> = (0..self.num_shards as u64).map(ShardId).collect();
        self.current = Some(InProgressCheckpoint {
            id,
            barrier_epoch,
            pending,
            total_shards: self.num_shards,
        });

        Ok(CheckpointBarrier {
            checkpoint_id: id,
            barrier_epoch,
        })
    }

    /// Accept a `ShardCheckpointAck`.
    ///
    /// If this is the last outstanding ack, returns `Committed`; otherwise
    /// returns `InProgress` with the updated `pending_shards` count.
    ///
    /// Returns `Err` if no checkpoint is in progress or the ack belongs to a
    /// different checkpoint.
    pub fn ack_shard(&mut self, ack: ShardCheckpointAck) -> Result<CheckpointStatus, String> {
        let cp = self
            .current
            .as_mut()
            .ok_or_else(|| "no checkpoint in progress; cannot accept shard ack".to_owned())?;

        if ack.checkpoint_id != cp.id {
            return Err(format!(
                "ack checkpoint_id {} does not match current checkpoint {}",
                ack.checkpoint_id, cp.id
            ));
        }

        cp.pending.remove(&ack.shard_id);

        if cp.pending.is_empty() {
            let id = cp.id;
            let epoch = cp.barrier_epoch;
            let count = cp.total_shards;
            self.current = None;
            self.gc.track(id, epoch);
            Ok(CheckpointStatus::Committed {
                checkpoint_id: id,
                barrier_epoch: epoch,
                shard_count: count,
            })
        } else {
            Ok(CheckpointStatus::InProgress {
                checkpoint_id: cp.id,
                barrier_epoch: cp.barrier_epoch,
                pending_shards: cp.pending.len(),
            })
        }
    }

    /// Abort the current checkpoint.
    ///
    /// Transitions to `Recovering` (RS-3602).  Clears the in-progress state so
    /// a new barrier can be injected after recovery.
    pub fn abort(&mut self, reason: impl Into<String>) -> CheckpointStatus {
        let id = self
            .current
            .as_ref()
            .map(|cp| cp.id)
            .unwrap_or(CheckpointId(u64::MAX));
        self.current = None;
        CheckpointStatus::Recovering {
            checkpoint_id: id,
            reason: format!(
                "RS-3602: cluster checkpoint recovery in progress — {}",
                reason.into()
            ),
        }
    }

    /// Return the current checkpoint status without modifying state.
    ///
    /// Returns `None` if no checkpoint is in progress and no barrier has been
    /// injected since construction.
    pub fn current_status(&self) -> Option<CheckpointStatus> {
        self.current
            .as_ref()
            .map(|cp| CheckpointStatus::InProgress {
                checkpoint_id: cp.id,
                barrier_epoch: cp.barrier_epoch,
                pending_shards: cp.pending.len(),
            })
    }

    /// Collect committed checkpoint IDs whose `barrier_epoch` is strictly less
    /// than `frontier_epoch`.  These are safe to delete from durable storage.
    ///
    /// Returns the list of checkpoint IDs to delete.
    pub fn gc_old_checkpoints(&mut self, frontier_epoch: Epoch) -> Vec<CheckpointId> {
        self.gc.collect(frontier_epoch)
    }

    /// Number of committed checkpoints currently tracked for GC.
    pub fn gc_len(&self) -> usize {
        self.gc.len()
    }
}

// ─── CheckpointGc ────────────────────────────────────────────────────────────

/// Old-checkpoint garbage collector.
///
/// Tracks `(CheckpointId, barrier_epoch)` pairs.  When the cluster frontier
/// advances past `barrier_epoch`, the corresponding checkpoint can be deleted.
///
/// `CheckpointGc` does not perform deletions itself — it returns the IDs to
/// delete so the caller can issue the appropriate storage `delete` calls.
pub struct CheckpointGc {
    entries: Vec<(CheckpointId, Epoch)>,
}

impl CheckpointGc {
    /// Create a new (empty) GC handle.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Register a committed checkpoint.
    pub fn track(&mut self, id: CheckpointId, barrier_epoch: Epoch) {
        self.entries.push((id, barrier_epoch));
    }

    /// Advance the frontier to `frontier_epoch`.
    ///
    /// Returns all checkpoint IDs whose `barrier_epoch < frontier_epoch`.
    /// Those entries are removed from the tracked set.
    pub fn collect(&mut self, frontier_epoch: Epoch) -> Vec<CheckpointId> {
        let mut to_delete = Vec::new();
        self.entries.retain(|(id, epoch)| {
            if *epoch < frontier_epoch {
                to_delete.push(*id);
                false
            } else {
                true
            }
        });
        to_delete
    }

    /// Number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no entries are tracked.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for CheckpointGc {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_id_display() {
        assert_eq!(CheckpointId(0).to_string(), "ckpt-0");
        assert_eq!(CheckpointId(42).to_string(), "ckpt-42");
    }

    #[test]
    fn alignment_buffer_rejects_when_full() {
        let mut buf = AlignmentBuffer::new(2);
        assert!(buf.push(b"k1".to_vec(), b"v1".to_vec()).is_ok());
        assert!(buf.push(b"k2".to_vec(), b"v2".to_vec()).is_ok());
        let err = buf.push(b"k3".to_vec(), b"v3".to_vec()).unwrap_err();
        assert!(err.contains("RS-3601"), "expected RS-3601 in error: {err}");
        // Buffer did not grow.
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn alignment_buffer_drain_clears() {
        let mut buf = AlignmentBuffer::new(4);
        buf.push(b"a".to_vec(), b"1".to_vec()).unwrap();
        buf.push(b"b".to_vec(), b"2".to_vec()).unwrap();
        let rows = buf.drain();
        assert_eq!(rows.len(), 2);
        assert!(buf.is_empty());
    }

    #[test]
    fn coordinator_inject_and_commit_single_shard() {
        let mut coord = CheckpointCoordinator::new(1);
        let barrier = coord.inject_barrier(5).unwrap();
        assert_eq!(barrier.barrier_epoch, 5);
        assert_eq!(barrier.checkpoint_id, CheckpointId(0));

        let status = coord
            .ack_shard(ShardCheckpointAck {
                shard_id: ShardId(0),
                checkpoint_id: barrier.checkpoint_id,
                epoch: 5,
                state_size_bytes: 1024,
            })
            .unwrap();
        assert!(
            matches!(
                status,
                CheckpointStatus::Committed {
                    barrier_epoch: 5,
                    shard_count: 1,
                    ..
                }
            ),
            "expected Committed: {status:?}"
        );
    }

    #[test]
    fn coordinator_abort_returns_recovering() {
        let mut coord = CheckpointCoordinator::new(2);
        let _barrier = coord.inject_barrier(10).unwrap();
        let status = coord.abort("shard-1 lost lease");
        assert!(
            matches!(status, CheckpointStatus::Recovering { .. }),
            "expected Recovering: {status:?}"
        );
        if let CheckpointStatus::Recovering { reason, .. } = &status {
            assert!(
                reason.contains("RS-3602"),
                "expected RS-3602 in reason: {reason}"
            );
        }
    }

    #[test]
    fn gc_collects_old_checkpoints() {
        let mut gc = CheckpointGc::new();
        gc.track(CheckpointId(0), 3);
        gc.track(CheckpointId(1), 7);
        gc.track(CheckpointId(2), 10);

        let deleted = gc.collect(8);
        assert_eq!(deleted.len(), 2);
        assert!(deleted.contains(&CheckpointId(0)));
        assert!(deleted.contains(&CheckpointId(1)));
        assert_eq!(gc.len(), 1);
    }
}

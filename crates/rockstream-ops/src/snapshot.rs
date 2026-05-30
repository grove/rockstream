//! Snapshot source operator for RockStream (v0.23).
//!
//! ## Design
//!
//! `SnapshotOp` delivers an existing relation as a sequence of insert-only
//! bootstrap epochs.  Each epoch emits at most `batch_size` rows as
//! positive-weight Z-set entries.  Once all rows have been emitted, `is_complete()`
//! returns `true` — the **bootstrap frontier** has been reached.
//!
//! ### Streamed bootstrap epochs
//!
//! ```text
//! rows = [r0, r1, ..., rN]
//! batch_size = B
//!
//! epoch 0 → ZSet { r0..rB-1 (weight=+1) }
//! epoch 1 → ZSet { rB..r2B-1 (weight=+1) }
//! ...
//! epoch K → ZSet { r(K*B)..rN (weight=+1) }   ← bootstrap complete
//! ```
//!
//! ### Reconciliation after connector position loss
//!
//! If a connector loses its position (e.g., after a crash with no saved offset)
//! the caller can invoke `resume_from(committed_rows)` to skip the first
//! `committed_rows` entries and re-deliver only the remaining rows.  This
//! prevents duplication: rows already committed to the arrangement are not
//! re-emitted.
//!
//! ### Bootstrap frontier
//!
//! The bootstrap phase ends when `is_complete()` is `true`.  Downstream
//! operators can use this as the `complete_through` token: all snapshot rows
//! have been emitted and committed, and no further snapshot inserts will
//! arrive.  After bootstrap the source transitions to live CDC streaming mode
//! (handled by the surrounding pipeline, not this operator).
//!
//! ### RS-1010
//!
//! If bootstrap is interrupted and the operator cannot determine the committed
//! watermark (position lost), the caller should return `RS-1010` to the user.
//! This operator surface exposes `rows_delivered()` so the pipeline can track
//! and persist the watermark between epochs.

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Current phase of the snapshot source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapPhase {
    /// Snapshot rows are still being delivered.
    Bootstrapping,
    /// All rows have been delivered; bootstrap frontier reached.
    Complete,
}

/// The `SnapshotOp` bootstrap source operator.
///
/// Delivers a pre-loaded list of `(key, value)` rows as insert-only Z-set
/// batches.  The operator is stateless from the IVM arrangement perspective:
/// it holds the source rows in memory and emits them in `batch_size` chunks.
pub struct SnapshotOp {
    /// All rows to be delivered.
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Maximum rows per bootstrap epoch.
    batch_size: usize,
    /// Number of rows already delivered (committed watermark).
    committed_watermark: usize,
    /// Operator display name.
    name: String,
}

impl SnapshotOp {
    /// Create a new `SnapshotOp`.
    ///
    /// # Parameters
    /// - `rows`: all `(key, value)` pairs to deliver.
    /// - `batch_size`: maximum rows per epoch; must be > 0.
    pub fn new(rows: Vec<(Vec<u8>, Vec<u8>)>, batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be positive");
        Self {
            rows,
            batch_size,
            committed_watermark: 0,
            name: "SnapshotOp".to_owned(),
        }
    }

    /// Resume bootstrap from the given committed watermark.
    ///
    /// Rows with index `< committed_rows` are skipped on the next delivery.
    /// This implements reconciliation after connector position loss: call
    /// `resume_from(N)` where `N` is the last persisted watermark to avoid
    /// re-emitting already-committed rows.
    ///
    /// Returns `Err("RS-1010: ...")` if `committed_rows` exceeds the total
    /// row count (position is ahead of the snapshot).
    pub fn resume_from(&mut self, committed_rows: usize) -> Result<(), String> {
        if committed_rows > self.rows.len() {
            return Err(format!(
                "RS-1010: committed watermark {} exceeds snapshot size {}",
                committed_rows,
                self.rows.len()
            ));
        }
        self.committed_watermark = committed_rows;
        Ok(())
    }

    /// Deliver the next batch of rows as a Z-set of inserts.
    ///
    /// Returns a `ZSet` containing at most `batch_size` rows with weight `+1`.
    /// Returns an empty `ZSet` if bootstrap is already complete.
    pub fn next_batch(&mut self) -> ZSet {
        let mut output = ZSet::new();
        let start = self.committed_watermark;
        let end = (start + self.batch_size).min(self.rows.len());
        for i in start..end {
            let (key, value) = &self.rows[i];
            output.insert(key.clone(), value.clone(), 1);
        }
        self.committed_watermark = end;
        output
    }

    /// Deliver all remaining rows as a sequence of batches.
    ///
    /// Each call to `next_batch()` is collected until `is_complete()`.
    /// Returns an empty `Vec` if already complete.
    pub fn drain_all(&mut self) -> Vec<ZSet> {
        let mut batches = Vec::new();
        while !self.is_complete() {
            let batch = self.next_batch();
            if !batch.is_empty() {
                batches.push(batch);
            }
        }
        batches
    }

    /// Whether all rows have been delivered.
    ///
    /// This is the **bootstrap frontier** signal: when `is_complete()` is
    /// `true`, all snapshot rows have been emitted and no further bootstrap
    /// inserts will arrive.
    pub fn is_complete(&self) -> bool {
        self.committed_watermark >= self.rows.len()
    }

    /// Current bootstrap phase.
    pub fn phase(&self) -> BootstrapPhase {
        if self.is_complete() {
            BootstrapPhase::Complete
        } else {
            BootstrapPhase::Bootstrapping
        }
    }

    /// Number of rows already delivered (the committed watermark).
    pub fn rows_delivered(&self) -> usize {
        self.committed_watermark
    }

    /// Total number of rows in the snapshot.
    pub fn total_rows(&self) -> usize {
        self.rows.len()
    }

    /// Number of rows remaining to be delivered.
    pub fn rows_remaining(&self) -> usize {
        self.rows.len().saturating_sub(self.committed_watermark)
    }
}

#[async_trait]
impl Operator for SnapshotOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    /// Deliver the next batch of snapshot rows.
    ///
    /// The `input` batch is ignored; the operator advances its own internal
    /// position.  Returns an empty delta once all rows have been delivered.
    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        ZSetBatch {
            zset: self.next_batch(),
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    /// Snapshot rows accumulate via `WeightAdd/v1` (insert-only, no
    /// retractions during bootstrap).
    fn merge_law(&self) -> Option<MergeLawId> {
        Some(WEIGHT_ADD_ID)
    }
}

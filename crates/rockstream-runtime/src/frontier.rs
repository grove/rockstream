//! Frontier protocol runtime — v0.32.
//!
//! Implements the three-layer frontier protocol described in DESIGN.md §10:
//!
//! ```text
//! [Shard]  ShardFrontierReporter ──┐
//! [Shard]  ShardFrontierReporter ──┤
//!                                  │ ShardFrontierReport
//!                             WorkerFrontierAggregator
//!                                  │ WorkerFrontierSummary
//!                       ClusterFrontierPublisher
//!                                  │ ClusterFrontier
//! ```
//!
//! # No per-shard subscriptions
//!
//! Consumers subscribe to `WorkerFrontierSummary` or `ClusterFrontier`
//! channels only.  There are no direct shard→operator subscriptions, so the
//! number of live channels is O(workers) not O(shards × operators).
//!
//! # Monotone partial progress
//!
//! Laws with `MergeLawClass::Semilattice` (idempotent merge) may call
//! `is_monotone_law()` and, if true, emit a `CompleteThroughToken` before
//! the cluster frontier has advanced.
//!
//! # Shuffle GC
//!
//! `ShuffleGc` tracks `(OutboxEntry, epoch)` pairs and deletes object-store
//! entries whose epoch falls strictly below the cluster frontier.

use std::collections::HashMap;
use std::sync::Arc;

use rockstream_types::frontier::{
    ClusterFrontier, CompleteThroughToken, ShardFrontierReport, WorkerFrontierSummary,
};
use rockstream_types::ids::{OperatorId, ShardId, WorkerId};
use rockstream_types::laws::LawRegistry;
use rockstream_types::merge_law::{LawBundle, MergeLawClass, MergeLawId};
use rockstream_types::timestamp::Epoch;

// ─── Worker role ─────────────────────────────────────────────────────────────

/// The role a worker process fulfils in the cluster.
///
/// Pass `--role=frontier` on the command line to start a dedicated frontier
/// aggregation node that only runs `ClusterFrontierPublisher` and does not
/// execute operator tasks.  In small clusters the default `Compute` role
/// performs all duties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierRole {
    /// Normal worker: executes operator tasks AND participates in frontier
    /// aggregation.
    Compute,
    /// Dedicated frontier node: runs only `ClusterFrontierPublisher`, no
    /// operator tasks.  Separable via `--role=frontier`.
    Frontier,
}

impl std::fmt::Display for FrontierRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrontierRole::Compute => write!(f, "compute"),
            FrontierRole::Frontier => write!(f, "frontier"),
        }
    }
}

impl std::str::FromStr for FrontierRole {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "compute" => Ok(FrontierRole::Compute),
            "frontier" => Ok(FrontierRole::Frontier),
            other => Err(format!("unknown role: {other}")),
        }
    }
}

// ─── Shard frontier reporter ─────────────────────────────────────────────────

/// Per-shard frontier reporter.
///
/// After each `commit_epoch` the shard calls `advance(epoch)` to push a
/// `ShardFrontierReport` to the aggregator channel.  Creating the reporter
/// registers the shard with the aggregator.
pub struct ShardFrontierReporter {
    shard_id: ShardId,
    tx: std::sync::mpsc::SyncSender<ShardFrontierReport>,
}

impl ShardFrontierReporter {
    /// Create a new reporter.  The caller is responsible for registering
    /// `shard_id` with a `WorkerFrontierAggregator` before the first advance.
    pub fn new(shard_id: ShardId, tx: std::sync::mpsc::SyncSender<ShardFrontierReport>) -> Self {
        Self { shard_id, tx }
    }

    /// Advance the frontier to `epoch` (i.e., all epochs < `epoch` are committed).
    ///
    /// Sends a `ShardFrontierReport` to the aggregator channel.
    /// Returns `Err` if the aggregator has been dropped.
    pub fn advance(
        &self,
        epoch: Epoch,
    ) -> Result<(), std::sync::mpsc::TrySendError<ShardFrontierReport>> {
        self.tx.try_send(ShardFrontierReport {
            shard_id: self.shard_id,
            epoch,
        })
    }

    /// Returns the shard ID this reporter is bound to.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

// ─── Worker frontier aggregator ──────────────────────────────────────────────

/// Worker-level frontier aggregator.
///
/// Collects `ShardFrontierReport` values from all shards on this worker and
/// computes the minimum epoch.  Publishes a `WorkerFrontierSummary` whenever
/// the minimum changes.
///
/// # Subscribe-free design
///
/// All shards push to a single `std::sync::mpsc` channel.  The aggregator
/// drains that channel in `poll()`.  No operator is directly subscribed to
/// individual shard channels.
pub struct WorkerFrontierAggregator {
    worker_id: WorkerId,
    rx: std::sync::mpsc::Receiver<ShardFrontierReport>,
    tx: std::sync::mpsc::SyncSender<ShardFrontierReport>,
    per_shard: HashMap<ShardId, Epoch>,
    last_published: Option<Epoch>,
}

impl WorkerFrontierAggregator {
    /// Create a new aggregator for `worker_id`.
    ///
    /// `capacity` is the bounded backlog for the internal channel.  Use a
    /// value of at least `num_shards * 2` to avoid dropping reports under
    /// burst conditions.
    pub fn new(worker_id: WorkerId, capacity: usize) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(capacity);
        Self {
            worker_id,
            rx,
            tx,
            per_shard: HashMap::new(),
            last_published: None,
        }
    }

    /// Register a shard and return a `ShardFrontierReporter` bound to it.
    ///
    /// Must be called before the shard's first `advance()`.
    pub fn register_shard(&mut self, shard_id: ShardId) -> ShardFrontierReporter {
        self.per_shard.entry(shard_id).or_insert(0);
        ShardFrontierReporter::new(shard_id, self.tx.clone())
    }

    /// Drain all pending `ShardFrontierReport` messages, update per-shard
    /// epochs, and return a `WorkerFrontierSummary` if the minimum has
    /// changed.
    ///
    /// Call this in the worker's main loop or a dedicated aggregation task.
    pub fn poll(&mut self) -> Option<WorkerFrontierSummary> {
        let mut changed = false;
        while let Ok(report) = self.rx.try_recv() {
            let slot = self.per_shard.entry(report.shard_id).or_insert(0);
            if report.epoch > *slot {
                *slot = report.epoch;
                changed = true;
            }
        }
        if !changed {
            return None;
        }
        let min = self.compute_min();
        if min != self.last_published {
            self.last_published = min;
            Some(WorkerFrontierSummary {
                worker_id: self.worker_id,
                min_epoch: min,
            })
        } else {
            None
        }
    }

    /// Force-emit the current summary regardless of whether it changed.
    pub fn summary(&self) -> WorkerFrontierSummary {
        WorkerFrontierSummary {
            worker_id: self.worker_id,
            min_epoch: self.compute_min(),
        }
    }

    fn compute_min(&self) -> Option<Epoch> {
        if self.per_shard.is_empty() {
            return None;
        }
        self.per_shard.values().copied().min()
    }

    /// Returns the worker ID.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
    }
}

// ─── Cluster frontier publisher ──────────────────────────────────────────────

/// Cluster-level frontier publisher.
///
/// Aggregates `WorkerFrontierSummary` values from all workers in the cluster
/// and computes the global minimum epoch.  The resulting `ClusterFrontier`
/// is the safe point at which shuffle GC and downstream consumers may act.
pub struct ClusterFrontierPublisher {
    per_worker: HashMap<WorkerId, Option<Epoch>>,
}

impl ClusterFrontierPublisher {
    /// Create a new publisher with no registered workers.
    pub fn new() -> Self {
        Self {
            per_worker: HashMap::new(),
        }
    }

    /// Register a worker.  Its frontier is initialised to `None`.
    pub fn register_worker(&mut self, worker_id: WorkerId) {
        self.per_worker.entry(worker_id).or_insert(None);
    }

    /// Accept a `WorkerFrontierSummary` and return the updated
    /// `ClusterFrontier`.
    pub fn update(&mut self, summary: WorkerFrontierSummary) -> ClusterFrontier {
        self.per_worker.insert(summary.worker_id, summary.min_epoch);
        ClusterFrontier {
            epoch: self.compute_cluster_min(),
        }
    }

    /// Compute the current cluster frontier without accepting a new update.
    pub fn current(&self) -> ClusterFrontier {
        ClusterFrontier {
            epoch: self.compute_cluster_min(),
        }
    }

    fn compute_cluster_min(&self) -> Option<Epoch> {
        if self.per_worker.is_empty() {
            return None;
        }
        // If any worker has not yet reported (None), the cluster frontier is None.
        let mut min: Option<Epoch> = None;
        for opt in self.per_worker.values() {
            match opt {
                None => return None,
                Some(e) => {
                    min = Some(match min {
                        None => *e,
                        Some(m) => m.min(*e),
                    });
                }
            }
        }
        min
    }
}

impl Default for ClusterFrontierPublisher {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Monotone law helpers ─────────────────────────────────────────────────────

/// Returns `true` if the law identified by `id` in `registry` is monotone
/// (semilattice / idempotent merge), meaning it is safe to emit partial
/// progress tokens before the cluster frontier.
pub fn is_monotone_law(id: MergeLawId, registry: &dyn LawRegistryLike) -> bool {
    registry
        .get(id)
        .map(|law| law.class() == MergeLawClass::Semilattice)
        .unwrap_or(false)
}

/// Minimal trait covering the `LawRegistry::get` method so the frontier
/// module does not depend on the concrete `LawRegistry` type.
pub trait LawRegistryLike: Send + Sync {
    /// Look up a law by ID.
    fn get(&self, id: MergeLawId) -> Option<&Arc<dyn LawBundle>>;
}

impl LawRegistryLike for LawRegistry {
    fn get(&self, id: MergeLawId) -> Option<&Arc<dyn LawBundle>> {
        LawRegistry::get(self, id)
    }
}

/// Build a `CompleteThroughToken` for a monotone operator.
///
/// Returns `Some(token)` if `law_id` is a monotone law; `None` otherwise.
/// Callers must not emit tokens for non-monotone laws.
pub fn try_complete_through(
    operator_id: OperatorId,
    law_id: MergeLawId,
    complete_through: Epoch,
    registry: &dyn LawRegistryLike,
) -> Option<CompleteThroughToken> {
    if is_monotone_law(law_id, registry) {
        Some(CompleteThroughToken {
            operator_id,
            law_id,
            complete_through,
        })
    } else {
        None
    }
}

// ─── Shuffle GC ──────────────────────────────────────────────────────────────

/// A record of a shuffle outbox entry plus the epoch in which it was written.
#[derive(Debug, Clone)]
pub struct ShuffleOutboxRecord {
    /// Path of the shuffle object in the object store.
    pub path: String,
    /// The pipeline epoch during which this object was written.
    pub epoch: Epoch,
}

/// Shuffle outbox garbage collector.
///
/// Tracks `ShuffleOutboxRecord` entries and, when the cluster frontier
/// advances, identifies paths that are safe to delete (their epoch is
/// strictly below the cluster frontier).
///
/// # Object-store deletion
///
/// `ShuffleGc` does not perform deletions itself — it returns the paths to
/// delete so the caller can issue `object_store.delete(&path)` calls.  This
/// keeps the GC logic pure and easily testable without object-store mocks.
pub struct ShuffleGc {
    entries: Vec<ShuffleOutboxRecord>,
}

impl ShuffleGc {
    /// Create a new (empty) GC handle.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Register a newly written shuffle outbox entry.
    pub fn track(&mut self, record: ShuffleOutboxRecord) {
        self.entries.push(record);
    }

    /// Advance the frontier to `cluster_epoch`.  Returns the paths of all
    /// tracked entries whose epoch is strictly less than `cluster_epoch`.
    /// Those entries are removed from the tracked set.
    pub fn collect(&mut self, cluster_epoch: Epoch) -> Vec<String> {
        let mut to_delete = Vec::new();
        self.entries.retain(|e| {
            if e.epoch < cluster_epoch {
                to_delete.push(e.path.clone());
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

impl Default for ShuffleGc {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{ShardId, WorkerId};

    #[test]
    fn frontier_role_roundtrip() {
        assert_eq!(
            "compute".parse::<FrontierRole>().unwrap(),
            FrontierRole::Compute
        );
        assert_eq!(
            "frontier".parse::<FrontierRole>().unwrap(),
            FrontierRole::Frontier
        );
        assert!("unknown".parse::<FrontierRole>().is_err());
        assert_eq!(FrontierRole::Compute.to_string(), "compute");
        assert_eq!(FrontierRole::Frontier.to_string(), "frontier");
    }

    #[test]
    fn worker_aggregator_basic() {
        let mut agg = WorkerFrontierAggregator::new(WorkerId(1), 64);
        let r1 = agg.register_shard(ShardId(1));
        let r2 = agg.register_shard(ShardId(2));
        // Before any advance: summary min_epoch = Some(0)
        assert_eq!(agg.summary().min_epoch, Some(0));
        r1.advance(5).unwrap();
        r2.advance(3).unwrap();
        let s = agg.poll().unwrap();
        assert_eq!(s.min_epoch, Some(3));
    }

    #[test]
    fn worker_aggregator_no_shards() {
        let mut agg = WorkerFrontierAggregator::new(WorkerId(1), 8);
        assert_eq!(agg.summary().min_epoch, None);
        assert!(agg.poll().is_none());
    }

    #[test]
    fn cluster_publisher_basic() {
        let mut pub_ = ClusterFrontierPublisher::new();
        pub_.register_worker(WorkerId(1));
        pub_.register_worker(WorkerId(2));
        // Both workers not yet reported — cluster frontier is None.
        assert_eq!(pub_.current().epoch, None);
        let cf = pub_.update(WorkerFrontierSummary {
            worker_id: WorkerId(1),
            min_epoch: Some(10),
        });
        // Worker 2 still None — cluster frontier still None.
        assert_eq!(cf.epoch, None);
        let cf = pub_.update(WorkerFrontierSummary {
            worker_id: WorkerId(2),
            min_epoch: Some(8),
        });
        assert_eq!(cf.epoch, Some(8));
    }

    #[test]
    fn shuffle_gc_collects_old_entries() {
        let mut gc = ShuffleGc::new();
        gc.track(ShuffleOutboxRecord {
            path: "a/1".into(),
            epoch: 1,
        });
        gc.track(ShuffleOutboxRecord {
            path: "a/2".into(),
            epoch: 5,
        });
        gc.track(ShuffleOutboxRecord {
            path: "a/3".into(),
            epoch: 10,
        });
        // Cluster frontier at epoch 6 — entries with epoch < 6 are deletable.
        let to_del = gc.collect(6);
        assert_eq!(to_del.len(), 2);
        assert!(to_del.contains(&"a/1".to_string()));
        assert!(to_del.contains(&"a/2".to_string()));
        assert_eq!(gc.len(), 1); // entry with epoch=10 remains
    }
}

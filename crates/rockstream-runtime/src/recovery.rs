//! Recovery driver and SLO metrics — v0.35.
//!
//! Implements the recovery path described in DESIGN.md §11.6 and §11.8:
//!
//! ```text
//! Worker heartbeat ──► WorkerHealthMonitor ──► (failure detected)
//!                                                      │
//!                                              RecoveryDriver
//!                                       (shard reassignment + SLO tracking)
//!                                                      │
//!                                      Healthy | Recovering | RecoveringSlow
//!                                                          │ RS-1603
//! Control-plane partition ──► ControlPlaneFence ──► worker stops committing
//!
//! Bulk restart ──► ThrottledLeaseGranter ──► (rate-limited lease grants)
//! ```
//!
//! ## Failure detection (≤5 s SLO)
//!
//! `WorkerHealthMonitor` accepts heartbeat messages and a deterministic
//! millisecond clock.  `tick(now_ms)` returns the list of workers whose last
//! heartbeat is more than `failure_timeout_ms` ago.  In production this
//! typically uses 5 000 ms (5 s) to satisfy the failure-detection SLO.
//!
//! ## Shard reassignment (≤30 s SLO)
//!
//! `RecoveryDriver::handle_worker_failure` calls the underlying
//! `ShardScheduler::on_worker_dead` synchronously — the reassignment itself
//! is instantaneous in the control plane.  The 30 s budget covers the time
//! between failure detection and the new worker completing its first epoch
//! commit, tracked by `RecoveryStatus`.
//!
//! ## RECOVERING_SLOW (RS-1603)
//!
//! If a recovery is still in progress after `slow_threshold_ms` (default
//! 60 000 ms / 60 s), `RecoveryDriver::status(now_ms)` transitions to
//! `RecoveryStatus::RecoveringSlow` and the caller should surface `RS-1603`.
//!
//! ## Worker self-fencing on control-plane partition (DESIGN.md §11.6)
//!
//! `ControlPlaneFence` tracks the time since last control-plane contact.  If
//! more than `fence_timeout_ms` passes without contact the fence fires and the
//! worker must stop committing (return its fencing token to an invalid state).
//!
//! ## Thundering herd prevention (DESIGN.md §11.8)
//!
//! `ThrottledLeaseGranter` caps the number of lease grants in a sliding window.
//! When 32 workers restart simultaneously, only `max_grants_per_window` shards
//! are granted per `window_ms`, preventing a burst of concurrent epoch replays
//! from overwhelming the storage layer.

use std::collections::HashMap;

use rockstream_types::ids::{ShardId, WorkerId};

// ─── WorkerHealthMonitor ─────────────────────────────────────────────────────

/// Health status of a monitored worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Worker is sending heartbeats within the timeout window.
    Healthy,
    /// Worker has not been heard from in more than `failure_timeout_ms`.
    Failed,
}

/// Worker heartbeat tracker with a deterministic millisecond clock.
///
/// In production the caller passes wall-clock milliseconds.  In tests a
/// synthetic counter is used, making failure-detection SLO proofs exact.
pub struct WorkerHealthMonitor {
    failure_timeout_ms: u64,
    workers: HashMap<WorkerId, u64>,
}

impl WorkerHealthMonitor {
    /// Create a monitor.  Workers that do not send a heartbeat within
    /// `failure_timeout_ms` are declared `Failed` by `tick()`.
    pub fn new(failure_timeout_ms: u64) -> Self {
        Self {
            failure_timeout_ms,
            workers: HashMap::new(),
        }
    }

    /// Register a worker at time `now_ms`.  The first heartbeat is implicitly
    /// set to `now_ms`.
    pub fn register(&mut self, id: WorkerId, now_ms: u64) {
        self.workers.insert(id, now_ms);
    }

    /// Record a heartbeat from `id` at time `now_ms`.
    ///
    /// No-op if the worker was never registered.
    pub fn heartbeat(&mut self, id: WorkerId, now_ms: u64) {
        if let Some(last) = self.workers.get_mut(&id) {
            *last = now_ms;
        }
    }

    /// Advance the clock to `now_ms`.
    ///
    /// Returns the IDs of workers that have not sent a heartbeat within
    /// `failure_timeout_ms`.  Workers are NOT removed from the registry;
    /// callers must call `deregister` after handling the failure.
    pub fn tick(&self, now_ms: u64) -> Vec<WorkerId> {
        self.workers
            .iter()
            .filter_map(|(&id, &last_hb)| {
                let elapsed = now_ms.saturating_sub(last_hb);
                if elapsed > self.failure_timeout_ms {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove a worker from tracking (after its failure has been handled).
    pub fn deregister(&mut self, id: WorkerId) {
        self.workers.remove(&id);
    }

    /// Return the current status of a worker.
    pub fn status(&self, id: WorkerId, now_ms: u64) -> Option<WorkerStatus> {
        self.workers.get(&id).map(|&last_hb| {
            let elapsed = now_ms.saturating_sub(last_hb);
            if elapsed > self.failure_timeout_ms {
                WorkerStatus::Failed
            } else {
                WorkerStatus::Healthy
            }
        })
    }

    /// Number of registered workers.
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns `true` if no workers are registered.
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}

// ─── RecoveryStatus ──────────────────────────────────────────────────────────

/// Status of an active or completed shard recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStatus {
    /// No recovery is in progress.
    Healthy,
    /// Recovery started at `started_at_ms`; `shards_pending` shards not yet
    /// confirmed healthy by their new owners.
    Recovering {
        started_at_ms: u64,
        shards_reassigned: usize,
    },
    /// Recovery is still in progress after the `slow_threshold_ms` SLO.
    /// Surfaces `RS-1603`.
    RecoveringSlow {
        started_at_ms: u64,
        shards_reassigned: usize,
        elapsed_ms: u64,
    },
}

/// Result of handling a worker failure.
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// Worker that failed.
    pub failed_worker: WorkerId,
    /// Shards that were freed and reassigned.
    pub reassigned: Vec<ShardReassignment>,
    /// Time at which recovery started (ms).
    pub started_at_ms: u64,
}

/// A single shard reassignment.
#[derive(Debug, Clone)]
pub struct ShardReassignment {
    pub shard_id: ShardId,
    pub new_owner: WorkerId,
}

/// Recovery driver: detects worker failures and orchestrates shard recovery.
///
/// Intentionally kept synchronous and pure (no network, no async) so it can be
/// driven by the `SimRuntime` deterministic clock in tests.
pub struct RecoveryDriver {
    /// Milliseconds after which recovery is considered SLOW (RS-1603).
    pub slow_threshold_ms: u64,
    active_recoveries: Vec<ActiveRecovery>,
}

struct ActiveRecovery {
    started_at_ms: u64,
    shards_reassigned: usize,
    completed: bool,
}

impl RecoveryDriver {
    /// Create a driver with a given `slow_threshold_ms` (typically 60 000).
    pub fn new(slow_threshold_ms: u64) -> Self {
        Self {
            slow_threshold_ms,
            active_recoveries: Vec::new(),
        }
    }

    /// Handle a worker failure by recording the recovery event.
    ///
    /// The caller is responsible for invoking `ShardScheduler::on_worker_dead`
    /// and passing the resulting reassignments here.  This separation keeps
    /// `RecoveryDriver` independent of the control-plane scheduler so it can be
    /// unit-tested without a full scheduler setup.
    pub fn record_recovery(
        &mut self,
        failed_worker: WorkerId,
        reassigned_shards: Vec<ShardReassignment>,
        now_ms: u64,
    ) -> RecoveryResult {
        let count = reassigned_shards.len();
        self.active_recoveries.push(ActiveRecovery {
            started_at_ms: now_ms,
            shards_reassigned: count,
            completed: false,
        });
        RecoveryResult {
            failed_worker,
            reassigned: reassigned_shards,
            started_at_ms: now_ms,
        }
    }

    /// Mark recovery as complete (all reassigned shards have confirmed their
    /// first successful epoch commit).
    pub fn mark_complete(&mut self, started_at_ms: u64) {
        for rec in &mut self.active_recoveries {
            if rec.started_at_ms == started_at_ms && !rec.completed {
                rec.completed = true;
                break;
            }
        }
        self.active_recoveries.retain(|r| !r.completed);
    }

    /// Return the current recovery status at time `now_ms`.
    ///
    /// Returns `Healthy` if no active recoveries exist.  If any active recovery
    /// has been running longer than `slow_threshold_ms`, returns
    /// `RecoveringSlow` (RS-1603).  Otherwise returns `Recovering`.
    pub fn status(&self, now_ms: u64) -> RecoveryStatus {
        if self.active_recoveries.is_empty() {
            return RecoveryStatus::Healthy;
        }

        for rec in &self.active_recoveries {
            let elapsed = now_ms.saturating_sub(rec.started_at_ms);
            if elapsed > self.slow_threshold_ms {
                return RecoveryStatus::RecoveringSlow {
                    started_at_ms: rec.started_at_ms,
                    shards_reassigned: rec.shards_reassigned,
                    elapsed_ms: elapsed,
                };
            }
        }

        let oldest = self
            .active_recoveries
            .iter()
            .map(|r| r.started_at_ms)
            .min()
            .unwrap_or(now_ms);
        let total_shards: usize = self
            .active_recoveries
            .iter()
            .map(|r| r.shards_reassigned)
            .sum();

        RecoveryStatus::Recovering {
            started_at_ms: oldest,
            shards_reassigned: total_shards,
        }
    }

    /// Number of active (incomplete) recovery events.
    pub fn active_count(&self) -> usize {
        self.active_recoveries.len()
    }
}

// ─── ControlPlaneFence ───────────────────────────────────────────────────────

/// Worker self-fencing on control-plane partition (DESIGN.md §11.6).
///
/// A worker that cannot reach the control plane for more than
/// `fence_timeout_ms` must stop committing.  This prevents a network-partitioned
/// worker from writing to a shard whose lease has been revoked and reassigned to
/// a healthy worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceStatus {
    /// Worker is connected to the control plane.
    Connected,
    /// Worker has been partitioned for too long and must stop committing.
    Fenced,
}

/// Tracks control-plane contact time for worker self-fencing.
pub struct ControlPlaneFence {
    last_contact_ms: u64,
    fence_timeout_ms: u64,
}

impl ControlPlaneFence {
    /// Create a fence.  `fence_timeout_ms` is typically 30 000 (30 s) to give
    /// ample time for network recovery before fencing.
    pub fn new(fence_timeout_ms: u64, now_ms: u64) -> Self {
        Self {
            last_contact_ms: now_ms,
            fence_timeout_ms,
        }
    }

    /// Record successful contact with the control plane at `now_ms`.
    pub fn record_contact(&mut self, now_ms: u64) {
        self.last_contact_ms = now_ms;
    }

    /// Check whether the worker should fence itself.
    ///
    /// Returns `Fenced` if `now_ms - last_contact_ms > fence_timeout_ms`.
    pub fn check(&self, now_ms: u64) -> FenceStatus {
        let elapsed = now_ms.saturating_sub(self.last_contact_ms);
        if elapsed > self.fence_timeout_ms {
            FenceStatus::Fenced
        } else {
            FenceStatus::Connected
        }
    }

    /// Milliseconds since last control-plane contact.
    pub fn elapsed_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.last_contact_ms)
    }
}

// ─── ThrottledLeaseGranter ───────────────────────────────────────────────────

/// Rate-limited lease granter to prevent thundering herd on bulk restarts
/// (DESIGN.md §11.8).
///
/// When many workers restart simultaneously, each tries to (re)acquire leases
/// for all its shards at once.  Without throttling this causes a burst of
/// concurrent epoch replays that can overwhelm storage.
///
/// `ThrottledLeaseGranter` caps grants to `max_grants_per_window` per
/// `window_ms`.  `try_grant(now_ms)` returns `true` if a grant is allowed and
/// `false` if the rate limit is active.  The window resets at the start of each
/// new `window_ms` interval.
pub struct ThrottledLeaseGranter {
    max_grants_per_window: usize,
    window_ms: u64,
    window_start_ms: u64,
    grants_this_window: usize,
}

impl ThrottledLeaseGranter {
    /// Create a granter.
    ///
    /// - `max_grants_per_window`: maximum leases to grant in each window.
    /// - `window_ms`: duration of each rate-limit window in milliseconds.
    /// - `now_ms`: current time (starts the first window).
    pub fn new(max_grants_per_window: usize, window_ms: u64, now_ms: u64) -> Self {
        Self {
            max_grants_per_window,
            window_ms,
            window_start_ms: now_ms,
            grants_this_window: 0,
        }
    }

    /// Attempt to grant a lease at time `now_ms`.
    ///
    /// Returns `true` (grant allowed) if the window budget has not been
    /// exhausted.  Rolls the window when `now_ms >= window_start + window_ms`.
    pub fn try_grant(&mut self, now_ms: u64) -> bool {
        if now_ms >= self.window_start_ms + self.window_ms {
            let windows_elapsed = (now_ms - self.window_start_ms) / self.window_ms;
            self.window_start_ms += windows_elapsed * self.window_ms;
            self.grants_this_window = 0;
        }

        if self.grants_this_window < self.max_grants_per_window {
            self.grants_this_window += 1;
            true
        } else {
            false
        }
    }

    /// Grants remaining in the current window.
    pub fn grants_remaining(&self, now_ms: u64) -> usize {
        if now_ms >= self.window_start_ms + self.window_ms {
            self.max_grants_per_window
        } else {
            self.max_grants_per_window
                .saturating_sub(self.grants_this_window)
        }
    }

    /// Total grants allowed per window.
    pub fn max_grants_per_window(&self) -> usize {
        self.max_grants_per_window
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_detection_timeout() {
        let mut mon = WorkerHealthMonitor::new(5_000);
        mon.register(WorkerId(1), 0);

        // At 4999 ms: still healthy.
        let failed = mon.tick(4_999);
        assert!(
            failed.is_empty(),
            "must not fail before timeout: {failed:?}"
        );

        // At 5001 ms: failed.
        let failed = mon.tick(5_001);
        assert_eq!(failed, vec![WorkerId(1)]);
    }

    #[test]
    fn heartbeat_resets_timer() {
        let mut mon = WorkerHealthMonitor::new(5_000);
        mon.register(WorkerId(1), 0);

        // Near-miss: heartbeat at 4000ms resets the timer.
        mon.heartbeat(WorkerId(1), 4_000);

        // At 8999 ms (4999 ms after last heartbeat): still healthy.
        assert!(mon.tick(8_999).is_empty());

        // At 9001 ms (5001 ms after last heartbeat): failed.
        assert_eq!(mon.tick(9_001), vec![WorkerId(1)]);
    }

    #[test]
    fn recovery_slow_after_threshold() {
        let mut driver = RecoveryDriver::new(60_000);
        driver.record_recovery(
            WorkerId(1),
            vec![ShardReassignment {
                shard_id: ShardId(0),
                new_owner: WorkerId(2),
            }],
            0,
        );

        // At 59 999 ms: still Recovering.
        assert!(matches!(
            driver.status(59_999),
            RecoveryStatus::Recovering { .. }
        ));

        // At 60 001 ms: RecoveringSlow (RS-1603).
        assert!(matches!(
            driver.status(60_001),
            RecoveryStatus::RecoveringSlow { .. }
        ));
    }

    #[test]
    fn control_plane_fence_fires_after_timeout() {
        let fence = ControlPlaneFence::new(30_000, 0);
        assert_eq!(fence.check(29_999), FenceStatus::Connected);
        assert_eq!(fence.check(30_001), FenceStatus::Fenced);
    }

    #[test]
    fn throttled_granter_caps_burst() {
        let mut granter = ThrottledLeaseGranter::new(4, 1_000, 0);
        // First 4 grants allowed.
        assert!(granter.try_grant(0));
        assert!(granter.try_grant(0));
        assert!(granter.try_grant(0));
        assert!(granter.try_grant(0));
        // 5th is denied.
        assert!(!granter.try_grant(0));
        // New window: grants reset.
        assert!(granter.try_grant(1_001));
    }
}

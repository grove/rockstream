//! Shard lease management for the RockStream control plane.
//!
//! The [`ShardManager`] is the authoritative source of truth for which worker
//! holds a write lease on each shard. It issues monotonically increasing
//! fencing tokens that prevent stale writers from committing after a lease is
//! revoked or transferred.
//!
//! ## Fencing invariant
//!
//! For any shard S, the manager maintains an **epoch counter** that
//! monotonically increases on every `acquire` call. A writer that holds token
//! `T` for shard `S` is only permitted to commit if `is_valid_writer(S, T)`
//! returns `true`. Once a newer token `T' > T` is issued for `S`, every
//! attempt with `T` returns `false` — the old writer is fenced out.
//!
//! ## Worker death
//!
//! When a worker TCP connection is lost, `release_worker(worker_id)` atomically
//! removes all shard leases held by that worker and returns their IDs. The
//! caller (typically [`ControlService`]) then uses [`ShardScheduler`] to
//! reassign those shards to surviving healthy workers.
//!
//! [`ControlService`]: crate::service::ControlService

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;

/// Errors returned by [`ShardManager`] operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    /// The shard is currently leased by a *different* worker.
    #[error("RS-1701: shard {shard_id} is already leased by worker {holder}")]
    AlreadyLeased { shard_id: ShardId, holder: WorkerId },
    /// The provided fencing token does not match the current token.
    #[error(
        "RS-1702: stale lease token for shard {shard_id} (provided {provided}, current {current})"
    )]
    StaleToken {
        shard_id: ShardId,
        provided: LeaseToken,
        current: LeaseToken,
    },
    /// The shard has no active lease (not yet acquired).
    #[error("RS-1703: shard {shard_id} has no active lease")]
    NoLease { shard_id: ShardId },
}

struct ShardManagerInner {
    /// Active leases keyed by shard ID.
    leases: HashMap<ShardId, ShardLease>,
    /// Global monotonic counter; incremented on every `acquire`.
    next_token: u64,
}

/// Thread-safe manager for shard write leases.
///
/// All public methods are safe to call from multiple threads simultaneously.
#[derive(Clone)]
pub struct ShardManager {
    inner: Arc<RwLock<ShardManagerInner>>,
}

impl ShardManager {
    /// Create a new, empty `ShardManager`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ShardManagerInner {
                leases: HashMap::new(),
                next_token: 1,
            })),
        }
    }

    /// Acquire a write lease on `shard_id` for `worker_id`.
    ///
    /// - If the shard has no current lease, a new lease is issued.
    /// - If the shard is already leased by `worker_id`, the existing lease is
    ///   **renewed** with a new (higher) fencing token.
    /// - If the shard is leased by a *different* worker, returns
    ///   [`LeaseError::AlreadyLeased`].
    ///
    /// Each successful call increments the global fencing token counter so the
    /// returned [`LeaseToken`] is always strictly greater than every previously
    /// issued token for *any* shard.
    pub fn acquire(
        &self,
        shard_id: ShardId,
        worker_id: WorkerId,
    ) -> Result<ShardLease, LeaseError> {
        let mut guard = self.inner.write();
        if let Some(existing) = guard.leases.get(&shard_id) {
            if existing.worker_id != worker_id {
                return Err(LeaseError::AlreadyLeased {
                    shard_id,
                    holder: existing.worker_id,
                });
            }
        }
        let token = LeaseToken(guard.next_token);
        guard.next_token += 1;
        let lease = ShardLease::new(shard_id, worker_id, token);
        guard.leases.insert(shard_id, lease.clone());
        Ok(lease)
    }

    /// Force-acquire a write lease, evicting any current holder.
    ///
    /// Used by the control plane for rebalancing. Returns the new lease and,
    /// if a previous holder was evicted, its `WorkerId`.
    pub fn force_acquire(
        &self,
        shard_id: ShardId,
        worker_id: WorkerId,
    ) -> (ShardLease, Option<WorkerId>) {
        let mut guard = self.inner.write();
        let evicted = guard.leases.get(&shard_id).map(|l| l.worker_id);
        let token = LeaseToken(guard.next_token);
        guard.next_token += 1;
        let lease = ShardLease::new(shard_id, worker_id, token);
        guard.leases.insert(shard_id, lease.clone());
        (lease, evicted)
    }

    /// Release the lease for `shard_id` if the provided `token` matches.
    ///
    /// Returns `true` if the lease was released. Returns `false` if there is
    /// no lease or the token is stale.
    pub fn release(&self, shard_id: ShardId, token: LeaseToken) -> bool {
        let mut guard = self.inner.write();
        let valid = guard
            .leases
            .get(&shard_id)
            .map(|l| l.lease_token == token)
            .unwrap_or(false);
        if valid {
            guard.leases.remove(&shard_id);
        }
        valid
    }

    /// Release all shards held by `worker_id`.
    ///
    /// Returns the list of shard IDs that were released. This is called when a
    /// worker's TCP connection drops so the control plane can reassign its
    /// shards to healthy workers.
    pub fn release_worker(&self, worker_id: WorkerId) -> Vec<ShardId> {
        let mut guard = self.inner.write();
        let freed: Vec<ShardId> = guard
            .leases
            .iter()
            .filter(|(_, l)| l.worker_id == worker_id)
            .map(|(id, _)| *id)
            .collect();
        for shard_id in &freed {
            guard.leases.remove(shard_id);
        }
        freed
    }

    /// Check whether `token` is the **current** active writer for `shard_id`.
    ///
    /// This is the write fence: a worker must call this before committing a
    /// shard epoch. If it returns `false`, the commit must be aborted.
    pub fn is_valid_writer(&self, shard_id: ShardId, token: LeaseToken) -> bool {
        let guard = self.inner.read();
        guard
            .leases
            .get(&shard_id)
            .map(|l| l.lease_token == token)
            .unwrap_or(false)
    }

    /// Return a snapshot of all active leases.
    pub fn leases(&self) -> Vec<ShardLease> {
        self.inner.read().leases.values().cloned().collect()
    }

    /// Return the lease for a specific shard, if one exists.
    pub fn get(&self, shard_id: ShardId) -> Option<ShardLease> {
        self.inner.read().leases.get(&shard_id).cloned()
    }

    /// Return the number of currently active leases.
    pub fn len(&self) -> usize {
        self.inner.read().leases.len()
    }

    /// Return `true` if there are no active leases.
    pub fn is_empty(&self) -> bool {
        self.inner.read().leases.is_empty()
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{ShardId, WorkerId};

    fn mgr() -> ShardManager {
        ShardManager::new()
    }

    // -----------------------------------------------------------------------
    // Basic acquire / release
    // -----------------------------------------------------------------------

    #[test]
    fn acquire_new_shard_succeeds() {
        let m = mgr();
        let lease = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        assert_eq!(lease.shard_id, ShardId(1));
        assert_eq!(lease.worker_id, WorkerId(10));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn same_worker_can_reacquire() {
        let m = mgr();
        let l1 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        let l2 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        // Token must be strictly higher on reacquire.
        assert!(l2.lease_token.0 > l1.lease_token.0);
    }

    #[test]
    fn different_worker_cannot_acquire_held_shard() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        let err = m.acquire(ShardId(1), WorkerId(2)).unwrap_err();
        assert!(matches!(
            err,
            LeaseError::AlreadyLeased {
                shard_id: ShardId(1),
                holder: WorkerId(1),
            }
        ));
    }

    #[test]
    fn release_removes_lease() {
        let m = mgr();
        let lease = m.acquire(ShardId(5), WorkerId(3)).unwrap();
        assert!(m.release(ShardId(5), lease.lease_token));
        assert!(m.is_empty());
    }

    #[test]
    fn release_with_stale_token_fails() {
        let m = mgr();
        let l1 = m.acquire(ShardId(1), WorkerId(1)).unwrap();
        // Reacquire → new token.
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        // Old token can no longer release.
        assert!(!m.release(ShardId(1), l1.lease_token));
    }

    // -----------------------------------------------------------------------
    // Two-writer fence test
    //
    // This is the core proof required by v0.29: only the holder of the current
    // fencing token is permitted to commit.
    // -----------------------------------------------------------------------

    #[test]
    fn two_writer_fence_test() {
        let m = mgr();

        // Worker A acquires shard 1.
        let lease_a = m.acquire(ShardId(1), WorkerId(1)).unwrap();

        // Simulate Worker A being replaced: control plane force-acquires for
        // Worker B.  This evicts Worker A and issues a strictly higher token.
        let (lease_b, evicted) = m.force_acquire(ShardId(1), WorkerId(2));

        assert_eq!(evicted, Some(WorkerId(1)));
        assert!(lease_b.lease_token.0 > lease_a.lease_token.0);

        // Worker A's old token is now stale — it cannot commit.
        assert!(
            !m.is_valid_writer(ShardId(1), lease_a.lease_token),
            "Worker A must be fenced out after Worker B acquired the lease"
        );

        // Worker B's new token is valid — it can commit.
        assert!(
            m.is_valid_writer(ShardId(1), lease_b.lease_token),
            "Worker B must be the valid writer"
        );
    }

    // -----------------------------------------------------------------------
    // Worker death / reassignment
    // -----------------------------------------------------------------------

    #[test]
    fn worker_death_clears_all_its_leases() {
        let m = mgr();
        // Worker 1 owns shards 1, 2, 3.
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        m.acquire(ShardId(2), WorkerId(1)).unwrap();
        m.acquire(ShardId(3), WorkerId(1)).unwrap();
        // Worker 2 owns shard 4.
        m.acquire(ShardId(4), WorkerId(2)).unwrap();

        let freed = m.release_worker(WorkerId(1));
        assert_eq!(freed.len(), 3);
        assert!(!freed.contains(&ShardId(4)));
        assert_eq!(m.len(), 1); // Only Worker 2's shard remains.
    }

    #[test]
    fn worker_death_then_reassignment_issues_fresh_token() {
        let m = mgr();
        let old_lease = m.acquire(ShardId(1), WorkerId(1)).unwrap();

        // Worker 1 dies.
        let freed = m.release_worker(WorkerId(1));
        assert_eq!(freed, vec![ShardId(1)]);

        // Shard 1 is reassigned to Worker 2.
        let new_lease = m.acquire(ShardId(1), WorkerId(2)).unwrap();

        // New token must be strictly greater than the old token.
        assert!(
            new_lease.lease_token.0 > old_lease.lease_token.0,
            "reassignment must produce a fresh fencing token"
        );

        // Old token is now invalid.
        assert!(!m.is_valid_writer(ShardId(1), old_lease.lease_token));

        // New token is valid.
        assert!(m.is_valid_writer(ShardId(1), new_lease.lease_token));
    }

    // -----------------------------------------------------------------------
    // Force acquire / eviction
    // -----------------------------------------------------------------------

    #[test]
    fn force_acquire_with_no_existing_lease_has_no_eviction() {
        let m = mgr();
        let (lease, evicted) = m.force_acquire(ShardId(1), WorkerId(99));
        assert!(evicted.is_none());
        assert_eq!(lease.worker_id, WorkerId(99));
    }

    #[test]
    fn force_acquire_evicts_existing_holder() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(5)).unwrap();
        let (_, evicted) = m.force_acquire(ShardId(1), WorkerId(6));
        assert_eq!(evicted, Some(WorkerId(5)));
        // Shard now belongs to Worker 6.
        assert_eq!(m.get(ShardId(1)).unwrap().worker_id, WorkerId(6));
    }

    // -----------------------------------------------------------------------
    // Token monotonicity across shards
    // -----------------------------------------------------------------------

    #[test]
    fn tokens_are_globally_monotone() {
        let m = mgr();
        let t1 = m.acquire(ShardId(1), WorkerId(1)).unwrap().lease_token;
        let t2 = m.acquire(ShardId(2), WorkerId(2)).unwrap().lease_token;
        let t3 = m.acquire(ShardId(3), WorkerId(3)).unwrap().lease_token;
        assert!(t1.0 < t2.0);
        assert!(t2.0 < t3.0);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    #[test]
    fn leases_snapshot() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        m.acquire(ShardId(2), WorkerId(2)).unwrap();
        let snapshot = m.leases();
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn get_returns_none_for_unknown_shard() {
        let m = mgr();
        assert!(m.get(ShardId(99)).is_none());
    }
}

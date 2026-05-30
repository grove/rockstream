//! In-memory topology catalog for the RockStream control plane.
//!
//! The `TopologyCatalog` is the authoritative registry of all worker nodes
//! known to the cluster. It is maintained by the `ControlService` and read by
//! the placement algorithm and the gateway for routing.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use rockstream_types::ids::WorkerId;
use rockstream_types::topology::{CapacityHeadroom, WorkerInfo, WorkerRegistration};

/// Thread-safe, in-memory registry of all workers in the cluster.
#[derive(Debug, Clone)]
pub struct TopologyCatalog {
    inner: Arc<RwLock<CatalogInner>>,
}

#[derive(Debug, Default)]
struct CatalogInner {
    workers: HashMap<WorkerId, WorkerInfo>,
    /// Monotonically increasing version; bumped on every mutation.
    version: u64,
}

impl Default for TopologyCatalog {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(CatalogInner::default())),
        }
    }
}

impl TopologyCatalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a worker from its registration message.
    ///
    /// If the worker was already registered, its information is updated and
    /// the catalog version is bumped. Returns the assigned `WorkerId`.
    pub fn register(&self, reg: &WorkerRegistration) -> WorkerId {
        let info = WorkerInfo::from_registration(reg);
        let worker_id = info.worker_id;
        let mut guard = self.inner.write();
        guard.workers.insert(worker_id, info);
        guard.version += 1;
        worker_id
    }

    /// Update the `capacity_headroom` for a registered worker (heartbeat).
    ///
    /// Returns `true` if the worker was found; `false` otherwise.
    pub fn heartbeat(&self, worker_id: WorkerId, headroom: CapacityHeadroom) -> bool {
        let mut guard = self.inner.write();
        if let Some(info) = guard.workers.get_mut(&worker_id) {
            info.update_capacity(headroom);
            guard.version += 1;
            true
        } else {
            false
        }
    }

    /// Mark a worker as deregistered (removed from the catalog).
    ///
    /// Returns `Some(WorkerInfo)` of the removed entry, or `None` if the
    /// worker was not found.
    pub fn deregister(&self, worker_id: WorkerId) -> Option<WorkerInfo> {
        let mut guard = self.inner.write();
        let removed = guard.workers.remove(&worker_id);
        if removed.is_some() {
            guard.version += 1;
        }
        removed
    }

    /// Snapshot of all healthy workers.
    pub fn healthy_workers(&self) -> Vec<WorkerInfo> {
        let guard = self.inner.read();
        guard
            .workers
            .values()
            .filter(|w| w.healthy)
            .cloned()
            .collect()
    }

    /// Snapshot of all workers (including unhealthy).
    pub fn all_workers(&self) -> Vec<WorkerInfo> {
        let guard = self.inner.read();
        guard.workers.values().cloned().collect()
    }

    /// Look up a single worker by ID.
    pub fn get(&self, worker_id: WorkerId) -> Option<WorkerInfo> {
        let guard = self.inner.read();
        guard.workers.get(&worker_id).cloned()
    }

    /// Current catalog version (bumped on every mutation).
    pub fn version(&self) -> u64 {
        self.inner.read().version
    }

    /// Number of registered workers.
    pub fn len(&self) -> usize {
        self.inner.read().workers.len()
    }

    /// Returns `true` if no workers are registered.
    pub fn is_empty(&self) -> bool {
        self.inner.read().workers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::topology::{CapacityHeadroom, NodeRole, WorkerRegistration};

    fn make_reg(id: u64, headroom: f64) -> WorkerRegistration {
        WorkerRegistration::new(
            WorkerId(id),
            NodeRole::Worker,
            format!("127.0.0.1:{}", 7000 + id),
            CapacityHeadroom::new(headroom),
        )
    }

    #[test]
    fn register_and_lookup() {
        let cat = TopologyCatalog::new();
        let reg = make_reg(1, 0.8);
        let wid = cat.register(&reg);
        assert_eq!(wid, WorkerId(1));

        let info = cat.get(WorkerId(1)).unwrap();
        assert_eq!(info.address, "127.0.0.1:7001");
        assert!(info.healthy);
        assert_eq!(info.capacity_headroom.fraction(), 0.8);
    }

    #[test]
    fn version_bumps_on_each_mutation() {
        let cat = TopologyCatalog::new();
        assert_eq!(cat.version(), 0);
        cat.register(&make_reg(1, 0.9));
        assert_eq!(cat.version(), 1);
        cat.heartbeat(WorkerId(1), CapacityHeadroom::new(0.5));
        assert_eq!(cat.version(), 2);
        cat.deregister(WorkerId(1));
        assert_eq!(cat.version(), 3);
    }

    #[test]
    fn deregister_removes_worker() {
        let cat = TopologyCatalog::new();
        cat.register(&make_reg(2, 0.7));
        assert_eq!(cat.len(), 1);
        let removed = cat.deregister(WorkerId(2));
        assert!(removed.is_some());
        assert_eq!(cat.len(), 0);
    }

    #[test]
    fn heartbeat_missing_worker_returns_false() {
        let cat = TopologyCatalog::new();
        assert!(!cat.heartbeat(WorkerId(99), CapacityHeadroom::FULL));
    }

    #[test]
    fn healthy_workers_filters_correctly() {
        let cat = TopologyCatalog::new();
        cat.register(&make_reg(1, 0.9));
        cat.register(&make_reg(2, 0.6));
        // Manually mark one unhealthy
        {
            let mut guard = cat.inner.write();
            guard.workers.get_mut(&WorkerId(2)).unwrap().healthy = false;
        }
        let healthy = cat.healthy_workers();
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].worker_id, WorkerId(1));
    }

    #[test]
    fn multiple_registrations_update_existing() {
        let cat = TopologyCatalog::new();
        cat.register(&make_reg(3, 0.5));
        // Re-register with higher headroom
        let reg2 = WorkerRegistration::new(
            WorkerId(3),
            NodeRole::Worker,
            "127.0.0.1:7003",
            CapacityHeadroom::new(0.95),
        );
        cat.register(&reg2);
        assert_eq!(cat.len(), 1);
        let info = cat.get(WorkerId(3)).unwrap();
        assert_eq!(info.capacity_headroom.fraction(), 0.95);
    }
}

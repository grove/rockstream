//! Topology types for the RockStream cluster control plane.
//!
//! These types are used by both the control-plane service and by worker nodes
//! to describe cluster membership, roles, and capacity.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ids::{ShardId, WorkerId};
use crate::lease::{ShardLease, ShardRevokeReason};

/// The role a node is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Runs all roles in a single process (embedded / development profile).
    All,
    /// Pure worker node: executes operator tasks and owns shards.
    Worker,
    /// Control-plane node: topology catalog, placement, lifecycle.
    Control,
    /// Gateway node: accepts SQL and pgwire connections.
    Gateway,
    /// Frontier coordinator node.
    Frontier,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::All => write!(f, "all"),
            NodeRole::Worker => write!(f, "worker"),
            NodeRole::Control => write!(f, "control"),
            NodeRole::Gateway => write!(f, "gateway"),
            NodeRole::Frontier => write!(f, "frontier"),
        }
    }
}

/// Fraction of available capacity on a worker node, in [0.0, 1.0].
///
/// 1.0 means the worker is completely idle; 0.0 means it is saturated.
/// The placement algorithm prefers workers with higher `capacity_headroom`
/// when assigning shards or operator instances.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct CapacityHeadroom(pub f64);

impl CapacityHeadroom {
    /// Fully available (no load).
    pub const FULL: Self = Self(1.0);

    /// Saturated (no headroom).
    pub const EMPTY: Self = Self(0.0);

    /// Create a new `CapacityHeadroom`, clamped to [0.0, 1.0].
    pub fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    /// Returns the raw fraction.
    pub fn fraction(&self) -> f64 {
        self.0
    }
}

impl Default for CapacityHeadroom {
    fn default() -> Self {
        Self::FULL
    }
}

impl std::fmt::Display for CapacityHeadroom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// Registration request sent by a worker to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    /// The worker's proposed identity (may be overridden by control plane).
    pub worker_id: WorkerId,
    /// The role this worker is running.
    pub role: NodeRole,
    /// The advertised address for peer connections (`host:port`).
    pub address: String,
    /// Current capacity headroom at the time of registration.
    pub capacity_headroom: CapacityHeadroom,
    /// Wall-clock timestamp (ms since Unix epoch) when the registration was
    /// sent.
    pub registered_at_ms: u64,
}

impl WorkerRegistration {
    /// Build a registration for a worker joining at `address`.
    pub fn new(
        worker_id: WorkerId,
        role: NodeRole,
        address: impl Into<String>,
        capacity_headroom: CapacityHeadroom,
    ) -> Self {
        let registered_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            worker_id,
            role,
            address: address.into(),
            capacity_headroom,
            registered_at_ms,
        }
    }
}

/// A snapshot of a worker's state as known to the topology catalog.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerInfo {
    /// Unique identifier assigned / confirmed by the control plane.
    pub worker_id: WorkerId,
    /// Role flags for this node.
    pub role: NodeRole,
    /// Advertised network address (`host:port`).
    pub address: String,
    /// Most recently reported capacity headroom.
    pub capacity_headroom: CapacityHeadroom,
    /// When this worker registered (ms since Unix epoch).
    pub registered_at_ms: u64,
    /// Whether the worker is currently considered healthy.
    pub healthy: bool,
}

impl WorkerInfo {
    /// Construct a `WorkerInfo` from a `WorkerRegistration`.
    pub fn from_registration(reg: &WorkerRegistration) -> Self {
        Self {
            worker_id: reg.worker_id,
            role: reg.role,
            address: reg.address.clone(),
            capacity_headroom: reg.capacity_headroom,
            registered_at_ms: reg.registered_at_ms,
            healthy: true,
        }
    }

    /// Update the capacity headroom from a heartbeat.
    pub fn update_capacity(&mut self, headroom: CapacityHeadroom) {
        self.capacity_headroom = headroom;
    }
}

/// A control-plane message sent from the control service to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlMessage {
    /// Acknowledgement of successful worker registration.
    Registered {
        /// The canonical worker ID assigned by the control plane.
        worker_id: WorkerId,
    },
    /// The topology has changed; the worker should update its view.
    TopologyChanged {
        /// Current list of healthy workers.
        workers: Vec<WorkerInfo>,
    },
    /// Instructs the worker to stop gracefully.
    Shutdown,
    /// The control plane has assigned a shard lease to this worker.
    ///
    /// The worker must not write to `lease.shard_id` unless it holds the
    /// current [`LeaseToken`].
    ShardAssigned {
        /// The new lease (includes shard_id, worker_id, lease_token).
        lease: ShardLease,
    },
    /// The control plane has revoked a previously assigned shard lease.
    ///
    /// The worker must stop writing to `shard_id` immediately and discard
    /// any buffered writes associated with the old token.
    ShardRevoked {
        /// The shard whose lease was revoked.
        shard_id: ShardId,
        /// Why the lease was revoked.
        reason: ShardRevokeReason,
    },
    /// Response to a [`WorkerMessage::FenceWrite`] request: confirms whether
    /// the given [`LeaseToken`] is still the current writer for `shard_id`.
    FenceAck {
        /// The shard that was fenced.
        shard_id: ShardId,
        /// `true` if `lease_token` is the current active token.
        valid: bool,
    },
}

/// A message sent from a worker to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    /// Initial registration request.
    Register(WorkerRegistration),
    /// Periodic heartbeat with updated capacity.
    Heartbeat {
        worker_id: WorkerId,
        capacity_headroom: CapacityHeadroom,
    },
    /// Graceful deregistration.
    Deregister { worker_id: WorkerId },
    /// Request the control plane to acquire a shard lease on behalf of this
    /// worker. The control plane responds with [`ControlMessage::ShardAssigned`]
    /// or an error (connection close).
    RequestShard {
        worker_id: WorkerId,
        shard_id: ShardId,
    },
    /// Ask the control plane whether this token is still the active writer.
    /// Used by a worker before committing an epoch to double-check the fence.
    FenceWrite {
        shard_id: ShardId,
        lease_token: crate::ids::LeaseToken,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::WorkerId;

    #[test]
    fn capacity_headroom_clamps() {
        assert_eq!(CapacityHeadroom::new(1.5).0, 1.0);
        assert_eq!(CapacityHeadroom::new(-0.1).0, 0.0);
        assert_eq!(CapacityHeadroom::new(0.75).0, 0.75);
    }

    #[test]
    fn worker_registration_roundtrip() {
        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:7001",
            CapacityHeadroom::new(0.8),
        );
        let json = serde_json::to_string(&reg).unwrap();
        let decoded: WorkerRegistration = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.worker_id, WorkerId(1));
        assert_eq!(decoded.address, "127.0.0.1:7001");
    }

    #[test]
    fn worker_message_register_roundtrip() {
        let reg = WorkerRegistration::new(
            WorkerId(2),
            NodeRole::Worker,
            "127.0.0.1:7002",
            CapacityHeadroom::FULL,
        );
        let msg = WorkerMessage::Register(reg);
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: WorkerMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            WorkerMessage::Register(r) => assert_eq!(r.worker_id, WorkerId(2)),
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn control_message_registered_roundtrip() {
        let msg = ControlMessage::Registered {
            worker_id: WorkerId(3),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let decoded: ControlMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            ControlMessage::Registered { worker_id } => {
                assert_eq!(worker_id, WorkerId(3))
            }
            _ => panic!("unexpected message variant"),
        }
    }

    #[test]
    fn node_role_display() {
        assert_eq!(NodeRole::Control.to_string(), "control");
        assert_eq!(NodeRole::Worker.to_string(), "worker");
        assert_eq!(NodeRole::All.to_string(), "all");
    }

    #[test]
    fn worker_info_from_registration() {
        let reg = WorkerRegistration::new(
            WorkerId(5),
            NodeRole::Worker,
            "10.0.0.1:7005",
            CapacityHeadroom::new(0.6),
        );
        let info = WorkerInfo::from_registration(&reg);
        assert_eq!(info.worker_id, WorkerId(5));
        assert!(info.healthy);
        assert_eq!(info.capacity_headroom.fraction(), 0.6);
    }
}

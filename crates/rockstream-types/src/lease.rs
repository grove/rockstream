//! Shard lease types for distributed shard ownership in RockStream.
//!
//! A shard lease is the control-plane record that grants a specific worker the
//! right to write to a shard. The [`LeaseToken`] is a monotonically increasing
//! fencing token: a writer holding an older token is fenced out the moment a
//! newer token is issued for the same shard.
//!
//! ## Fencing invariant
//!
//! For any shard S:
//! - At most one worker holds the **current** lease at any time.
//! - A commit attempt with a stale [`LeaseToken`] (lower than the current) is
//!   rejected with [`RS-1601`].
//! - Re-acquiring a shard after a worker recovers produces a strictly higher
//!   token than any token that existed for that shard — including tokens that
//!   were active when the worker died.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ids::{LeaseToken, ShardId, WorkerId};

/// The reason a shard lease was revoked by the control plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShardRevokeReason {
    /// The lease holder's TCP connection was lost; the worker is presumed dead.
    WorkerDead,
    /// The control plane preempted the lease for rebalancing.
    ControlPlanePreempt,
}

impl std::fmt::Display for ShardRevokeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardRevokeReason::WorkerDead => write!(f, "worker_dead"),
            ShardRevokeReason::ControlPlanePreempt => write!(f, "control_plane_preempt"),
        }
    }
}

/// A lease granting a specific worker the right to write to a shard.
///
/// The [`lease_token`][ShardLease::lease_token] is a monotonically increasing
/// fencing token.  When the control plane grants a new lease for a shard, the
/// new token is always strictly greater than every previous token for that
/// shard — this ensures stale writers are always detectable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardLease {
    /// The shard this lease covers.
    pub shard_id: ShardId,
    /// The worker that holds this lease.
    pub worker_id: WorkerId,
    /// Monotonically increasing fencing token.
    ///
    /// The shard manager increments a global counter on every new lease
    /// acquisition so tokens are globally ordered across all shards and all
    /// workers.
    pub lease_token: LeaseToken,
    /// Wall-clock timestamp (ms since Unix epoch) when the lease was acquired.
    pub acquired_at_ms: u64,
}

impl ShardLease {
    /// Construct a new `ShardLease`.
    pub fn new(shard_id: ShardId, worker_id: WorkerId, lease_token: LeaseToken) -> Self {
        let acquired_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            shard_id,
            worker_id,
            lease_token,
            acquired_at_ms,
        }
    }
}

impl std::fmt::Display for ShardLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shard={} worker={} token={}",
            self.shard_id, self.worker_id, self.lease_token
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{LeaseToken, ShardId, WorkerId};

    #[test]
    fn shard_lease_display() {
        let lease = ShardLease::new(ShardId(3), WorkerId(7), LeaseToken(42));
        let s = lease.to_string();
        assert!(s.contains("shard-3"));
        assert!(s.contains("worker-7"));
        assert!(s.contains("lease-42"));
    }

    #[test]
    fn shard_lease_roundtrip() {
        let lease = ShardLease::new(ShardId(1), WorkerId(2), LeaseToken(99));
        let json = serde_json::to_string(&lease).unwrap();
        let back: ShardLease = serde_json::from_str(&json).unwrap();
        assert_eq!(lease.shard_id, back.shard_id);
        assert_eq!(lease.worker_id, back.worker_id);
        assert_eq!(lease.lease_token, back.lease_token);
    }

    #[test]
    fn shard_revoke_reason_display() {
        assert_eq!(ShardRevokeReason::WorkerDead.to_string(), "worker_dead");
        assert_eq!(
            ShardRevokeReason::ControlPlanePreempt.to_string(),
            "control_plane_preempt"
        );
    }
}

//! Placement algorithm for RockStream operator and shard assignment.
//!
//! The placement algorithm decides which worker should receive a new shard or
//! operator instance. It respects the `capacity_headroom` reported by each
//! worker: workers with more headroom are preferred.

use rockstream_types::ids::WorkerId;
use rockstream_types::topology::WorkerInfo;

/// Result of a placement decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementDecision {
    /// The worker chosen to host the workload.
    pub worker_id: WorkerId,
    /// The headroom fraction that influenced this decision.
    pub headroom_at_decision: u32,
}

/// Placement algorithm: prefer workers with the highest capacity headroom.
///
/// When multiple workers have equal headroom, the one with the lowest
/// `WorkerId` is chosen for deterministic tie-breaking.
pub struct PlacementAlgorithm;

impl PlacementAlgorithm {
    /// Choose the best worker from `candidates` for a new placement.
    ///
    /// Returns `None` if `candidates` is empty.
    pub fn choose(candidates: &[WorkerInfo]) -> Option<&WorkerInfo> {
        candidates.iter().max_by(|a, b| {
            // Compare headroom (higher is better), break ties by worker_id
            // (lower is better for determinism).
            let ha = (a.capacity_headroom.fraction() * 1_000_000.0) as u64;
            let hb = (b.capacity_headroom.fraction() * 1_000_000.0) as u64;
            ha.cmp(&hb).then_with(|| b.worker_id.0.cmp(&a.worker_id.0))
        })
    }

    /// Assign `n` distinct workers to host `n` units of work (e.g. shards).
    ///
    /// Workers are sorted by descending headroom. If fewer than `n` workers
    /// are available, all available workers are returned.
    pub fn assign_n(candidates: &[WorkerInfo], n: usize) -> Vec<WorkerId> {
        let mut sorted: Vec<&WorkerInfo> = candidates.iter().collect();
        sorted.sort_by(|a, b| {
            let ha = (a.capacity_headroom.fraction() * 1_000_000.0) as u64;
            let hb = (b.capacity_headroom.fraction() * 1_000_000.0) as u64;
            hb.cmp(&ha).then_with(|| a.worker_id.0.cmp(&b.worker_id.0))
        });
        sorted.iter().take(n).map(|w| w.worker_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{CapacityHeadroom, NodeRole, WorkerInfo, WorkerLifecycleState};

    fn make_worker(id: u64, headroom: f64) -> WorkerInfo {
        WorkerInfo {
            worker_id: WorkerId(id),
            role: NodeRole::Worker,
            address: format!("127.0.0.1:{}", 7000 + id),
            capacity_headroom: CapacityHeadroom::new(headroom),
            registered_at_ms: 0,
            healthy: true,
            lifecycle: WorkerLifecycleState::Active,
        }
    }

    #[test]
    fn choose_highest_headroom() {
        let workers = vec![
            make_worker(1, 0.3),
            make_worker(2, 0.8),
            make_worker(3, 0.5),
        ];
        let chosen = PlacementAlgorithm::choose(&workers).unwrap();
        assert_eq!(chosen.worker_id, WorkerId(2));
    }

    #[test]
    fn choose_tie_breaks_by_lowest_id() {
        let workers = vec![
            make_worker(5, 0.8),
            make_worker(2, 0.8),
            make_worker(9, 0.8),
        ];
        let chosen = PlacementAlgorithm::choose(&workers).unwrap();
        assert_eq!(chosen.worker_id, WorkerId(2));
    }

    #[test]
    fn choose_empty_returns_none() {
        assert!(PlacementAlgorithm::choose(&[]).is_none());
    }

    #[test]
    fn assign_n_respects_headroom_order() {
        let workers = vec![
            make_worker(1, 0.3),
            make_worker(2, 0.9),
            make_worker(3, 0.7),
            make_worker(4, 0.5),
        ];
        let assigned = PlacementAlgorithm::assign_n(&workers, 2);
        assert_eq!(assigned, vec![WorkerId(2), WorkerId(3)]);
    }

    #[test]
    fn assign_n_capped_at_available() {
        let workers = vec![make_worker(1, 0.5), make_worker(2, 0.9)];
        let assigned = PlacementAlgorithm::assign_n(&workers, 10);
        assert_eq!(assigned.len(), 2);
    }

    #[test]
    fn placement_respects_capacity_headroom() {
        // The placement algorithm must prefer the worker reporting higher
        // capacity_headroom — this is the core v0.28 proof criterion.
        let workers = vec![make_worker(10, 0.1), make_worker(20, 0.95)];
        let chosen = PlacementAlgorithm::choose(&workers).unwrap();
        assert_eq!(chosen.worker_id, WorkerId(20));
    }
}

//! Liveness checks tied to recovery SLOs (v0.36).
//!
//! Every recoverable injected fault must either commit a new epoch within
//! the 5 s / 30 s / 60 s budgets or surface a **named degraded state**.
//! This module provides the named states and the checker used by the
//! simulation suite to verify the invariant holds.

/// Named degraded pipeline states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DegradedState {
    /// `RS-1603` — recovery active for > 60 s; pipeline freshness behind SLO.
    RecoveringSlow { elapsed_ms: u64 },
    /// `RS-3003` — object store brownout; pipeline blocked due to buffer exhaustion.
    StorageStalled,
    /// Epoch frontier has not advanced within the expected freshness window.
    FrontierStalled { stalled_for_ms: u64 },
}

/// Overall pipeline liveness status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivenessStatus {
    /// All SLOs met; no degraded states active.
    Healthy,
    /// Pipeline is degraded but still making progress or awaiting recovery.
    Degraded(DegradedState),
    /// Pipeline is unavailable.
    Unavailable,
}

/// Checks pipeline liveness against the v0.35 recovery SLOs.
pub struct LivenessChecker {
    /// Milliseconds a recovery can be active before `RecoveringSlow` fires.
    recovery_slow_threshold_ms: u64,
    /// Milliseconds the frontier can stall before `FrontierStalled` fires.
    frontier_stall_threshold_ms: u64,
}

impl LivenessChecker {
    pub fn new(recovery_slow_threshold_ms: u64, frontier_stall_threshold_ms: u64) -> Self {
        Self {
            recovery_slow_threshold_ms,
            frontier_stall_threshold_ms,
        }
    }

    /// Check liveness given the current runtime state.
    ///
    /// Priority (highest first):
    /// 1. Object store stall (`RS-3003`)
    /// 2. Slow recovery (`RS-1603`)
    /// 3. Frontier stall
    pub fn check(
        &self,
        recovery_active_for_ms: Option<u64>,
        storage_stalled: bool,
        frontier_stalled_for_ms: Option<u64>,
    ) -> LivenessStatus {
        if storage_stalled {
            return LivenessStatus::Degraded(DegradedState::StorageStalled);
        }
        if let Some(elapsed) = recovery_active_for_ms {
            if elapsed > self.recovery_slow_threshold_ms {
                return LivenessStatus::Degraded(DegradedState::RecoveringSlow {
                    elapsed_ms: elapsed,
                });
            }
        }
        if let Some(stalled) = frontier_stalled_for_ms {
            if stalled > self.frontier_stall_threshold_ms {
                return LivenessStatus::Degraded(DegradedState::FrontierStalled {
                    stalled_for_ms: stalled,
                });
            }
        }
        LivenessStatus::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checker() -> LivenessChecker {
        LivenessChecker::new(60_000, 30_000)
    }

    #[test]
    fn healthy_when_all_nominal() {
        assert_eq!(checker().check(None, false, None), LivenessStatus::Healthy);
    }

    #[test]
    fn storage_stall_is_highest_priority() {
        // Even with a slow recovery, storage stall takes priority.
        let status = checker().check(Some(100_000), true, None);
        assert_eq!(
            status,
            LivenessStatus::Degraded(DegradedState::StorageStalled)
        );
    }

    #[test]
    fn recovering_slow_fires_after_threshold() {
        let status = checker().check(Some(60_001), false, None);
        assert_eq!(
            status,
            LivenessStatus::Degraded(DegradedState::RecoveringSlow { elapsed_ms: 60_001 })
        );
    }

    #[test]
    fn recovering_slow_does_not_fire_at_threshold() {
        assert_eq!(
            checker().check(Some(60_000), false, None),
            LivenessStatus::Healthy
        );
    }

    #[test]
    fn frontier_stall_surfaces_named_state() {
        let status = checker().check(None, false, Some(30_001));
        assert_eq!(
            status,
            LivenessStatus::Degraded(DegradedState::FrontierStalled {
                stalled_for_ms: 30_001
            })
        );
    }
}

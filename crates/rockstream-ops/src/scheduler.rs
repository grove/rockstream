//! Cooperative scheduler configuration and yield-ratio metric (DESIGN.md §9.3).
//!
//! Workers run operator tasks as tokio async tasks on a shared thread pool. A
//! large recomputation can hold the tokio executor long enough to starve
//! heartbeat sends. To prevent this, every operator loop is bounded by a
//! **records-per-quantum** limit. When an operator has more work remaining
//! after consuming its quantum, it yields via `tokio::task::yield_now()`.
//!
//! The `scheduler_yield_ratio` metric (fraction of epochs that hit the quantum
//! limit) is the observable proof that cooperative scheduling is active.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Per-pipeline configuration for the cooperative scheduler.
///
/// All values have sensible defaults so callers that don't care can use
/// `SchedulerConfig::default()`.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum number of rows an operator processes per tokio poll quantum.
    ///
    /// When an input epoch contains more rows than this limit, the operator
    /// task processes `max_rows_per_quantum` rows, emits a partial output,
    /// calls `tokio::task::yield_now()`, and is re-scheduled. This prevents
    /// one expensive epoch from starving heartbeat sends and frontier reports.
    ///
    /// Default: 65536 (per DESIGN.md §9.3).
    pub max_rows_per_quantum: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_rows_per_quantum: 65_536,
        }
    }
}

/// Shared counters for the `scheduler_yield_ratio` metric.
///
/// Cloning is cheap (Arc-backed). The same `YieldCounter` can be shared
/// between the operator task and any monitoring path.
#[derive(Debug, Default, Clone)]
pub struct YieldCounter {
    inner: Arc<YieldCounterInner>,
}

#[derive(Debug, Default)]
struct YieldCounterInner {
    /// Total number of epochs for which a ProcessDelta was dispatched.
    epoch_count: AtomicU64,
    /// Number of those epochs where the quantum limit was hit (yield occurred).
    yield_epoch_count: AtomicU64,
}

impl YieldCounter {
    /// Create a new counter starting at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that one epoch was processed.
    pub fn record_epoch(&self) {
        self.inner.epoch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this epoch hit the quantum limit and caused a yield.
    ///
    /// Call this at most once per epoch (multiple yields within a single
    /// epoch still count as one yield-epoch in the ratio).
    pub fn record_yield(&self) {
        self.inner.yield_epoch_count.fetch_add(1, Ordering::Relaxed);
    }

    /// `scheduler_yield_ratio`: fraction of epochs that hit the quantum limit.
    ///
    /// Returns 0.0 if no epochs have been processed yet.
    /// Returns 1.0 if every epoch caused at least one yield.
    pub fn yield_ratio(&self) -> f64 {
        let total = self.inner.epoch_count.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let yields = self.inner.yield_epoch_count.load(Ordering::Relaxed);
        yields as f64 / total as f64
    }

    /// Total epoch count seen by this counter.
    pub fn epoch_count(&self) -> u64 {
        self.inner.epoch_count.load(Ordering::Relaxed)
    }

    /// Number of epochs that caused at least one yield.
    pub fn yield_epoch_count(&self) -> u64 {
        self.inner.yield_epoch_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_quantum_is_65536() {
        let cfg = SchedulerConfig::default();
        assert_eq!(cfg.max_rows_per_quantum, 65_536);
    }

    #[test]
    fn yield_ratio_zero_before_any_epoch() {
        let counter = YieldCounter::new();
        assert_eq!(counter.yield_ratio(), 0.0);
    }

    #[test]
    fn yield_ratio_zero_when_no_yields() {
        let counter = YieldCounter::new();
        counter.record_epoch();
        counter.record_epoch();
        assert_eq!(counter.yield_ratio(), 0.0);
        assert_eq!(counter.epoch_count(), 2);
        assert_eq!(counter.yield_epoch_count(), 0);
    }

    #[test]
    fn yield_ratio_one_when_all_epochs_yield() {
        let counter = YieldCounter::new();
        counter.record_epoch();
        counter.record_yield();
        counter.record_epoch();
        counter.record_yield();
        assert_eq!(counter.yield_ratio(), 1.0);
    }

    #[test]
    fn yield_ratio_half_when_half_epochs_yield() {
        let counter = YieldCounter::new();
        counter.record_epoch();
        counter.record_yield();
        counter.record_epoch(); // no yield
        assert!((counter.yield_ratio() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn yield_counter_clone_shares_state() {
        let c1 = YieldCounter::new();
        let c2 = c1.clone();
        c1.record_epoch();
        c1.record_yield();
        // c2 shares the same Arc
        assert_eq!(c2.epoch_count(), 1);
        assert_eq!(c2.yield_epoch_count(), 1);
    }
}

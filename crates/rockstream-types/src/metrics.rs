//! Merge-law metric counters (IVM-0, DESIGN.md §6.11).
//!
//! Every code path that invokes a merge law increments `merge_law_applied_total`.
//! Code paths that fall back to a safe default (e.g., on parse error) increment
//! `merge_law_fallback_total`. Later phases wire these into a Prometheus registry;
//! for now they are process-global atomics that tests and diagnostics can read.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use crate::merge_law::MergeLawId;

/// Key for a per-law metric bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LawMetricKey {
    pub law_id: MergeLawId,
    pub law_name: &'static str,
    pub law_version: u16,
}

/// A single atomic counter for one metric key.
struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Global registry for merge-law metric counters.
struct MetricRegistry {
    applied: HashMap<LawMetricKey, Counter>,
    fallback: HashMap<LawMetricKey, Counter>,
}

impl MetricRegistry {
    fn new() -> Self {
        Self {
            applied: HashMap::new(),
            fallback: HashMap::new(),
        }
    }
}

static REGISTRY: LazyLock<Mutex<MetricRegistry>> =
    LazyLock::new(|| Mutex::new(MetricRegistry::new()));

fn with_registry<F, R>(f: F) -> R
where
    F: FnOnce(&mut MetricRegistry) -> R,
{
    let mut guard = REGISTRY.lock().expect("merge law metrics mutex poisoned");
    f(&mut guard)
}

/// Increment `merge_law_applied_total` for the given law.
pub fn inc_applied(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.applied
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Increment `merge_law_fallback_total` for the given law.
pub fn inc_fallback(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.fallback
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Read the `merge_law_applied_total` counter for a law (for tests/diagnostics).
pub fn read_applied(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.applied.get(key).map(|c| c.get()).unwrap_or(0))
}

/// Read the `merge_law_fallback_total` counter for a law (for tests/diagnostics).
pub fn read_fallback(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.fallback.get(key).map(|c| c.get()).unwrap_or(0))
}

/// Reset all counters to zero.
///
/// For use in tests only. Calling this from production code has no effect on
/// correctness but loses metric history.
#[doc(hidden)]
pub fn reset_all() {
    with_registry(|reg| {
        reg.applied.clear();
        reg.fallback.clear();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::MergeLawId;
    use std::sync::{LazyLock, Mutex};

    /// Serialise all tests that touch the process-global REGISTRY so that
    /// concurrent test threads don't corrupt each other's `reset_all` / `inc`
    /// sequences.
    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn key() -> LawMetricKey {
        LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
        }
    }

    #[test]
    fn applied_counter_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = key();
        assert_eq!(read_applied(&k), 0);
        inc_applied(&k);
        inc_applied(&k);
        assert_eq!(read_applied(&k), 2);
    }

    #[test]
    fn fallback_counter_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = key();
        assert_eq!(read_fallback(&k), 0);
        inc_fallback(&k);
        assert_eq!(read_fallback(&k), 1);
    }

    #[test]
    fn independent_counters_per_law() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k1 = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
        };
        let k2 = LawMetricKey {
            law_id: MergeLawId(0x0002),
            law_name: "SumCount",
            law_version: 1,
        };
        inc_applied(&k1);
        inc_applied(&k1);
        inc_fallback(&k2);
        assert_eq!(read_applied(&k1), 2);
        assert_eq!(read_applied(&k2), 0);
        assert_eq!(read_fallback(&k1), 0);
        assert_eq!(read_fallback(&k2), 1);
    }
}

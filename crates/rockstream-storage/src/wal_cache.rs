//! Per-shard WAL listing cache (DESIGN.md §9.1).
//!
//! WAL listing is expensive at high retention: `WalReader` documents that
//! listing thousands of WAL files is costly. Every worker keeps a per-shard
//! `WalListingCache`, invalidated only on WAL rotation, and tails via
//! object-store reads (not LIST calls) in the hot path.
//!
//! # Usage
//!
//! ```rust
//! use rockstream_storage::wal_cache::WalListingCache;
//!
//! let cache = WalListingCache::new();
//! // Initial mount: perform one object-store LIST call and populate the cache.
//! let initial_files = vec!["wal/0001.log".to_string(), "wal/0002.log".to_string()];
//! cache.populate(initial_files);
//!
//! // Hot path: get entries from memory — no LIST call.
//! let files = cache.get_cached_entries();
//! assert_eq!(cache.list_call_count(), 1);
//! ```

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

/// Per-shard cache for WAL file listings.
///
/// Workers populate this cache once on mount (incurring exactly one object-
/// store LIST call) and then serve all hot-path read requests from memory.
/// The cache is invalidated on WAL rotation; the next `populate` call records
/// the fresh listing and increments `list_call_count` by one.
///
/// `list_call_count` is exposed so tests can assert that the hot path issues
/// zero additional LIST calls after the initial mount.
#[derive(Debug)]
pub struct WalListingCache {
    entries: Mutex<Vec<String>>,
    /// Monotonic count of `populate` calls (each represents one LIST operation).
    list_call_count: AtomicU64,
}

impl Default for WalListingCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WalListingCache {
    /// Create a new, empty cache.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            list_call_count: AtomicU64::new(0),
        }
    }

    /// Populate the cache with a fresh WAL file listing.
    ///
    /// The caller is responsible for performing the actual object-store
    /// `list()` operation and passing the results here. Increments
    /// `list_call_count` by 1 so tests can assert total LIST usage.
    pub fn populate(&self, entries: Vec<String>) {
        self.list_call_count.fetch_add(1, Ordering::Relaxed);
        *self.entries.lock().unwrap() = entries;
    }

    /// Return cached WAL file entries **without** issuing a LIST call.
    ///
    /// This is the hot path. Always reads from in-memory state.
    pub fn get_cached_entries(&self) -> Vec<String> {
        self.entries.lock().unwrap().clone()
    }

    /// Invalidate the cache on WAL rotation.
    ///
    /// After invalidation, `is_populated()` returns false. The next
    /// `populate` call will re-fill the cache and record one more LIST call.
    pub fn invalidate(&self) {
        self.entries.lock().unwrap().clear();
    }

    /// Number of `populate` calls made (proxy for object-store LIST operations).
    pub fn list_call_count(&self) -> u64 {
        self.list_call_count.load(Ordering::Relaxed)
    }

    /// Whether the cache currently has any entries.
    pub fn is_populated(&self) -> bool {
        !self.entries.lock().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_cache_is_empty() {
        let cache = WalListingCache::new();
        assert!(!cache.is_populated());
        assert_eq!(cache.list_call_count(), 0);
        assert!(cache.get_cached_entries().is_empty());
    }

    #[test]
    fn populate_increments_list_count() {
        let cache = WalListingCache::new();
        cache.populate(vec!["wal/0001.log".to_string()]);
        assert_eq!(cache.list_call_count(), 1);
        cache.populate(vec!["wal/0001.log".to_string(), "wal/0002.log".to_string()]);
        assert_eq!(cache.list_call_count(), 2);
    }

    #[test]
    fn hot_path_returns_entries_without_incrementing_count() {
        let cache = WalListingCache::new();
        cache.populate(vec!["wal/0001.log".to_string(), "wal/0002.log".to_string()]);
        assert_eq!(cache.list_call_count(), 1);

        // 100 hot-path accesses must not increment list_call_count.
        for _ in 0..100 {
            let entries = cache.get_cached_entries();
            assert_eq!(entries.len(), 2);
        }
        assert_eq!(
            cache.list_call_count(),
            1,
            "hot path must not issue LIST calls"
        );
    }

    #[test]
    fn invalidate_clears_entries() {
        let cache = WalListingCache::new();
        cache.populate(vec!["wal/0001.log".to_string()]);
        assert!(cache.is_populated());

        cache.invalidate();
        assert!(!cache.is_populated());
        assert!(cache.get_cached_entries().is_empty());
        // list_call_count is NOT incremented by invalidate.
        assert_eq!(cache.list_call_count(), 1);
    }

    #[test]
    fn repopulate_after_invalidate_increments_count() {
        let cache = WalListingCache::new();
        cache.populate(vec!["wal/0001.log".to_string()]);
        cache.invalidate();
        cache.populate(vec!["wal/0001.log".to_string(), "wal/0002.log".to_string()]);
        assert_eq!(cache.list_call_count(), 2);
        assert_eq!(cache.get_cached_entries().len(), 2);
    }
}

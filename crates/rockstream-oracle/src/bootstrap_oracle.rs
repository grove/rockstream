//! Bootstrap reference oracle for RockStream (v0.23).
//!
//! `BootstrapOracle` provides batch reference implementations for the
//! snapshot bootstrap algorithm.  It is used in property tests to verify
//! that the incremental `SnapshotOp` produces the same rows as the batch
//! reference across all epoch configurations and resume scenarios.

use rockstream_types::batch::ZSet;

/// Flat row representation for deterministic comparison in tests.
///
/// `(key, value, weight)` where `weight` is always `+1` for snapshot rows.
pub type FlatRow = (Vec<u8>, Vec<u8>, i64);

/// Batch reference oracle for the snapshot bootstrap algorithm.
pub struct BootstrapOracle;

impl BootstrapOracle {
    /// Simulate streaming bootstrap: split `rows` into batches of at most
    /// `batch_size` and return each as an insert-only `ZSet`.
    ///
    /// This is the reference for `SnapshotOp::drain_all()`.
    pub fn batches(rows: &[(Vec<u8>, Vec<u8>)], batch_size: usize) -> Vec<ZSet> {
        assert!(batch_size > 0, "batch_size must be positive");
        rows.chunks(batch_size)
            .map(|chunk| {
                let mut z = ZSet::new();
                for (key, value) in chunk {
                    z.insert(key.clone(), value.clone(), 1);
                }
                z
            })
            .collect()
    }

    /// Merge all batches into a single `ZSet`.
    ///
    /// This is the "batch reference" for a complete snapshot: all rows
    /// delivered with weight `+1`.
    pub fn merge_all(batches: &[ZSet]) -> ZSet {
        let mut result = ZSet::new();
        for batch in batches {
            result.merge(batch);
        }
        result
    }

    /// Simulate a resume from `committed_rows`.
    ///
    /// Returns the batches for `rows[committed_rows..]`, i.e., only the rows
    /// that have not yet been committed.  This is the reference for
    /// `SnapshotOp::resume_from(committed_rows)` followed by `drain_all()`.
    pub fn resume(
        rows: &[(Vec<u8>, Vec<u8>)],
        batch_size: usize,
        committed_rows: usize,
    ) -> Vec<ZSet> {
        let remaining = if committed_rows >= rows.len() {
            &[][..]
        } else {
            &rows[committed_rows..]
        };
        Self::batches(remaining, batch_size)
    }
}

/// Return all rows from a `ZSet` as a sorted `Vec<FlatRow>`.
///
/// Rows are sorted lexicographically by `(key, value)` for deterministic
/// comparison in proof tests.
pub fn sorted_rows(zset: &ZSet) -> Vec<FlatRow> {
    let mut rows: Vec<FlatRow> = zset
        .iter()
        .filter(|r| r.weight != 0)
        .map(|r| (r.key.clone(), r.value.clone(), r.weight))
        .collect();
    rows.sort();
    rows
}

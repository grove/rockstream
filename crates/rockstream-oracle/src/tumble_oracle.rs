//! Reference oracle for tumbling time-window correctness (v0.20).
//!
//! `TumbleOracle` computes the ground-truth output for a set of input rows
//! under given window parameters and a watermark value. It groups rows into
//! fixed-size tumbling windows and returns only the windows whose end is ≤
//! the watermark (i.e., windows that are "closed").
//!
//! The oracle is intentionally simple and non-incremental.  Property tests
//! use it to assert that `TumbleOp` produces the same output after processing
//! the same rows and watermark incrementally.

use std::collections::HashMap;

/// Start timestamp of a tumbling window (milliseconds epoch).
pub type WindowStart = i64;

/// A row in the oracle's output: (row_id, key, value).
pub type OracleRow = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Batch reference implementation for tumbling time windows.
pub struct TumbleOracle;

impl TumbleOracle {
    /// Compute window assignments for a set of rows.
    ///
    /// # Parameters
    /// - `rows`: slice of `(key, event_ts_ms, value)` triples.
    /// - `window_size_ms`: width of each tumbling window in milliseconds.
    /// - `watermark_ms`: only windows with `window_end <= watermark_ms` are
    ///   included in the result (closed windows).
    ///
    /// # Returns
    /// A map from `window_start` to the list of `(row_id, key, value)` triples
    /// that belong to that window.  Late rows (event_ts falls in a closed
    /// window but the policy is Drop) are excluded by the caller — this oracle
    /// returns all rows assigned to closed windows without late-data filtering.
    pub fn compute(
        rows: &[(Vec<u8>, i64, Vec<u8>)],
        window_size_ms: i64,
        watermark_ms: i64,
    ) -> HashMap<WindowStart, Vec<OracleRow>> {
        assert!(window_size_ms > 0, "window_size_ms must be positive");

        let mut windows: HashMap<WindowStart, Vec<OracleRow>> = HashMap::new();

        for (key, event_ts, value) in rows {
            let ws = event_ts.div_euclid(window_size_ms) * window_size_ms;
            let we = ws + window_size_ms;
            if we <= watermark_ms {
                // Closed window — include this row.
                let row_id = make_oracle_row_id(key, value);
                windows
                    .entry(ws)
                    .or_default()
                    .push((row_id, key.clone(), value.clone()));
            }
        }

        windows
    }

    /// Compute the total number of rows across all closed windows.
    pub fn total_rows(
        rows: &[(Vec<u8>, i64, Vec<u8>)],
        window_size_ms: i64,
        watermark_ms: i64,
    ) -> usize {
        Self::compute(rows, window_size_ms, watermark_ms)
            .values()
            .map(|v| v.len())
            .sum()
    }
}

/// Build a row identifier matching the one used by `TumbleOp::make_row_id`.
pub fn make_oracle_row_id(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(key.len() + 1 + value.len());
    id.extend_from_slice(key);
    id.push(0xFF);
    id.extend_from_slice(value);
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: u8, event_ts: i64, data: u8) -> (Vec<u8>, i64, Vec<u8>) {
        let mut val = event_ts.to_be_bytes().to_vec();
        val.push(data);
        (vec![key], event_ts, val)
    }

    #[test]
    fn oracle_groups_into_windows() {
        let rows = vec![row(1, 100, 0), row(2, 500, 0), row(3, 1200, 0)];
        // watermark=2000 → windows [0,1000) and [1000,2000) are closed.
        let result = TumbleOracle::compute(&rows, 1000, 2000);
        assert_eq!(result[&0].len(), 2, "two rows in [0,1000)");
        assert_eq!(result[&1000].len(), 1, "one row in [1000,2000)");
    }

    #[test]
    fn oracle_excludes_open_windows() {
        let rows = vec![row(1, 1500, 0)];
        // watermark=1000 → window [1000,2000) is still open.
        let result = TumbleOracle::compute(&rows, 1000, 1000);
        assert!(result.is_empty(), "open window should not appear");
    }

    #[test]
    fn oracle_empty_input() {
        let result = TumbleOracle::compute(&[], 1000, 5000);
        assert!(result.is_empty());
    }
}

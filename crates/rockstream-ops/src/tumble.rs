//! Tumbling time-window operator for RockStream IVM (v0.20).
//!
//! ## Design
//!
//! `TumbleOp` groups rows into fixed-size, non-overlapping time windows of
//! `window_size_ms` milliseconds.  Each row is assigned to the window
//! `[start, start + window_size_ms)` where
//! `start = floor(event_ts / window_size_ms) * window_size_ms`.
//!
//! Windows close **exactly once** when the event-time watermark advances past
//! the window end.  Watermark advancement follows `MaxRegister/v1` semantics:
//! the current watermark is `max(current, new_watermark)`.  Passing the same
//! watermark value twice is a no-op (idempotent).
//!
//! Late rows (arriving after window close) are handled per `LateDataPolicy`:
//! - `Drop` — silently discard; closed window output is unchanged.
//! - `Update` — retract the previous window output and re-emit with the
//!   late row included.
//! - `RouteToSink` — copy the row to the `TumbleResult::late_sink` Z-set.
//!
//! ## Time function
//!
//! Callers supply a `TimeFn` closure that extracts an `i64` millisecond epoch
//! timestamp from `(key_bytes, value_bytes)`.  The canonical encoding for
//! tests is the first 8 bytes of `value` as a big-endian `i64`.
//!
//! ## TTL / state retention
//!
//! Closed-window row state is deliberately retained in `self.emitted` for
//! the lifetime of the operator.  This satisfies the v0.20 proof criterion:
//! "TTL never removes visible state."

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_plan::LateDataPolicy;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::max_register::MAX_REGISTER_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Closure that extracts an i64 event timestamp (milliseconds) from a row.
pub type TimeFn = Arc<dyn Fn(&[u8], &[u8]) -> i64 + Send + Sync + 'static>;

/// Stable row identifier: `key ++ 0xFF ++ value`.
type RowId = Vec<u8>;

/// Emitted rows for a closed window: `(row_id, key, value)` triples.
type EmittedRows = Vec<(RowId, Vec<u8>, Vec<u8>)>;

/// Start timestamp of a tumbling window (milliseconds epoch).
type WindowStart = i64;

/// Per-window live state: row_id → (key, value, net_weight).
type WindowRows = HashMap<RowId, (Vec<u8>, Vec<u8>, i64)>;

/// Output of a single `process` call.
pub struct TumbleResult {
    /// Delta for the main output stream (closed-window rows).
    pub output: ZSet,
    /// Rows routed to the late-data sink (`RouteToSink` policy only).
    pub late_sink: ZSet,
}

/// Tumbling time-window IVM operator.
pub struct TumbleOp {
    window_size_ms: i64,
    late_data_policy: LateDataPolicy,
    time_fn: TimeFn,
    /// Live row state per window (open and, for Update policy, closed).
    window_state: BTreeMap<WindowStart, WindowRows>,
    /// Current watermark (MaxRegister/v1: only monotonically non-decreasing).
    current_watermark_ms: i64,
    /// Windows that have been closed and whose output emitted.
    closed: BTreeSet<WindowStart>,
    /// Last-emitted row list per closed window (for Update-policy retraction).
    ///
    /// Retained indefinitely — TTL never removes visible state.
    emitted: HashMap<WindowStart, EmittedRows>,
    /// Name used in `Operator::name()`.
    name: String,
}

impl TumbleOp {
    /// Create a new `TumbleOp`.
    pub fn new(window_size_ms: i64, late_data_policy: LateDataPolicy, time_fn: TimeFn) -> Self {
        assert!(window_size_ms > 0, "window_size_ms must be positive");
        Self {
            window_size_ms,
            late_data_policy,
            time_fn,
            window_state: BTreeMap::new(),
            current_watermark_ms: i64::MIN,
            closed: BTreeSet::new(),
            emitted: HashMap::new(),
            name: "TumbleOp".to_owned(),
        }
    }

    /// Compute the window start for an event timestamp using Euclidean division
    /// so that negative timestamps are handled correctly.
    pub fn window_start_for(&self, event_ts: i64) -> WindowStart {
        event_ts.div_euclid(self.window_size_ms) * self.window_size_ms
    }

    /// Compute the window end (exclusive) for a window start.
    pub fn window_end_of(&self, window_start: WindowStart) -> i64 {
        window_start + self.window_size_ms
    }

    /// Current watermark value (`i64::MIN` = no watermark received yet).
    pub fn current_watermark(&self) -> i64 {
        self.current_watermark_ms
    }

    /// Emitted rows per closed window (retained indefinitely for TTL proof).
    pub fn emitted(&self) -> &HashMap<WindowStart, EmittedRows> {
        &self.emitted
    }

    /// Process a Z-set delta and an optional watermark advancement.
    ///
    /// Returns a `TumbleResult` with the main output delta and any late-sink
    /// rows.
    pub fn process(&mut self, input: &ZSet, watermark_ms: i64) -> TumbleResult {
        // --- Watermark advancement (MaxRegister/v1: monotone max) -----------
        let new_wm = self.current_watermark_ms.max(watermark_ms);
        self.current_watermark_ms = new_wm;

        let mut output = ZSet::new();
        let mut late_sink = ZSet::new();

        // --- Phase 1: Classify and buffer each input row --------------------
        for row in input.iter() {
            let event_ts = (self.time_fn)(&row.key, &row.value);
            let ws = self.window_start_for(event_ts);
            let row_id = make_row_id(&row.key, &row.value);

            if self.closed.contains(&ws) {
                // ── Late data ──────────────────────────────────────────────
                match self.late_data_policy.clone() {
                    LateDataPolicy::Drop => {
                        // Silently discard; no output change.
                    }
                    LateDataPolicy::RouteToSink { .. } => {
                        late_sink.insert(row.key.clone(), row.value.clone(), row.weight);
                    }
                    LateDataPolicy::Update => {
                        // Retract the previous closed-window emission.
                        if let Some(old) = self.emitted.remove(&ws) {
                            for (rid, _k, v) in &old {
                                output.insert(rid.clone(), v.clone(), -1);
                            }
                        }

                        // Update the retained window state for this closed window.
                        let state = self.window_state.entry(ws).or_default();
                        let entry = state
                            .entry(row_id.clone())
                            .or_insert_with(|| (row.key.clone(), row.value.clone(), 0));
                        entry.2 += row.weight;
                        if entry.2 <= 0 {
                            state.remove(&row_id);
                        } else {
                            entry.0 = row.key.clone();
                            entry.1 = row.value.clone();
                        }

                        // Re-emit the updated window.
                        let mut new_emitted = Vec::new();
                        if let Some(state) = self.window_state.get(&ws) {
                            for (rid, (k, v, w)) in state {
                                if *w > 0 {
                                    output.insert(rid.clone(), v.clone(), 1);
                                    new_emitted.push((rid.clone(), k.clone(), v.clone()));
                                }
                            }
                        }
                        self.emitted.insert(ws, new_emitted);
                    }
                }
                continue;
            }

            // ── Open window: buffer the row ──────────────────────────────
            let state = self.window_state.entry(ws).or_default();
            let entry = state
                .entry(row_id.clone())
                .or_insert_with(|| (row.key.clone(), row.value.clone(), 0));
            entry.2 += row.weight;
            if entry.2 <= 0 {
                state.remove(&row_id);
            } else {
                entry.0 = row.key.clone();
                entry.1 = row.value.clone();
            }
        }

        // --- Phase 2: Close windows whose end ≤ current watermark ----------
        let to_close: Vec<WindowStart> = self
            .window_state
            .keys()
            .filter(|&&ws| self.window_end_of(ws) <= new_wm && !self.closed.contains(&ws))
            .copied()
            .collect();

        for ws in to_close {
            let state = self.window_state.get(&ws).cloned().unwrap_or_default();
            let mut emitted_rows = Vec::new();
            for (rid, (k, v, w)) in &state {
                if *w > 0 {
                    output.insert(rid.clone(), v.clone(), 1);
                    emitted_rows.push((rid.clone(), k.clone(), v.clone()));
                }
            }
            self.closed.insert(ws);
            self.emitted.insert(ws, emitted_rows);
        }

        TumbleResult { output, late_sink }
    }

    /// Merge-law ID used for watermark tracking (`MaxRegister/v1`).
    pub fn watermark_law_id(&self) -> MergeLawId {
        MAX_REGISTER_ID
    }
}

/// Build a stable row identifier from key and value bytes.
pub fn make_row_id(key: &[u8], value: &[u8]) -> RowId {
    let mut id = Vec::with_capacity(key.len() + 1 + value.len());
    id.extend_from_slice(key);
    id.push(0xFF);
    id.extend_from_slice(value);
    id
}

#[async_trait]
impl Operator for TumbleOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        // Use batch-level watermark=i64::MIN (no watermark advancement).
        // Real integration uses the dedicated `process` method.
        let result = TumbleOp::process(self, &input.zset, i64::MIN);
        ZSetBatch {
            zset: result.output,
            epoch: 0,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(MAX_REGISTER_ID)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::LateDataPolicy;

    /// Build a 9-byte value: 8 bytes event_ts (i64 BE) + 1 byte data.
    fn make_value(event_ts: i64, data: u8) -> Vec<u8> {
        let mut v = event_ts.to_be_bytes().to_vec();
        v.push(data);
        v
    }

    fn ts_time_fn() -> TimeFn {
        Arc::new(|_key: &[u8], value: &[u8]| i64::from_be_bytes(value[..8].try_into().unwrap()))
    }

    #[test]
    fn tumble_op_compiles() {
        let _ = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
    }

    #[test]
    fn window_start_computation() {
        let op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
        assert_eq!(op.window_start_for(0), 0);
        assert_eq!(op.window_start_for(999), 0);
        assert_eq!(op.window_start_for(1000), 1000);
        assert_eq!(op.window_start_for(1500), 1000);
        assert_eq!(op.window_start_for(2000), 2000);
    }

    #[test]
    fn watermark_law_is_max_register() {
        let op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
        assert_eq!(op.watermark_law_id(), MAX_REGISTER_ID);
    }

    #[test]
    fn open_window_no_output_before_watermark() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
        let mut input = ZSet::new();
        input.insert(vec![1u8], make_value(500, 42), 1);
        // watermark=0 — window [0,1000) not yet closed
        let result = op.process(&input, 0);
        assert_eq!(
            result.output.iter().count(),
            0,
            "no output before window closes"
        );
    }

    #[test]
    fn window_closes_exactly_once_on_watermark() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
        let mut input = ZSet::new();
        input.insert(vec![1u8], make_value(500, 42), 1);
        // Advance watermark to 1000 → window [0, 1000) closes
        let result = op.process(&input, 1000);
        assert_eq!(
            result.output.iter().count(),
            1,
            "window should emit one row"
        );
        // Second watermark at 1000 → idempotent, no re-emission
        let result2 = op.process(&ZSet::new(), 1000);
        assert_eq!(
            result2.output.iter().count(),
            0,
            "duplicate watermark is no-op"
        );
    }
}

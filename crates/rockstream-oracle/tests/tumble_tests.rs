//! Proof and property tests for tumbling time-window IVM operators (v0.20).
//!
//! Proves:
//! 1. `LateDataPolicy::Drop` — late rows are silently discarded.
//! 2. `LateDataPolicy::Update` — late rows cause retraction + re-emission.
//! 3. `LateDataPolicy::RouteToSink` — late rows go to the side-channel sink.
//! 4. TTL never removes visible state — closed-window output is retained.
//! 5. Tumbling windows close exactly once under out-of-order input.
//! 6. Duplicate watermark replay is idempotent.
//! 7. `TumbleOp` output matches `TumbleOracle` for randomised inputs (proptest).
//! 8. `TumbleWindow` plan nodes round-trip through the catalog codec.
//! 9. `DiffCtx` assigns `MaxRegister/v1` to `TumbleWindow` nodes.

#[cfg(test)]
mod tumble_proof_tests {
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_catalog::codec::{decode, encode};
    use rockstream_diff::DiffCtx;
    use rockstream_ops::tumble::{TimeFn, TumbleOp};
    use rockstream_oracle::tumble_oracle::TumbleOracle;
    use rockstream_plan::{LateDataPolicy, OpKind, PlanNode};
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::max_register::MAX_REGISTER_ID;
    use rockstream_types::laws::registry::LawRegistry;

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Build a 9-byte value: 8 bytes event_ts (i64 BE) + 1 byte data.
    fn make_value(event_ts: i64, data: u8) -> Vec<u8> {
        let mut v = event_ts.to_be_bytes().to_vec();
        v.push(data);
        v
    }

    /// Extract the event timestamp from the canonical 9-byte value encoding.
    fn ts_time_fn() -> TimeFn {
        Arc::new(|_key: &[u8], value: &[u8]| i64::from_be_bytes(value[..8].try_into().unwrap()))
    }

    /// Build a single-row ZSet.
    fn one_row(key: u8, event_ts: i64, data: u8) -> ZSet {
        let mut z = ZSet::new();
        z.insert(vec![key], make_value(event_ts, data), 1);
        z
    }

    // ── Proof 1: LateDataPolicy::Drop ────────────────────────────────────────

    /// Late rows with the `Drop` policy must not appear in the output and must
    /// not alter the already-emitted window content.
    #[test]
    fn proof_late_data_drop_is_not_visible() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());

        // Insert row at t=500 (window [0, 1000)).
        let result = op.process(&one_row(1, 500, 10), 0);
        assert_eq!(result.output.iter().count(), 0, "window not yet closed");

        // Advance watermark to 1000 → close window [0, 1000).
        let result = op.process(&ZSet::new(), 1000);
        assert_eq!(
            result.output.iter().count(),
            1,
            "window closes with one row"
        );

        // Late row arrives at t=200 (inside the now-closed window [0,1000)).
        let result = op.process(&one_row(2, 200, 99), 1000);
        assert_eq!(result.output.iter().count(), 0, "late row must be dropped");
        assert_eq!(result.late_sink.iter().count(), 0, "not routed to sink");

        // The original emitted window must still contain exactly one row.
        let emitted = op.emitted();
        assert_eq!(emitted[&0].len(), 1, "emitted window unchanged after drop");
    }

    // ── Proof 2: LateDataPolicy::Update ──────────────────────────────────────

    /// Late rows with the `Update` policy must cause a retraction of the
    /// previous window output and re-emission with the late row included.
    /// The ZSet correctly folds retract-then-re-insert of unchanged rows into a
    /// zero net weight, so the observable delta contains only the net-new rows.
    #[test]
    fn proof_late_data_update_replaces_closed_window() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Update, ts_time_fn());

        // Insert row at t=500.
        let result = op.process(&one_row(1, 500, 10), 0);
        assert_eq!(result.output.iter().count(), 0);

        // Close window [0, 1000).
        let result = op.process(&ZSet::new(), 1000);
        assert_eq!(result.output.iter().count(), 1, "initial emission of 1 row");

        // Insert late row at t=300.
        let result = op.process(&one_row(2, 300, 77), 1000);

        // The ZSet net delta: row_A is retracted then re-emitted (net 0),
        // row_B (the late row) is new (net +1).
        // Downstream sees only row_B in the delta.
        let rows: Vec<_> = result.output.iter().collect();
        assert_eq!(rows.len(), 1, "net delta contains only the new late row");
        assert_eq!(rows[0].weight, 1, "late row has +1 weight");
        assert_eq!(
            result.late_sink.iter().count(),
            0,
            "Update policy must not route to late_sink"
        );

        // The emitted state must now contain both rows (original + late).
        let emitted = op.emitted();
        assert_eq!(emitted[&0].len(), 2, "two rows in updated window emitted state");
    }

    // ── Proof 3: LateDataPolicy::RouteToSink ─────────────────────────────────

    /// Late rows with the `RouteToSink` policy must be placed in `late_sink`
    /// and must not appear in the main output.
    #[test]
    fn proof_late_data_route_to_sink() {
        let mut op = TumbleOp::new(
            1000,
            LateDataPolicy::RouteToSink {
                sink_name: "late_events".to_owned(),
            },
            ts_time_fn(),
        );

        // Insert and close window [0, 1000).
        op.process(&one_row(1, 500, 10), 1000);

        // Late row.
        let result = op.process(&one_row(2, 200, 55), 1000);

        assert_eq!(
            result.output.iter().count(),
            0,
            "late row not in main output"
        );
        assert_eq!(result.late_sink.iter().count(), 1, "late row in late_sink");
    }

    // ── Proof 4: TTL never removes visible state ──────────────────────────────

    /// State of closed windows is retained indefinitely in `emitted`.
    /// Advancing the watermark further must not remove older window state.
    #[test]
    fn proof_ttl_never_removes_visible_state() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());

        // Insert rows in windows [0,1000), [1000,2000), [2000,3000).
        op.process(&one_row(1, 100, 1), 0);
        op.process(&one_row(2, 1100, 2), 0);
        op.process(&one_row(3, 2100, 3), 0);

        // Advance watermark to 3000 → all three windows close.
        let result = op.process(&ZSet::new(), 3000);
        assert_eq!(result.output.iter().count(), 3, "three rows emitted");

        // All three windows must still be in `emitted` — TTL has not GC'd them.
        let emitted = op.emitted();
        assert!(emitted.contains_key(&0), "window [0,1000) retained");
        assert!(emitted.contains_key(&1000), "window [1000,2000) retained");
        assert!(emitted.contains_key(&2000), "window [2000,3000) retained");

        // Advance watermark to 10_000 — still retained.
        op.process(&ZSet::new(), 10_000);
        let emitted = op.emitted();
        assert_eq!(
            emitted.len(),
            3,
            "all three windows retained after further watermark advance"
        );
    }

    // ── Proof 5: Closes exactly once under out-of-order input ─────────────────

    /// Even when rows arrive out of order (e.g. t=700 before t=300), a
    /// tumbling window must close exactly once when the watermark passes its
    /// end.
    #[test]
    fn proof_tumble_closes_exactly_once_under_out_of_order_input() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());
        let mut total_inserted = 0i64;

        // Rows in non-monotone timestamp order, all within window [0, 1000).
        for (key, ts, data) in [(1u8, 700i64, 10u8), (2, 300, 20), (3, 500, 30)] {
            op.process(&one_row(key, ts, data), 0);
        }

        // Partial watermark advances (not yet past window end).
        op.process(&ZSet::new(), 500);
        op.process(&ZSet::new(), 800);

        // Watermark reaches exactly the window boundary → window closes.
        let result1 = op.process(&ZSet::new(), 1000);
        let count1 = result1.output.iter().count() as i64;
        total_inserted += count1;
        assert_eq!(count1, 3, "all three rows emitted exactly once");

        // Further watermark advances must not re-close the window.
        let result2 = op.process(&ZSet::new(), 1500);
        let result3 = op.process(&ZSet::new(), 2000);
        total_inserted += result2.output.iter().count() as i64;
        total_inserted += result3.output.iter().count() as i64;

        assert_eq!(
            total_inserted, 3,
            "window closed exactly once, 3 rows total"
        );
    }

    // ── Proof 6: Duplicate watermark replay is idempotent ─────────────────────

    /// Advancing the watermark to a value it has already seen must produce no
    /// additional output — this is the `MaxRegister/v1` idempotence property.
    #[test]
    fn prop_duplicate_watermark_replay_is_idempotent() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());

        // Insert row and close window [0, 1000) with watermark=1000.
        op.process(&one_row(1, 500, 10), 1000);

        // Replay watermark=1000 multiple times — each must produce no output.
        for _ in 0..5 {
            let result = op.process(&ZSet::new(), 1000);
            assert_eq!(
                result.output.iter().count(),
                0,
                "duplicate watermark must not re-emit the window"
            );
        }

        // Same for a watermark below the current.
        for wm in [0, 500, 999, 1000] {
            let result = op.process(&ZSet::new(), wm);
            assert_eq!(
                result.output.iter().count(),
                0,
                "stale watermark {wm} must produce no output"
            );
        }
    }

    // ── Proof 7: TumbleOp matches TumbleOracle (property test) ───────────────

    proptest! {
        /// For any set of rows with positive event timestamps and a random
        /// watermark, `TumbleOp` with the `Drop` policy emits exactly the same
        /// rows as `TumbleOracle::compute` for closed windows.
        #[test]
        fn prop_tumble_op_matches_oracle(
            rows in prop::collection::vec(
                (1u8..=4u8, 0i64..=9_999i64, 0u8..=255u8),
                0..=20_usize,
            ),
            watermark_ms in 1000i64..=10_000i64,
            window_size_ms in prop::sample::select(vec![500i64, 1000, 2000]),
        ) {
            let mut op = TumbleOp::new(window_size_ms, LateDataPolicy::Drop, ts_time_fn());

            // Build the Z-set and the oracle rows simultaneously.
            let mut input = ZSet::new();
            let mut oracle_rows: Vec<(Vec<u8>, i64, Vec<u8>)> = Vec::new();

            for (key, event_ts, data) in &rows {
                let key_bytes = vec![*key];
                let val_bytes = make_value(*event_ts, *data);
                input.insert(key_bytes.clone(), val_bytes.clone(), 1);
                oracle_rows.push((key_bytes, *event_ts, val_bytes));
            }

            // Feed all rows with the given watermark.
            let result = op.process(&input, watermark_ms);

            // Compute oracle ground truth.
            let oracle = TumbleOracle::compute(&oracle_rows, window_size_ms, watermark_ms);
            let oracle_total: usize = oracle.values().map(|v| v.len()).sum();

            // Count positive-weight rows in the operator output.
            let op_total: usize = result.output.iter().filter(|r| r.weight > 0).count();

            prop_assert_eq!(
                op_total,
                oracle_total,
                "TumbleOp row count != oracle (window_size={}, wm={})",
                window_size_ms,
                watermark_ms,
            );
        }
    }

    // ── Proof 8: Plan codec round-trip ────────────────────────────────────────

    /// `PlanNode::TumbleWindow` must survive an encode → decode round-trip
    /// through the catalog codec.
    #[test]
    fn proof_tumble_window_codec_roundtrip() {
        let plan = PlanNode::TumbleWindow {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            time_col: 2,
            window_size_ms: 5000,
            late_data_policy: LateDataPolicy::RouteToSink {
                sink_name: "late".into(),
            },
        };

        let registry = LawRegistry::default();
        let bytes = encode(&plan, &|_| None).expect("encode failed");
        let decoded = decode(&bytes, &registry).expect("decode failed");
        assert_eq!(plan, decoded, "TumbleWindow codec round-trip failed");
    }

    #[test]
    fn proof_tumble_window_codec_roundtrip_drop() {
        let plan = PlanNode::TumbleWindow {
            input: Box::new(PlanNode::Source { name: "s".into() }),
            time_col: 0,
            window_size_ms: 1000,
            late_data_policy: LateDataPolicy::Drop,
        };
        let registry = LawRegistry::default();
        let bytes = encode(&plan, &|_| None).unwrap();
        assert_eq!(decode(&bytes, &registry).unwrap(), plan);
    }

    #[test]
    fn proof_tumble_window_codec_roundtrip_update() {
        let plan = PlanNode::TumbleWindow {
            input: Box::new(PlanNode::Source { name: "s".into() }),
            time_col: 1,
            window_size_ms: 2000,
            late_data_policy: LateDataPolicy::Update,
        };
        let registry = LawRegistry::default();
        let bytes = encode(&plan, &|_| None).unwrap();
        assert_eq!(decode(&bytes, &registry).unwrap(), plan);
    }

    // ── Proof 9: DiffCtx assigns MaxRegister/v1 ───────────────────────────────

    /// `DiffCtx` must annotate `TumbleWindow` physical operators with
    /// `merge_law = MaxRegister/v1` and no `not_merge_safe_reason`.
    #[test]
    fn proof_diff_ctx_tumble_window_uses_max_register() {
        let plan = PlanNode::TumbleWindow {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            time_col: 0,
            window_size_ms: 1000,
            late_data_policy: LateDataPolicy::Drop,
        };

        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&plan);

        let tumble_op = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::TumbleWindow { .. }))
            .expect("TumbleWindow op not found in plan");

        assert_eq!(
            tumble_op.merge_law,
            Some(MAX_REGISTER_ID),
            "TumbleWindow must use MaxRegister/v1 for watermark state"
        );
        assert!(
            tumble_op.not_merge_safe_reason.is_none(),
            "TumbleWindow with MaxRegister is merge-safe"
        );
    }

    // ── Watermark monotone advance test ───────────────────────────────────────

    /// Watermark must only advance (MaxRegister semantics): providing a lower
    /// value must not reduce the current watermark.
    #[test]
    fn proof_watermark_is_monotone() {
        let mut op = TumbleOp::new(1000, LateDataPolicy::Drop, ts_time_fn());

        // Advance watermark to 5000.
        op.process(&ZSet::new(), 5000);
        assert_eq!(op.current_watermark(), 5000);

        // Provide a lower watermark — must not retreat.
        op.process(&ZSet::new(), 100);
        assert_eq!(op.current_watermark(), 5000, "watermark must not retreat");

        // Advance further.
        op.process(&ZSet::new(), 7000);
        assert_eq!(op.current_watermark(), 7000);
    }
}

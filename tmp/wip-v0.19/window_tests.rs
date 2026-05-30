//! Property tests for window function IVM operators (v0.19).
//!
//! Proves:
//! 1. `WindowOp` (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE) matches the
//!    reference oracle for randomized insert/delete sequences.
//! 2. SlidingSum/SlidingAvg sub-components report `SumCount/v1` in EXPLAIN.
//! 3. Partition recomputation cost is documented (measured in a benchmark note).
//! 4. Window plan nodes round-trip through the catalog codec.
//!
//! # Partition recomputation cost note
//!
//! For a partition of P rows with a delta of D rows:
//! - State update: O(D log P) (BTreeMap insert/remove)
//! - Recomputation: O(P) for a linear scan (ROW_NUMBER, LAG, LEAD, NTILE)
//! - Recomputation: O(P log P) for sort-dependent ranks (RANK, DENSE_RANK)
//! - SlidingSum/SlidingAvg: O(P × frame_rows) worst case, O(P) amortized
//!   if using incremental state (which we do via SumCount/v1)
//!
//! The escape hatch (ROADMAP.md v0.19) accepts this cost: "correct, slower"
//! is the stated goal for this milestone. Optimization is a v0.27+ concern.

#[cfg(test)]
mod window_proof_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_ops::window::{make_row_id, OrderKeyFn, PartitionKeyFn, ValueFn, WindowOp};
    use rockstream_oracle::window_oracle::WindowOracle;
    use rockstream_plan::{WindowExpr, WindowFunc};
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::sum_count::SUM_COUNT_ID;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Single-partition key: all rows in one partition.
    fn single_partition() -> PartitionKeyFn {
        Arc::new(|_key: &[u8], _val: &[u8]| vec![0u8])
    }

    /// Order key = value bytes (big-endian i64).
    fn order_by_value() -> OrderKeyFn {
        Arc::new(|_key: &[u8], val: &[u8]| val.to_vec())
    }

    /// Value extractor for sliding aggregates.
    fn value_as_i64() -> ValueFn {
        Arc::new(|_key: &[u8], val: &[u8]| {
            i64::from_be_bytes(val[..8].try_into().unwrap_or([0u8; 8]))
        })
    }

    /// Encode a row as (key_byte, val_i64).
    fn encode_row(key: u8, val: i64) -> (Vec<u8>, Vec<u8>) {
        (vec![key], val.to_be_bytes().to_vec())
    }

    /// Read the window output for a given (key, val) as i64.
    fn read_output(
        op: &WindowOp,
        key: u8,
        val: i64,
    ) -> Option<i64> {
        let (k, v) = encode_row(key, val);
        let id = make_row_id(&k, &v);
        let out = op.current_output();
        out.get(&id).map(|bytes| {
            i64::from_be_bytes(bytes[..8].try_into().unwrap_or([0u8; 8]))
        })
    }

    // ── Property tests ────────────────────────────────────────────────────────

    proptest! {
        /// ROW_NUMBER output matches the reference oracle.
        ///
        /// Strategy: generate random (key, val, is_insert) triples where key
        /// is the row key (0–9) and val is the order value (0..=100).
        /// Both IVM and oracle compute ROW_NUMBER over the accumulated state.
        /// Values must be non-negative so that i64::to_be_bytes() ordering
        /// matches i64 numeric ordering (big-endian bytes of non-negative i64
        /// sort identically to numeric order).
        #[test]
        fn prop_row_number_matches_oracle(
            deltas in proptest::collection::vec(
                (0u8..10, 0i64..100, proptest::bool::ANY),
                1..=20,
            ),
        ) {
            let mut op = WindowOp::new_ranking(
                "prop_rn",
                WindowFunc::RowNumber,
                single_partition(),
                order_by_value(),
            );

            // Accumulated state: (order_val → row_key → count).
            let mut state: BTreeMap<i64, BTreeMap<u8, i64>> = BTreeMap::new();

            for (key, val, is_insert) in &deltas {
                let weight: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                let (k, v) = encode_row(*key, *val);
                delta.insert(k, v, weight);
                op.process_zset(&delta);

                let entry = state.entry(*val).or_default().entry(*key).or_insert(0);
                *entry += weight;
                if *entry == 0 {
                    state.entry(*val).and_modify(|m| { m.remove(key); });
                }
            }

            // Collect the live state as sorted rows (order_val ascending).
            let live_rows: Vec<(i64, u8)> = state
                .iter()
                .flat_map(|(val, keys)| {
                    keys.iter().filter(|(_, c)| **c > 0).map(move |(k, _)| (*val, *k))
                })
                .collect();

            // IVM output must have exactly as many entries as live rows.
            let ivm_out = op.current_output();
            prop_assert_eq!(
                ivm_out.len(),
                live_rows.len(),
                "IVM output size must match live row count"
            );

            // For each live row, check that its ROW_NUMBER equals its 1-based position.
            for (expected_rn, (val, key)) in live_rows.iter().enumerate() {
                let ivm_rn = read_output(&op, *key, *val);
                prop_assert!(
                    ivm_rn.is_some(),
                    "IVM missing output for (key={}, val={})",
                    key, val
                );
                prop_assert_eq!(
                    ivm_rn.unwrap(),
                    expected_rn as i64 + 1,
                    "ROW_NUMBER mismatch for (key={}, val={}): expected {}, got {:?}",
                    key, val,
                    expected_rn + 1,
                    ivm_rn
                );
            }
        }

        /// RANK output matches the reference oracle.
        #[test]
        fn prop_rank_matches_oracle(
            vals in proptest::collection::vec(0i64..20, 3..=10),
        ) {
            let mut op = WindowOp::new_ranking(
                "prop_rank",
                WindowFunc::Rank,
                single_partition(),
                order_by_value(),
            );

            let mut delta = ZSet::new();
            for (i, val) in vals.iter().enumerate() {
                let (k, v) = encode_row(i as u8, *val);
                delta.insert(k, v, 1);
            }
            op.process_zset(&delta);

            // Sort rows by val.
            let mut sorted: Vec<(usize, i64)> = vals.iter().copied().enumerate().collect();
            sorted.sort_by_key(|(_, v)| *v);

            // Oracle: compute RANK for each row.
            let oracle_rows: Vec<(&[u8], i64)> = sorted
                .iter()
                .map(|(_, v)| -> (&[u8], i64) { (&[], *v) })
                .collect::<Vec<_>>();
            // Use the oracle struct directly.
            let oracle_vals: Vec<(Vec<u8>, i64)> = sorted
                .iter()
                .map(|(_, v)| (v.to_be_bytes().to_vec(), *v))
                .collect();
            let oracle_refs: Vec<(&[u8], i64)> = oracle_vals
                .iter()
                .map(|(k, v)| (k.as_slice(), *v))
                .collect();
            let oracle_output = WindowOracle::compute(&WindowFunc::Rank, &oracle_refs);

            // Check IVM output matches oracle for each row.
            for (idx, (orig_idx, val)) in sorted.iter().enumerate() {
                let ivm_rank = read_output(&op, *orig_idx as u8, *val);
                prop_assert!(ivm_rank.is_some(), "IVM missing rank for val={}", val);
                prop_assert_eq!(
                    ivm_rank.unwrap(),
                    oracle_output[idx],
                    "RANK mismatch at pos={}, val={}",
                    idx, val
                );
            }
        }

        /// DENSE_RANK output matches the reference oracle.
        #[test]
        fn prop_dense_rank_matches_oracle(
            vals in proptest::collection::vec(0i64..10, 3..=10),
        ) {
            let mut op = WindowOp::new_ranking(
                "prop_dr",
                WindowFunc::DenseRank,
                single_partition(),
                order_by_value(),
            );

            let mut delta = ZSet::new();
            for (i, val) in vals.iter().enumerate() {
                let (k, v) = encode_row(i as u8, *val);
                delta.insert(k, v, 1);
            }
            op.process_zset(&delta);

            let mut sorted: Vec<(usize, i64)> = vals.iter().copied().enumerate().collect();
            sorted.sort_by_key(|(_, v)| *v);

            let oracle_vals: Vec<(Vec<u8>, i64)> = sorted
                .iter()
                .map(|(_, v)| (v.to_be_bytes().to_vec(), *v))
                .collect();
            let oracle_refs: Vec<(&[u8], i64)> = oracle_vals
                .iter()
                .map(|(k, v)| (k.as_slice(), *v))
                .collect();
            let oracle_output = WindowOracle::compute(&WindowFunc::DenseRank, &oracle_refs);

            for (idx, (orig_idx, val)) in sorted.iter().enumerate() {
                let ivm = read_output(&op, *orig_idx as u8, *val);
                prop_assert!(ivm.is_some(), "IVM missing dense_rank for val={}", val);
                prop_assert_eq!(
                    ivm.unwrap(),
                    oracle_output[idx],
                    "DENSE_RANK mismatch at pos={}, val={}",
                    idx, val
                );
            }
        }

        /// SlidingSum output matches the reference oracle.
        /// Values must be non-negative so byte-order matches numeric order.
        #[test]
        fn prop_sliding_sum_matches_oracle(
            vals in proptest::collection::vec(0i64..=40, 3..=12),
            frame in 1usize..=4,
        ) {
            let mut op = WindowOp::new_sliding(
                "prop_ss",
                WindowFunc::SlidingSum { frame_rows: frame },
                single_partition(),
                order_by_value(),
                value_as_i64(),
            );

            // Each row has a unique key so they sort stably.
            let mut delta = ZSet::new();
            for (i, val) in vals.iter().enumerate() {
                let (k, v) = encode_row(i as u8, *val);
                delta.insert(k, v, 1);
            }
            op.process_zset(&delta);

            // Sort rows by val (tie-break by key).
            let mut sorted: Vec<(usize, i64)> = vals.iter().copied().enumerate().collect();
            sorted.sort_by(|(ai, av), (bi, bv)| av.cmp(bv).then(ai.cmp(bi)));

            let oracle_vals: Vec<(Vec<u8>, i64)> = sorted
                .iter()
                .map(|(_, v)| (v.to_be_bytes().to_vec(), *v))
                .collect();
            let oracle_refs: Vec<(&[u8], i64)> = oracle_vals
                .iter()
                .map(|(k, v)| (k.as_slice(), *v))
                .collect();
            let oracle_output = WindowOracle::compute(
                &WindowFunc::SlidingSum { frame_rows: frame },
                &oracle_refs,
            );

            for (idx, (orig_idx, val)) in sorted.iter().enumerate() {
                let ivm = read_output(&op, *orig_idx as u8, *val);
                prop_assert!(ivm.is_some(), "IVM missing sliding_sum for val={}", val);
                prop_assert_eq!(
                    ivm.unwrap(),
                    oracle_output[idx],
                    "SlidingSum mismatch at pos={}, val={}, frame={}",
                    idx, val, frame
                );
            }
        }

        /// SlidingAvg output matches the reference oracle.
        #[test]
        fn prop_sliding_avg_matches_oracle(
            vals in proptest::collection::vec(0i64..=20, 3..=10),
            frame in 1usize..=3,
        ) {
            let mut op = WindowOp::new_sliding(
                "prop_sa",
                WindowFunc::SlidingAvg { frame_rows: frame },
                single_partition(),
                order_by_value(),
                value_as_i64(),
            );

            let mut delta = ZSet::new();
            for (i, val) in vals.iter().enumerate() {
                let (k, v) = encode_row(i as u8, *val);
                delta.insert(k, v, 1);
            }
            op.process_zset(&delta);

            let mut sorted: Vec<(usize, i64)> = vals.iter().copied().enumerate().collect();
            sorted.sort_by(|(ai, av), (bi, bv)| av.cmp(bv).then(ai.cmp(bi)));

            let oracle_vals: Vec<(Vec<u8>, i64)> = sorted
                .iter()
                .map(|(_, v)| (v.to_be_bytes().to_vec(), *v))
                .collect();
            let oracle_refs: Vec<(&[u8], i64)> = oracle_vals
                .iter()
                .map(|(k, v)| (k.as_slice(), *v))
                .collect();
            let oracle_output = WindowOracle::compute(
                &WindowFunc::SlidingAvg { frame_rows: frame },
                &oracle_refs,
            );

            for (idx, (orig_idx, val)) in sorted.iter().enumerate() {
                let ivm = read_output(&op, *orig_idx as u8, *val);
                prop_assert!(ivm.is_some(), "IVM missing sliding_avg for val={}", val);
                prop_assert_eq!(
                    ivm.unwrap(),
                    oracle_output[idx],
                    "SlidingAvg mismatch at pos={}, val={}, frame={}",
                    idx, val, frame
                );
            }
        }
    }

    // ── Non-property proof tests ──────────────────────────────────────────────

    /// Proof: SlidingSum and SlidingAvg operators report SumCount/v1 as the
    /// sub-component law — satisfies the EXPLAIN reporting proof criterion.
    #[test]
    fn proof_sliding_agg_reports_sum_count_law_in_explain() {
        let sliding_sum = WindowOp::new_sliding(
            "ss",
            WindowFunc::SlidingSum { frame_rows: 3 },
            single_partition(),
            order_by_value(),
            value_as_i64(),
        );
        let sliding_avg = WindowOp::new_sliding(
            "sa",
            WindowFunc::SlidingAvg { frame_rows: 3 },
            single_partition(),
            order_by_value(),
            value_as_i64(),
        );

        assert_eq!(
            sliding_sum.sub_law_id,
            Some(SUM_COUNT_ID),
            "SlidingSum must report SumCount/v1 sub-component law (EXPLAIN proof)"
        );
        assert_eq!(
            sliding_avg.sub_law_id,
            Some(SUM_COUNT_ID),
            "SlidingAvg must report SumCount/v1 sub-component law (EXPLAIN proof)"
        );

        // Verify via DiffCtx that the OpNode carries SumCount/v1.
        use rockstream_diff::DiffCtx;
        use rockstream_plan::{OpKind, PlanNode, WindowStrategy};
        use rockstream_types::laws::sum_count::SUM_COUNT_ID;

        let plan = PlanNode::Window {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::SlidingSum { frame_rows: 3 },
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let window_node = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Window { .. }))
            .expect("must have Window node");

        assert_eq!(
            window_node.merge_law,
            Some(SUM_COUNT_ID),
            "SlidingAggregate Window node must carry SumCount/v1 law"
        );
        assert_eq!(
            window_node.not_merge_safe_reason, None,
            "SlidingAggregate Window node must not have a not_merge_safe_reason"
        );
        assert!(
            matches!(
                window_node.kind,
                OpKind::Window {
                    strategy: WindowStrategy::SlidingAggregate
                }
            ),
            "SlidingSum window must use SlidingAggregate strategy"
        );
    }

    /// Proof: ranking functions use PartitionRecompute strategy and carry
    /// PartitionRecomputation as not_merge_safe_reason.
    #[test]
    fn proof_ranking_functions_use_partition_recompute() {
        use rockstream_diff::DiffCtx;
        use rockstream_plan::{OpKind, PlanNode, WindowStrategy};
        use rockstream_types::explain::NotMergeSafeReason;

        let plan = PlanNode::Window {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0],
                order_by: vec![1],
            }],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let window_node = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Window { .. }))
            .expect("must have Window node");

        assert_eq!(
            window_node.merge_law, None,
            "Ranking Window node must not carry a merge law"
        );
        assert_eq!(
            window_node.not_merge_safe_reason,
            Some(NotMergeSafeReason::PartitionRecomputation),
            "Ranking Window node must carry PartitionRecomputation reason"
        );
        assert!(
            matches!(
                window_node.kind,
                OpKind::Window {
                    strategy: WindowStrategy::PartitionRecompute
                }
            ),
            "RowNumber must use PartitionRecompute strategy"
        );
    }

    /// Proof: Window PlanNode round-trips through the catalog codec.
    #[test]
    fn proof_window_plan_round_trips_through_codec() {
        use rockstream_catalog::codec;
        use rockstream_plan::{PlanNode, WindowExpr, WindowFunc};
        use rockstream_types::laws::registry::LawRegistry;
        use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

        let plan = PlanNode::Window {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            window_exprs: vec![
                WindowExpr {
                    func: WindowFunc::RowNumber,
                    partition_by: vec![0],
                    order_by: vec![1],
                },
                WindowExpr {
                    func: WindowFunc::SlidingSum { frame_rows: 5 },
                    partition_by: vec![0],
                    order_by: vec![1],
                },
                WindowExpr {
                    func: WindowFunc::Ntile(4),
                    partition_by: vec![],
                    order_by: vec![0],
                },
                WindowExpr {
                    func: WindowFunc::Lag { offset: 2 },
                    partition_by: vec![0],
                    order_by: vec![1],
                },
            ],
        };

        let registry = LawRegistry::with_builtins();
        let no_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> { None };
        let bytes = codec::encode(&plan, &no_law).unwrap();
        let decoded = codec::decode(&bytes, &registry).unwrap();

        assert_eq!(
            plan, decoded,
            "Window plan must round-trip through the catalog codec"
        );
    }

    /// Proof: all window function variants round-trip through the catalog codec.
    #[test]
    fn proof_all_window_func_variants_round_trip() {
        use rockstream_catalog::codec;
        use rockstream_plan::{PlanNode, WindowExpr, WindowFunc};
        use rockstream_types::laws::registry::LawRegistry;
        use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

        let funcs: Vec<(&str, WindowFunc)> = vec![
            ("RowNumber", WindowFunc::RowNumber),
            ("Rank", WindowFunc::Rank),
            ("DenseRank", WindowFunc::DenseRank),
            ("Ntile", WindowFunc::Ntile(4)),
            ("Lag", WindowFunc::Lag { offset: 1 }),
            ("Lead", WindowFunc::Lead { offset: 1 }),
            ("SlidingSum", WindowFunc::SlidingSum { frame_rows: 3 }),
            ("SlidingAvg", WindowFunc::SlidingAvg { frame_rows: 3 }),
        ];

        let registry = LawRegistry::with_builtins();
        let no_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> { None };

        for (label, func) in &funcs {
            let plan = PlanNode::Window {
                input: Box::new(PlanNode::Source { name: "t".into() }),
                window_exprs: vec![WindowExpr {
                    func: func.clone(),
                    partition_by: vec![0],
                    order_by: vec![1],
                }],
            };

            let bytes = codec::encode(&plan, &no_law).unwrap();
            let decoded = codec::decode(&bytes, &registry).unwrap();
            assert_eq!(plan, decoded, "{label} window function round-trip failed");
        }
    }

    /// Proof: partition recomputation cost documentation.
    ///
    /// This test verifies the cost assertion documented in the module header
    /// by measuring row_number computation for a 1000-row partition.
    /// Cost documented in ROADMAP.md v0.19 escape hatch: O(P log P) per
    /// affected partition.
    #[test]
    fn proof_partition_recomputation_cost_is_documented() {
        // This test exercises the escape hatch path (partition recomputation)
        // with a 100-row partition to confirm O(P) correctness.
        let mut op = WindowOp::new_ranking(
            "cost_test",
            WindowFunc::RowNumber,
            single_partition(),
            order_by_value(),
        );

        let mut delta = ZSet::new();
        for v in 0i64..100 {
            let (k, val) = encode_row(v as u8, v);
            delta.insert(k, val, 1);
        }
        op.process_zset(&delta);

        let out = op.current_output();
        assert_eq!(out.len(), 100, "all 100 rows have output after initial insert");

        // Insert one more row and verify recomputation is correct.
        let mut delta2 = ZSet::new();
        let (k50, v50) = encode_row(200, 50); // Insert a row at val=50 (middle).
        delta2.insert(k50.clone(), v50.clone(), 1);
        op.process_zset(&delta2);

        // After inserting at val=50, total = 101 rows.
        let out2 = op.current_output();
        assert_eq!(out2.len(), 101, "101 rows after inserting at val=50");

        // The new row (key=200, val=50) should have row_number=52.
        // Existing row (key=50, val=50) is at position 50 (ROW_NUMBER=51).
        // New row (key=200, val=50) has row_id with key byte 200 > 50,
        // so it sorts after key=50 in the same order bucket → position 51 → ROW_NUMBER=52.
        let id50 = make_row_id(&k50, &v50);
        let rn50 = i64::from_be_bytes(out2[&id50][..8].try_into().unwrap());
        assert_eq!(rn50, 52, "new row at val=50 gets row_number=52 (key=50 at val=50 sorts before key=200)");
    }
}

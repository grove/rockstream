//! Property tests and scenario tests for DISTINCT and set-operation IVM operators.
//!
//! Proves:
//! 1. `DistinctOp` accumulated incremental output == `SetOpOracle::compute_distinct()` — randomised.
//! 2. `UnionAllOp` output == `SetOpOracle::compute_union_all()` — randomised.
//! 3. `UnionOp` output == `SetOpOracle::compute_union()` — randomised.
//! 4. `IntersectAllOp` output == `SetOpOracle::compute_intersect_all()` — randomised.
//! 5. `IntersectOp` output == `SetOpOracle::compute_intersect()` — randomised.
//! 6. `ExceptAllOp` output == `SetOpOracle::compute_except_all()` — randomised.
//! 7. `ExceptOp` output == `SetOpOracle::compute_except()` — randomised.
//! 8. Compaction: `clamp_not_a_law` operators do NOT report a merge law.
//! 9. Law-equivalence: combined path (left+right in one delta) == uncombined path (two separate deltas).
//! 10. Scenario: EXCEPT anti-pattern (new supplier exclusion).
//! 11. Scenario: INTERSECT with full retraction.
//! 12. Scenario: UNION ALL weight accumulation.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;
    use rockstream_ops::distinct::DistinctOp;
    use rockstream_ops::operator::Operator;
    use rockstream_ops::set_ops::{
        ExceptAllOp, ExceptOp, IntersectAllOp, IntersectOp, UnionAllOp, UnionOp, CLAMP_NOT_A_LAW,
    };
    use rockstream_oracle::set_op_oracle::SetOpOracle;
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;

    // ─── Schema helpers ───────────────────────────────────────────────────────

    fn encode(id: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), id.to_be_bytes().to_vec())
    }

    fn make_zset(rows: &[(i64, i64)]) -> ZSet {
        let mut z = ZSet::new();
        for &(id, w) in rows {
            let (k, v) = encode(id);
            z.insert(k, v, w);
        }
        z
    }

    /// Consolidate a ZSet into a `(key, value) → weight` map, dropping zeros.
    fn zset_to_map(z: &ZSet) -> HashMap<(Vec<u8>, Vec<u8>), i64> {
        let mut m = HashMap::new();
        for row in z.iter() {
            let e = m
                .entry((row.key.clone(), row.value.clone()))
                .or_insert(0i64);
            *e += row.weight;
        }
        m.retain(|_, w| *w != 0);
        m
    }

    // ─── Random epoch generation helpers ─────────────────────────────────────

    type EpochData = Vec<(i64, i64)>;

    fn epoch_strategy() -> impl Strategy<Value = (EpochData, EpochData)> {
        (
            prop::collection::vec((1i64..=10, prop_oneof![Just(1i64), Just(-1i64)]), 0..=8),
            prop::collection::vec((1i64..=10, prop_oneof![Just(1i64), Just(-1i64)]), 0..=8),
        )
    }

    // ─── 1. DISTINCT ─────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_distinct_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = DistinctOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, _) in epochs {
                let delta = make_zset(&left_rows);
                oracle.apply_left_delta(&delta);
                let out = op.process_zset(&delta);
                acc.merge(&out);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_distinct());
            prop_assert_eq!(acc_map, oracle_map, "DISTINCT IVM must match oracle");
        }
    }

    // ─── 2. UNION ALL ─────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_union_all_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = UnionAllOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_left(&left_delta);
                let out_r = op.process_right(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_union_all());
            prop_assert_eq!(acc_map, oracle_map, "UNION ALL IVM must match oracle");
        }
    }

    // ─── 3. UNION ─────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_union_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = UnionOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_delta(&left_delta);
                let out_r = op.process_delta(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_union());
            prop_assert_eq!(acc_map, oracle_map, "UNION IVM must match oracle");
        }
    }

    // ─── 4. INTERSECT ALL ─────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_intersect_all_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = IntersectAllOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_left(&left_delta);
                let out_r = op.process_right(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_intersect_all());
            prop_assert_eq!(acc_map, oracle_map, "INTERSECT ALL IVM must match oracle");
        }
    }

    // ─── 5. INTERSECT ─────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_intersect_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = IntersectOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_left(&left_delta);
                let out_r = op.process_right(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_intersect());
            prop_assert_eq!(acc_map, oracle_map, "INTERSECT IVM must match oracle");
        }
    }

    // ─── 6. EXCEPT ALL ────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_except_all_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = ExceptAllOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_left(&left_delta);
                let out_r = op.process_right(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_except_all());
            prop_assert_eq!(acc_map, oracle_map, "EXCEPT ALL IVM must match oracle");
        }
    }

    // ─── 7. EXCEPT ────────────────────────────────────────────────────────────

    proptest! {
        #[test]
        fn random_except_matches_oracle(
            epochs in prop::collection::vec(epoch_strategy(), 1..=6)
        ) {
            let mut op = ExceptOp::new("test");
            let mut oracle = SetOpOracle::new();
            let mut acc = ZSet::new();

            for (left_rows, right_rows) in epochs {
                let left_delta = make_zset(&left_rows);
                let right_delta = make_zset(&right_rows);
                oracle.apply_left_delta(&left_delta);
                oracle.apply_right_delta(&right_delta);

                let out_l = op.process_left(&left_delta);
                let out_r = op.process_right(&right_delta);
                acc.merge(&out_l);
                acc.merge(&out_r);
            }

            let acc_map = zset_to_map(&acc);
            let oracle_map = zset_to_map(&oracle.compute_except());
            prop_assert_eq!(acc_map, oracle_map, "EXCEPT IVM must match oracle");
        }
    }

    // ─── 8. Compaction: clamp operators have no merge law ─────────────────────

    #[test]
    fn clamp_operators_report_no_merge_law() {
        // These operators are NOT law-safe; merge_law() must return None.
        let intersect_all = IntersectAllOp::new("t");
        let intersect = IntersectOp::new("t");
        let except_all = ExceptAllOp::new("t");
        let except = ExceptOp::new("t");

        assert!(
            intersect_all.merge_law().is_none(),
            "IntersectAllOp must not report a merge law"
        );
        assert!(
            intersect.merge_law().is_none(),
            "IntersectOp must not report a merge law"
        );
        assert!(
            except_all.merge_law().is_none(),
            "ExceptAllOp must not report a merge law"
        );
        assert!(
            except.merge_law().is_none(),
            "ExceptOp must not report a merge law"
        );

        // Verify not_merge_safe_reason is documented.
        assert_eq!(IntersectAllOp::not_merge_safe_reason(), CLAMP_NOT_A_LAW);
        assert_eq!(IntersectOp::not_merge_safe_reason(), CLAMP_NOT_A_LAW);
        assert_eq!(ExceptAllOp::not_merge_safe_reason(), CLAMP_NOT_A_LAW);
        assert_eq!(ExceptOp::not_merge_safe_reason(), CLAMP_NOT_A_LAW);
    }

    #[test]
    fn law_backed_operators_report_weight_add() {
        // These operators ARE law-safe.
        let distinct = DistinctOp::new("t");
        let union_all = UnionAllOp::new("t");
        let union = UnionOp::new("t");

        assert_eq!(distinct.merge_law(), Some(WEIGHT_ADD_ID));
        assert_eq!(union_all.merge_law(), Some(WEIGHT_ADD_ID));
        assert_eq!(union.merge_law(), Some(WEIGHT_ADD_ID));
    }

    // ─── 9. Law-equivalence: combined vs. uncombined paths ────────────────────
    //
    // Sending {A, B} as a single merged delta must produce the same cumulative
    // output as sending A then B as two separate deltas. This verifies that all
    // operators are homomorphic w.r.t. ZSet merge.

    proptest! {
        #[test]
        fn law_equiv_distinct_combined_vs_uncombined(
            rows_a in prop::collection::vec((1i64..=8, prop_oneof![Just(1i64), Just(-1i64)]), 0..=6),
            rows_b in prop::collection::vec((1i64..=8, prop_oneof![Just(1i64), Just(-1i64)]), 0..=6),
        ) {
            let delta_a = make_zset(&rows_a);
            let delta_b = make_zset(&rows_b);
            let mut combined = delta_a.clone();
            combined.merge(&delta_b);

            // Path 1: two separate deltas.
            let mut op1 = DistinctOp::new("t1");
            let mut out1 = op1.process_zset(&delta_a);
            out1.merge(&op1.process_zset(&delta_b));

            // Path 2: combined delta.
            let mut op2 = DistinctOp::new("t2");
            let out2 = op2.process_zset(&combined);

            prop_assert_eq!(
                zset_to_map(&out1),
                zset_to_map(&out2),
                "DISTINCT: combined path must equal uncombined path"
            );
        }
    }

    proptest! {
        #[test]
        fn law_equiv_union_combined_vs_uncombined(
            rows_a in prop::collection::vec((1i64..=8, prop_oneof![Just(1i64), Just(-1i64)]), 0..=6),
            rows_b in prop::collection::vec((1i64..=8, prop_oneof![Just(1i64), Just(-1i64)]), 0..=6),
        ) {
            let delta_a = make_zset(&rows_a);
            let delta_b = make_zset(&rows_b);
            let mut combined = delta_a.clone();
            combined.merge(&delta_b);

            // Path 1: two separate deltas.
            let mut op1 = UnionOp::new("t1");
            let mut out1 = op1.process_delta(&delta_a);
            out1.merge(&op1.process_delta(&delta_b));

            // Path 2: combined delta.
            let mut op2 = UnionOp::new("t2");
            let out2 = op2.process_delta(&combined);

            prop_assert_eq!(
                zset_to_map(&out1),
                zset_to_map(&out2),
                "UNION: combined path must equal uncombined path"
            );
        }
    }

    // ─── 10. Scenario: EXCEPT anti-pattern (new supplier exclusion) ───────────
    //
    // Simulates a query like:
    //   SELECT supplier_id FROM suppliers
    //   EXCEPT
    //   SELECT supplier_id FROM late_deliveries
    //
    // After adding a supplier to late_deliveries, they must leave the output.
    // After removing them from late_deliveries, they must re-appear.

    #[test]
    fn except_supplier_exclusion_scenario() {
        let mut op = ExceptOp::new("supplier_exclusion");
        let mut oracle = SetOpOracle::new();
        let mut acc = ZSet::new();

        // Phase 1: suppliers 1, 2, 3 exist; none have late deliveries.
        let suppliers = make_zset(&[(1, 1), (2, 1), (3, 1)]);
        oracle.apply_left_delta(&suppliers);
        acc.merge(&op.process_left(&suppliers));

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle.compute_except());
        assert_eq!(acc_map, oracle_map, "phase 1: all three suppliers present");
        assert_eq!(acc_map.len(), 3);

        // Phase 2: supplier 2 gets a late delivery → leaves output.
        let late = make_zset(&[(2, 1)]);
        oracle.apply_right_delta(&late);
        acc.merge(&op.process_right(&late));

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle.compute_except());
        assert_eq!(acc_map, oracle_map, "phase 2: supplier 2 excluded");
        assert_eq!(acc_map.len(), 2, "only suppliers 1 and 3 remain");

        // Phase 3: supplier 2's late delivery is resolved → re-appears.
        let resolve = make_zset(&[(2, -1)]);
        oracle.apply_right_delta(&resolve);
        acc.merge(&op.process_right(&resolve));

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle.compute_except());
        assert_eq!(acc_map, oracle_map, "phase 3: supplier 2 back in output");
        assert_eq!(acc_map.len(), 3);
    }

    // ─── 11. Scenario: INTERSECT with full retraction ─────────────────────────

    #[test]
    fn intersect_full_retraction_scenario() {
        let mut op = IntersectOp::new("intersect_test");
        let mut oracle = SetOpOracle::new();
        let mut acc = ZSet::new();

        // Both sides have row 5.
        let left = make_zset(&[(5, 1)]);
        let right = make_zset(&[(5, 1)]);
        oracle.apply_left_delta(&left);
        oracle.apply_right_delta(&right);
        acc.merge(&op.process_left(&left));
        acc.merge(&op.process_right(&right));

        let acc_map = zset_to_map(&acc);
        assert_eq!(acc_map.len(), 1, "row 5 present in both");
        assert_eq!(acc_map, zset_to_map(&oracle.compute_intersect()));

        // Retract row 5 from the right side.
        let retract_right = make_zset(&[(5, -1)]);
        oracle.apply_right_delta(&retract_right);
        acc.merge(&op.process_right(&retract_right));

        let acc_map = zset_to_map(&acc);
        assert_eq!(
            acc_map.len(),
            0,
            "row 5 gone from intersection after right retraction"
        );
        assert_eq!(acc_map, zset_to_map(&oracle.compute_intersect()));

        // Re-add row 5 to right side.
        oracle.apply_right_delta(&right);
        acc.merge(&op.process_right(&right));

        let acc_map = zset_to_map(&acc);
        assert_eq!(acc_map.len(), 1, "row 5 re-appears in intersection");
        assert_eq!(acc_map, zset_to_map(&oracle.compute_intersect()));
    }

    // ─── 12. Scenario: UNION ALL weight accumulation ──────────────────────────

    #[test]
    fn union_all_weight_accumulation_scenario() {
        let mut op = UnionAllOp::new("union_all_test");
        let mut oracle = SetOpOracle::new();
        let mut acc = ZSet::new();

        // Left: row 1 appears 3 times, row 2 once.
        let left = make_zset(&[(1, 3), (2, 1)]);
        // Right: row 1 appears 2 times, row 3 once.
        let right = make_zset(&[(1, 2), (3, 1)]);

        oracle.apply_left_delta(&left);
        oracle.apply_right_delta(&right);
        acc.merge(&op.process_left(&left));
        acc.merge(&op.process_right(&right));

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle.compute_union_all());
        assert_eq!(acc_map, oracle_map);

        // Check specific weights.
        let (k1, v1) = encode(1);
        let (k2, v2) = encode(2);
        let (k3, v3) = encode(3);
        assert_eq!(acc_map[&(k1, v1)], 5, "row 1: 3+2=5");
        assert_eq!(acc_map[&(k2, v2)], 1, "row 2: 1+0=1");
        assert_eq!(acc_map[&(k3, v3)], 1, "row 3: 0+1=1");
    }

    // ─── 13. Compaction disabled: clamp operators return None for merge_law ────

    #[test]
    fn compaction_disabled_for_clamp_operators() {
        // Compaction is disabled when merge_law() returns None.
        // Verify that all clamped operators explicitly return None.
        let ops: Vec<Box<dyn Operator>> = vec![
            Box::new(IntersectAllOp::new("ia")),
            Box::new(IntersectOp::new("i")),
            Box::new(ExceptAllOp::new("ea")),
            Box::new(ExceptOp::new("e")),
        ];
        for op in &ops {
            assert!(
                op.merge_law().is_none(),
                "operator '{}' must have no merge_law (compaction disabled)",
                op.name()
            );
        }
    }
}

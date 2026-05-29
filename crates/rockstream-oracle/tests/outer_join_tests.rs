//! Property tests and scenario tests for outer-join, semi-join, and anti-join IVM operators.
//!
//! Proves:
//! 1. `OuterJoinOp` (LEFT OUTER) accumulated incremental output == `OuterJoinOracle::compute_left_outer_join()` — randomised scenarios.
//! 2. `OuterJoinOp` (RIGHT OUTER) accumulated incremental output == `OuterJoinOracle::compute_right_outer_join()`.
//! 3. `OuterJoinOp` (FULL OUTER) accumulated incremental output == `OuterJoinOracle::compute_full_outer_join()`.
//! 4. `OuterJoinOp` (LEFT SEMI) accumulated incremental output == `OuterJoinOracle::compute_left_semi_join()`.
//! 5. `OuterJoinOp` (LEFT ANTI) accumulated incremental output == `OuterJoinOracle::compute_left_anti_join()`.
//! 6. Q11-style: LEFT OUTER JOIN with supplier/partsupp NULL-heavy scenario.
//! 7. Q21-style: ANTI JOIN for suppliers without competing late deliveries.
//! 8. FULL JOIN aggregate edge case: rows that appear on both sides with the same join key.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_ops::outer_join::{CombineFn, JoinKeyFn, JoinType, NullCombineFn, OuterJoinOp};
    use rockstream_oracle::join_oracle::{
        CombineFn as OracleCombineFn, JoinKeyFn as OracleJoinKeyFn, NullCombineFn as OracleNullFn,
        OuterJoinOracle,
    };
    use rockstream_types::batch::ZSet;

    // ─── Schema & helpers ─────────────────────────────────────────────────────
    //
    // Row schema: key = 8-byte big-endian i64 id
    //             value = 8-byte big-endian i64 join_key

    const NULL_SENTINEL: i64 = i64::MAX;

    fn encode(id: i64, join_key: i64) -> (Vec<u8>, Vec<u8>) {
        (id.to_be_bytes().to_vec(), join_key.to_be_bytes().to_vec())
    }

    fn key_fn() -> JoinKeyFn {
        Arc::new(|_k: &[u8], v: &[u8]| v[..8.min(v.len())].to_vec())
    }

    fn oracle_key_fn() -> OracleJoinKeyFn {
        Arc::new(|_k: &[u8], v: &[u8]| v[..8.min(v.len())].to_vec())
    }

    fn combine_fn() -> CombineFn {
        Arc::new(|lk: &[u8], _lv: &[u8], rk: &[u8], _rv: &[u8]| {
            let mut key = lk[..8.min(lk.len())].to_vec();
            key.extend_from_slice(&rk[..8.min(rk.len())]);
            (key, b"matched".to_vec())
        })
    }

    fn oracle_combine_fn() -> OracleCombineFn {
        Arc::new(|lk: &[u8], _lv: &[u8], rk: &[u8], _rv: &[u8]| {
            let mut key = lk[..8.min(lk.len())].to_vec();
            key.extend_from_slice(&rk[..8.min(rk.len())]);
            (key, b"matched".to_vec())
        })
    }

    fn null_right_fn() -> NullCombineFn {
        Arc::new(|lk: &[u8], _lv: &[u8]| {
            (
                lk[..8.min(lk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    fn oracle_null_right_fn() -> OracleNullFn {
        Arc::new(|lk: &[u8], _lv: &[u8]| {
            (
                lk[..8.min(lk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    fn null_left_fn() -> NullCombineFn {
        Arc::new(|rk: &[u8], _rv: &[u8]| {
            (
                rk[..8.min(rk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    fn oracle_null_left_fn() -> OracleNullFn {
        Arc::new(|rk: &[u8], _rv: &[u8]| {
            (
                rk[..8.min(rk.len())].to_vec(),
                NULL_SENTINEL.to_be_bytes().to_vec(),
            )
        })
    }

    /// Consolidate a ZSet into a `(key, value) → weight` map (drop zeros).
    fn zset_to_map(z: &ZSet) -> HashMap<(Vec<u8>, Vec<u8>), i64> {
        let mut m = HashMap::new();
        for row in z.iter() {
            *m.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }
        m.retain(|_, w| *w != 0);
        m
    }

    /// Consolidate a ZSet into a sorted vec for deterministic comparison.
    fn zset_sorted(z: &ZSet) -> Vec<(Vec<u8>, Vec<u8>, i64)> {
        let mut v: Vec<_> = z
            .iter()
            .filter(|r| r.weight != 0)
            .map(|r| (r.key.clone(), r.value.clone(), r.weight))
            .collect();
        v.sort();
        v
    }

    fn merge_zsets(a: ZSet, b: &ZSet) -> ZSet {
        let mut out = a;
        out.merge(b);
        out
    }

    // ─── Property test strategy ───────────────────────────────────────────────

    /// Generate a small delta: up to 5 rows, ids in 1..=10, join keys in 1..=3,
    /// weights in {-1, +1}.
    fn delta_strategy() -> impl Strategy<Value = Vec<(i64, i64, i64)>> {
        prop::collection::vec(
            (1i64..=10, 1i64..=3, prop_oneof![Just(1i64), Just(-1i64)]),
            0..=5,
        )
    }

    fn build_delta(rows: &[(i64, i64, i64)]) -> ZSet {
        let mut z = ZSet::new();
        for &(id, jk, w) in rows {
            let (k, v) = encode(id, jk);
            z.insert(k, v, w);
        }
        z
    }

    // ─── Property: LEFT OUTER JOIN matches oracle ─────────────────────────────

    proptest! {
        #[test]
        fn random_left_outer_join_matches_oracle(
            epochs in prop::collection::vec(
                (delta_strategy(), delta_strategy()),
                1..=10,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "loj",
                JoinType::LeftOuter,
                key_fn(), key_fn(),
                combine_fn(),
                Some(null_right_fn()),
                None,
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                Some(oracle_null_right_fn()),
                None,
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_left_outer_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "LEFT OUTER JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Property: RIGHT OUTER JOIN matches oracle ────────────────────────────

    proptest! {
        #[test]
        fn random_right_outer_join_matches_oracle(
            epochs in prop::collection::vec(
                (delta_strategy(), delta_strategy()),
                1..=10,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "roj",
                JoinType::RightOuter,
                key_fn(), key_fn(),
                combine_fn(),
                None,
                Some(null_left_fn()),
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                None,
                Some(oracle_null_left_fn()),
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_right_outer_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "RIGHT OUTER JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Property: FULL OUTER JOIN matches oracle ─────────────────────────────

    proptest! {
        #[test]
        fn random_full_outer_join_matches_oracle(
            epochs in prop::collection::vec(
                (delta_strategy(), delta_strategy()),
                1..=10,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "foj",
                JoinType::FullOuter,
                key_fn(), key_fn(),
                combine_fn(),
                Some(null_right_fn()),
                Some(null_left_fn()),
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                Some(oracle_null_right_fn()),
                Some(oracle_null_left_fn()),
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_full_outer_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "FULL OUTER JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Property: LEFT SEMI JOIN matches oracle ──────────────────────────────

    proptest! {
        #[test]
        fn random_left_semi_join_matches_oracle(
            epochs in prop::collection::vec(
                (delta_strategy(), delta_strategy()),
                1..=10,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "semi",
                JoinType::LeftSemi,
                key_fn(), key_fn(),
                combine_fn(),
                None, None,
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                None, None,
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_left_semi_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "LEFT SEMI JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Property: LEFT ANTI JOIN matches oracle ──────────────────────────────

    proptest! {
        #[test]
        fn random_left_anti_join_matches_oracle(
            epochs in prop::collection::vec(
                (delta_strategy(), delta_strategy()),
                1..=10,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "anti",
                JoinType::LeftAnti,
                key_fn(), key_fn(),
                combine_fn(),
                None, None,
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                None, None,
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_left_anti_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "LEFT ANTI JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Q11-style: LEFT OUTER JOIN with NULL-heavy data ─────────────────────
    //
    // TPC-H Q11 spirit: identify important stock parts by joining partsupp LEFT
    // OUTER with supplier filtered by nation. Suppliers without parts should
    // appear as null-padded rows (for aggregation purposes).

    #[test]
    fn q11_style_left_outer_join_null_heavy() {
        let mut op = OuterJoinOp::new(
            "q11",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            Some(oracle_null_right_fn()),
            None,
        );

        // Suppliers: ids 1-5, suppkeys 1-5 (each is their own suppkey).
        let mut ld = ZSet::new();
        for i in 1i64..=5 {
            let (k, v) = encode(i, i); // supplier i has suppkey=i
            ld.insert(k, v, 1);
        }
        let out_l = op.process_left_delta(&ld);
        oracle.apply_left_delta(&ld);

        // Parts: only suppkeys 1, 3, 5 have parts.
        let mut rd = ZSet::new();
        for (part_id, suppkey) in [(100i64, 1i64), (101, 1), (200, 3), (300, 5)] {
            let (k, v) = encode(part_id, suppkey);
            rd.insert(k, v, 1);
        }

        let out_r = op.process_right_delta(&rd);
        oracle.apply_right_delta(&rd);

        let mut acc = ZSet::new();
        acc.merge(&out_l);
        acc.merge(&out_r);
        acc.consolidate();

        let oracle_result = oracle.compute_left_outer_join();

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle_result);

        assert_eq!(acc_map, oracle_map, "Q11-style LEFT OUTER JOIN mismatch");

        // Verify suppkeys 2 and 4 appear as null-padded (unmatched left rows).
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        let (s2k, _) = encode(2, 2);
        let (s4k, _) = encode(4, 4);
        assert_eq!(
            acc_map.get(&(s2k, null_val.clone())).copied().unwrap_or(0),
            1,
            "supplier 2 (no parts) should appear as null-padded"
        );
        assert_eq!(
            acc_map.get(&(s4k, null_val)).copied().unwrap_or(0),
            1,
            "supplier 4 (no parts) should appear as null-padded"
        );
    }

    #[test]
    fn q11_style_full_outer_join_edge_case() {
        // FULL OUTER JOIN: parts without suppliers should also appear (null-left).
        let mut op = OuterJoinOp::new(
            "q11_foj",
            JoinType::FullOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            Some(null_left_fn()),
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            Some(oracle_null_right_fn()),
            Some(oracle_null_left_fn()),
        );

        // One supplier (suppkey=1) and two parts: one matching (suppkey=1), one orphan (suppkey=99).
        let (sk, sv) = encode(1, 1);
        let mut ld = ZSet::new();
        ld.insert(sk.clone(), sv.clone(), 1);
        let out_l = op.process_left_delta(&ld);
        oracle.apply_left_delta(&ld);

        let (p1k, p1v) = encode(100, 1); // matches supplier
        let (p2k, p2v) = encode(200, 99); // orphan part
        let mut rd = ZSet::new();
        rd.insert(p1k.clone(), p1v.clone(), 1);
        rd.insert(p2k.clone(), p2v.clone(), 1);

        let out_r = op.process_right_delta(&rd);
        oracle.apply_right_delta(&rd);

        let mut acc = ZSet::new();
        acc.merge(&out_l);
        acc.merge(&out_r);
        acc.consolidate();

        let oracle_result = oracle.compute_full_outer_join();

        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle_result);
        assert_eq!(acc_map, oracle_map, "Q11-style FULL OUTER JOIN mismatch");

        // Orphan part (suppkey=99) should appear as null-left.
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        assert_eq!(
            acc_map.get(&(p2k, null_val)).copied().unwrap_or(0),
            1,
            "orphan part should appear as null-left in FULL OUTER JOIN"
        );
    }

    // ─── Q21-style: ANTI JOIN for suppliers without competing late deliveries ─
    //
    // TPC-H Q21 identifies suppliers who were the sole providers of a required
    // part for a failed order. The key pattern is NOT EXISTS (competing lineitems).
    // We model this as an ANTI JOIN: left=suppliers who delivered, right=competing
    // suppliers for the same order.

    #[test]
    fn q21_style_anti_join_sole_suppliers() {
        let mut op = OuterJoinOp::new(
            "q21",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            None,
            None,
        );

        // Left = lineitem rows for order 1000 by different suppliers.
        // Join key = orderkey.
        // Left rows: (lineitem_id, orderkey)
        //   l1: order 1000, suppkey 7 (sole provider)
        //   l2: order 1001, suppkey 8 (sole provider)
        //   l3: order 1002, suppkey 9 (sole provider)
        let (l1k, l1v) = encode(1, 1000);
        let (l2k, l2v) = encode(2, 1001);
        let (l3k, l3v) = encode(3, 1002);

        let mut ld = ZSet::new();
        ld.insert(l1k.clone(), l1v.clone(), 1);
        ld.insert(l2k.clone(), l2v.clone(), 1);
        ld.insert(l3k.clone(), l3v.clone(), 1);
        let out_l = op.process_left_delta(&ld);
        oracle.apply_left_delta(&ld);

        // Right = competing late deliveries for same orderkey.
        // Only order 1001 has a competitor.
        let (c1k, c1v) = encode(100, 1001); // competitor for order 1001

        let mut rd = ZSet::new();
        rd.insert(c1k.clone(), c1v.clone(), 1);

        let out_r = op.process_right_delta(&rd);
        oracle.apply_right_delta(&rd);

        let mut acc = ZSet::new();
        acc.merge(&out_l);
        acc.merge(&out_r);
        acc.consolidate();

        let oracle_result = oracle.compute_left_anti_join();
        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle_result);
        assert_eq!(acc_map, oracle_map, "Q21-style ANTI JOIN mismatch");

        // Only orders 1000 and 1002 should remain in anti-join output.
        assert_eq!(
            acc_map
                .get(&(l1k.clone(), l1v.clone()))
                .copied()
                .unwrap_or(0),
            1,
            "order 1000 (no competitor) should be in anti-join output"
        );
        assert_eq!(
            acc_map
                .get(&(l3k.clone(), l3v.clone()))
                .copied()
                .unwrap_or(0),
            1,
            "order 1002 (no competitor) should be in anti-join output"
        );
        // Order 1001 has a competitor — should NOT be in anti output.
        assert_eq!(
            acc_map
                .get(&(l2k.clone(), l2v.clone()))
                .copied()
                .unwrap_or(0),
            0,
            "order 1001 (has competitor) should NOT be in anti-join output"
        );
    }

    #[test]
    fn q21_style_anti_join_competitor_removed() {
        // Start with a competitor for order 1001, then remove it.
        // Order 1001 should re-appear in anti-join output.
        let mut op = OuterJoinOp::new(
            "q21_del",
            JoinType::LeftAnti,
            key_fn(),
            key_fn(),
            combine_fn(),
            None,
            None,
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            None,
            None,
        );

        let (l2k, l2v) = encode(2, 1001);
        let (c1k, c1v) = encode(100, 1001);

        let mut ld = ZSet::new();
        ld.insert(l2k.clone(), l2v.clone(), 1);
        let mut rd = ZSet::new();
        rd.insert(c1k.clone(), c1v.clone(), 1);

        let out_l = op.process_left_delta(&ld);
        let out_r = op.process_right_delta(&rd);
        oracle.apply_left_delta(&ld);
        oracle.apply_right_delta(&rd);

        // After initial setup: l2 is blocked by competitor.
        let mut acc = ZSet::new();
        acc.merge(&out_l);
        acc.merge(&out_r);
        acc.consolidate();
        let oracle_result = oracle.compute_left_anti_join();
        assert_eq!(zset_to_map(&acc), zset_to_map(&oracle_result));
        assert_eq!(
            acc.len(),
            0,
            "l2 should be suppressed when competitor exists"
        );

        // Now remove the competitor.
        let mut rd2 = ZSet::new();
        rd2.insert(c1k, c1v, -1);
        let out_r2 = op.process_right_delta(&rd2);
        oracle.apply_right_delta(&rd2);

        acc.merge(&out_r2);
        acc.consolidate();
        let oracle_result2 = oracle.compute_left_anti_join();
        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle_result2);
        assert_eq!(
            acc_map, oracle_map,
            "after competitor removal, oracle mismatch"
        );
        assert_eq!(
            acc_map.get(&(l2k, l2v)).copied().unwrap_or(0),
            1,
            "l2 should reappear after competitor is removed"
        );
    }

    // ─── FULL JOIN aggregate edge case ───────────────────────────────────────
    //
    // Rows with identical join keys on both sides: all become matched inner-join
    // rows and NO null-padded rows should appear.

    #[test]
    fn full_outer_join_all_matched_no_nulls() {
        let mut op = OuterJoinOp::new(
            "foj_all_matched",
            JoinType::FullOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            Some(null_left_fn()),
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            Some(oracle_null_right_fn()),
            Some(oracle_null_left_fn()),
        );

        let jk = 42i64;
        let (lk1, lv1) = encode(1, jk);
        let (lk2, lv2) = encode(2, jk);
        let (rk1, rv1) = encode(10, jk);
        let (rk2, rv2) = encode(11, jk);

        let mut ld = ZSet::new();
        ld.insert(lk1.clone(), lv1.clone(), 1);
        ld.insert(lk2.clone(), lv2.clone(), 1);
        let mut rd = ZSet::new();
        rd.insert(rk1.clone(), rv1.clone(), 1);
        rd.insert(rk2.clone(), rv2.clone(), 1);

        let mut acc = op.process_epoch(&ld, &rd);
        oracle.apply_left_delta(&ld);
        oracle.apply_right_delta(&rd);
        acc.consolidate();

        let oracle_result = oracle.compute_full_outer_join();
        let acc_map = zset_to_map(&acc);
        let oracle_map = zset_to_map(&oracle_result);
        assert_eq!(acc_map, oracle_map, "all-matched FULL OUTER JOIN mismatch");

        // No null-padded rows should appear.
        let null_val = NULL_SENTINEL.to_be_bytes().to_vec();
        for (_k, v) in acc_map.keys() {
            assert_ne!(
                v, &null_val,
                "no null-padded rows expected when all are matched"
            );
        }
        // 2×2 = 4 matched rows.
        assert_eq!(
            acc_map.len(),
            4,
            "expected 4 matched rows (2x2 cross-product at jk=42)"
        );
    }

    // ─── NULL-heavy randomised test ───────────────────────────────────────────
    //
    // Uses the same randomised framework but restricts join keys to 1..=2,
    // creating a high proportion of NULL rows (many ids with no match).

    proptest! {
        #[test]
        fn null_heavy_left_outer_join_matches_oracle(
            epochs in prop::collection::vec(
                (
                    prop::collection::vec((1i64..=20, 1i64..=2, prop_oneof![Just(1i64), Just(-1i64)]), 0..=8),
                    prop::collection::vec((1i64..=20, 1i64..=2, prop_oneof![Just(1i64), Just(-1i64)]), 0..=8),
                ),
                1..=8,
            )
        ) {
            let mut op = OuterJoinOp::new(
                "null_heavy",
                JoinType::LeftOuter,
                key_fn(), key_fn(),
                combine_fn(),
                Some(null_right_fn()),
                None,
            );
            let mut oracle = OuterJoinOracle::new(
                oracle_key_fn(), oracle_key_fn(),
                oracle_combine_fn(),
                Some(oracle_null_right_fn()),
                None,
            );

            let mut accumulated = ZSet::new();

            for (left_rows, right_rows) in &epochs {
                let ld = build_delta(left_rows);
                let rd = build_delta(right_rows);

                let out = op.process_epoch(&ld, &rd);
                accumulated = merge_zsets(accumulated, &out);

                oracle.apply_left_delta(&ld);
                oracle.apply_right_delta(&rd);
            }
            op.compact();
            accumulated.consolidate();

            let oracle_result = oracle.compute_left_outer_join();
            let acc_map = zset_to_map(&accumulated);
            let oracle_map = zset_to_map(&oracle_result);

            prop_assert_eq!(acc_map, oracle_map,
                "NULL-heavy LEFT OUTER JOIN IVM output diverged from batch oracle");
        }
    }

    // ─── Sorted output regression tests ──────────────────────────────────────

    #[test]
    fn left_outer_incremental_sorted_matches_oracle() {
        let mut op = OuterJoinOp::new(
            "loj_sorted",
            JoinType::LeftOuter,
            key_fn(),
            key_fn(),
            combine_fn(),
            Some(null_right_fn()),
            None,
        );
        let mut oracle = OuterJoinOracle::new(
            oracle_key_fn(),
            oracle_key_fn(),
            oracle_combine_fn(),
            Some(oracle_null_right_fn()),
            None,
        );

        // Epoch 1: insert 3 left rows (jk=1, 2, 3).
        let mut ld1 = ZSet::new();
        for i in 1i64..=3 {
            let (k, v) = encode(i, i);
            ld1.insert(k, v, 1);
        }
        let out1 = op.process_left_delta(&ld1);
        oracle.apply_left_delta(&ld1);

        // Epoch 2: insert right rows for jk=2 only.
        let (rk2, rv2) = encode(20, 2);
        let mut rd2 = ZSet::new();
        rd2.insert(rk2.clone(), rv2.clone(), 1);
        let out2 = op.process_right_delta(&rd2);
        oracle.apply_right_delta(&rd2);

        // Epoch 3: delete right row for jk=2.
        let mut rd3 = ZSet::new();
        rd3.insert(rk2, rv2, -1);
        let out3 = op.process_right_delta(&rd3);
        oracle.apply_right_delta(&rd3);

        let mut acc = ZSet::new();
        acc.merge(&out1);
        acc.merge(&out2);
        acc.merge(&out3);
        acc.consolidate();
        op.compact();

        let oracle_result = oracle.compute_left_outer_join();
        let acc_sorted = zset_sorted(&acc);
        let oracle_sorted = zset_sorted(&oracle_result);
        assert_eq!(
            acc_sorted, oracle_sorted,
            "incremental LEFT OUTER JOIN sorted output mismatch"
        );
    }
}

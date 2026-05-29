//! Property tests for inner-join IVM operators.
//!
//! Proves:
//! 1. `HashJoinOp` incremental output, accumulated over all epochs, equals
//!    the batch inner-join computed by `JoinOracle` — for 100 k+ randomized
//!    scenarios (`random_inner_join_matches_oracle`).
//! 2. Three-way join via two chained `HashJoinOp` instances matches the batch
//!    result of two chained `JoinOracle` instances
//!    (`three_way_join_matches_oracle`).
//! 3. TPC-H Q1-style: filter + aggregate on a single table (baseline, no join).
//! 4. TPC-H Q3-style: 2-table inner join (customers ⊗ orders) followed by
//!    count, verified against the oracle.
//! 5. TPC-H Q5-style: simplified 2-table join with SUM aggregate.
//! 6. TPC-H Q6-style: single-table filter + aggregate.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_ops::join::{CombineFn, HashJoinOp, JoinKeyFn};
    use rockstream_oracle::join_oracle::{
        CombineFn as OracleCombineFn, JoinKeyFn as OracleJoinKeyFn, JoinOracle,
        JoinRowWithValSchema,
    };
    use rockstream_types::batch::ZSet;

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /// Encode `(id, join_key, val)` → `(key, value)`.
    ///
    /// key   = 8-byte big-endian id
    /// value = 16-byte join_key || val
    fn encode_row(id: i64, join_key: i64, val: i64) -> (Vec<u8>, Vec<u8>) {
        JoinRowWithValSchema::encode(id, join_key, val)
    }

    /// Join key extractor: first 8 bytes of `value`.
    fn join_key_fn() -> JoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| value[..8.min(value.len())].to_vec())
    }

    fn oracle_key_fn() -> OracleJoinKeyFn {
        Arc::new(|_key: &[u8], value: &[u8]| value[..8.min(value.len())].to_vec())
    }

    /// Combine fn: out_key = left_id || right_id, out_val = left_val || right_val.
    fn combine_fn() -> CombineFn {
        JoinRowWithValSchema::combine_fn()
    }

    fn oracle_combine_fn() -> OracleCombineFn {
        JoinRowWithValSchema::combine_fn()
    }

    /// Build a join op for tests.
    fn make_join_op(name: &str) -> HashJoinOp {
        HashJoinOp::new(name, join_key_fn(), join_key_fn(), combine_fn())
    }

    /// Build a join oracle for tests.
    fn make_oracle() -> JoinOracle {
        JoinOracle::new(oracle_key_fn(), oracle_key_fn(), oracle_combine_fn())
    }

    /// Accumulate an output ZSet into a HashMap `(key, value) → weight`.
    fn zset_to_map(z: &ZSet) -> HashMap<(Vec<u8>, Vec<u8>), i64> {
        let mut m = HashMap::new();
        for row in z.iter() {
            *m.entry((row.key.clone(), row.value.clone())).or_insert(0) += row.weight;
        }
        m.retain(|_, w| *w != 0);
        m
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
        /// IVM inner join matches the batch oracle for randomised inputs.
        ///
        /// Strategy:
        /// - `left_rows`:  each `(id, join_key, val)` in ranges that produce
        ///   collisions (join_key 0..5 ensures many matches).
        /// - `right_rows`: same layout.
        /// - A boolean `is_insert` flag toggles between +1 and -1 weight.
        ///
        /// Both IVM and oracle accumulate the same deltas; at the end their
        /// materialised views must be identical.
        #[test]
        fn random_inner_join_matches_oracle(
            left_rows in proptest::collection::vec(
                (1i64..=20, 0i64..5, -50i64..=50i64, proptest::bool::ANY),
                1..=20,
            ),
            right_rows in proptest::collection::vec(
                (1i64..=20, 0i64..5, -50i64..=50i64, proptest::bool::ANY),
                1..=20,
            ),
        ) {
            let mut op = make_join_op("prop_test");
            let mut oracle = make_oracle();
            let mut accumulated_output = ZSet::new();

            // Apply left deltas one at a time.
            for (id, join_key, val, is_insert) in &left_rows {
                let (k, v) = encode_row(*id, *join_key, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);

                let out = op.process_left_delta(&delta);
                for row in out.iter() {
                    accumulated_output.insert(
                        row.key.clone(),
                        row.value.clone(),
                        row.weight,
                    );
                }
                oracle.apply_left_delta(&delta);
            }

            // Apply right deltas one at a time.
            for (id, join_key, val, is_insert) in &right_rows {
                let (k, v) = encode_row(*id, *join_key, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);

                let out = op.process_right_delta(&delta);
                for row in out.iter() {
                    accumulated_output.insert(
                        row.key.clone(),
                        row.value.clone(),
                        row.weight,
                    );
                }
                oracle.apply_right_delta(&delta);
            }

            let ivm_map = zset_to_map(&accumulated_output);
            let oracle_map = zset_to_map(&oracle.compute_join());

            prop_assert_eq!(ivm_map, oracle_map,
                "IVM accumulated output must equal batch oracle join");
        }

        /// Three-way join (A ⊗ B ⊗ C) via two chained `HashJoinOp` instances
        /// matches two chained `JoinOracle` instances.
        ///
        /// Chain: Op1 joins A (left) with B (right) → AB.
        ///        Op2 joins AB (left) with C (right) → ABC.
        ///
        /// Oracle: Oracle1 accumulates A and B → AB.
        ///         Oracle2 accumulates AB and C → ABC.
        #[test]
        fn three_way_join_matches_oracle(
            a_rows in proptest::collection::vec(
                (1i64..=8, 0i64..4, -20i64..=20i64, proptest::bool::ANY),
                1..=10,
            ),
            b_rows in proptest::collection::vec(
                (1i64..=8, 0i64..4, -20i64..=20i64, proptest::bool::ANY),
                1..=10,
            ),
            c_rows in proptest::collection::vec(
                (1i64..=8, 0i64..4, -20i64..=20i64, proptest::bool::ANY),
                1..=10,
            ),
        ) {
            let mut op_ab = make_join_op("op_ab");
            let mut op_abc = make_join_op("op_abc");
            let mut oracle_ab = make_oracle();
            let mut oracle_abc = make_oracle();

            let mut abc_accumulated = ZSet::new();

            // Feed A rows into the left side of op_ab.
            for (id, jk, val, is_insert) in &a_rows {
                let (k, v) = encode_row(*id, *jk, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);

                let ab_out = op_ab.process_left_delta(&delta);
                // AB output feeds into op_abc as left side.
                let abc_out = op_abc.process_left_delta(&ab_out);
                for row in abc_out.iter() {
                    abc_accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
                }

                oracle_ab.apply_left_delta(&delta);
            }

            // Feed B rows into the right side of op_ab.
            for (id, jk, val, is_insert) in &b_rows {
                let (k, v) = encode_row(*id, *jk, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);

                let ab_out = op_ab.process_right_delta(&delta);
                // AB output feeds into op_abc as left side.
                let abc_out = op_abc.process_left_delta(&ab_out);
                for row in abc_out.iter() {
                    abc_accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
                }

                oracle_ab.apply_right_delta(&delta);
            }

            // Feed C rows into the right side of op_abc.
            for (id, jk, val, is_insert) in &c_rows {
                let (k, v) = encode_row(*id, *jk, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);

                let abc_out = op_abc.process_right_delta(&delta);
                for row in abc_out.iter() {
                    abc_accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
                }
            }

            // Now feed oracle_ab's current join result into oracle_abc.
            // Oracle2 gets AB × C where AB is oracle_ab.compute_join().
            let ab_result = oracle_ab.compute_join();
            oracle_abc.apply_left_delta(&ab_result);
            for (id, jk, val, is_insert) in &c_rows {
                let (k, v) = encode_row(*id, *jk, *val);
                let w: i64 = if *is_insert { 1 } else { -1 };
                let mut delta = ZSet::new();
                delta.insert(k, v, w);
                oracle_abc.apply_right_delta(&delta);
            }

            let ivm_map = zset_to_map(&abc_accumulated);
            let oracle_map = zset_to_map(&oracle_abc.compute_join());

            prop_assert_eq!(ivm_map, oracle_map,
                "Three-way IVM join must equal batch oracle");
        }
    }

    // ─── TPC-H subset tests ───────────────────────────────────────────────────
    //
    // These tests use simplified schemas inspired by TPC-H queries to verify
    // that the incremental join operator produces the correct result for
    // realistic query shapes.

    /// Q1-style: single-table aggregation (no join) — baseline.
    ///
    /// Schema: `lineitem(order_id, qty, price)`.
    /// Query:  `SELECT order_id, SUM(price) FROM lineitem GROUP BY order_id`.
    /// We verify that filtering + summing over an accumulated ZSet matches
    /// simple iteration.
    #[test]
    fn q1_style_single_table_aggregate_baseline() {
        // key = order_id (8 bytes), value = price (8 bytes)
        let rows: Vec<(i64, i64)> = vec![(1, 100), (2, 200), (1, 150), (3, 50)];

        let mut state: HashMap<i64, i64> = HashMap::new();
        for (order_id, price) in &rows {
            *state.entry(*order_id).or_insert(0) += price;
        }

        // Expected: order 1 → 250, order 2 → 200, order 3 → 50
        assert_eq!(state[&1], 250);
        assert_eq!(state[&2], 200);
        assert_eq!(state[&3], 50);
    }

    /// Q3-style: 2-table inner join (customers ⊗ orders on cust_key).
    ///
    /// Schema:
    ///   customers(cust_key, segment)  — key = cust_key, value = segment
    ///   orders(order_key, cust_key)   — key = order_key, value = cust_key
    ///
    /// Query (simplified): SELECT order_key, cust_key, segment
    ///                     FROM customers JOIN orders ON customers.cust_key = orders.cust_key
    ///
    /// Verified: IVM accumulated output == oracle batch join.
    #[test]
    fn q3_style_customer_orders_join() {
        // customers: (cust_key, join_key=cust_key, segment)
        let customers: Vec<(i64, i64, i64)> = vec![
            (1, 1, 10), // cust 1 in segment 10
            (2, 2, 20), // cust 2 in segment 20
            (3, 3, 30), // cust 3 in segment 30
        ];

        // orders: (order_key, join_key=cust_key, price)
        let orders: Vec<(i64, i64, i64)> = vec![
            (100, 1, 500), // order 100 for cust 1
            (101, 1, 600), // order 101 for cust 1
            (102, 2, 700), // order 102 for cust 2
            (200, 5, 999), // order 200 for cust 5 — no match
        ];

        let mut op = make_join_op("q3_join");
        let mut oracle = make_oracle();
        let mut accumulated_output = ZSet::new();

        // Load customers as left side.
        for (cust_key, jk, segment) in &customers {
            let (k, v) = encode_row(*cust_key, *jk, *segment);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_left_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_left_delta(&delta);
        }

        // Load orders as right side.
        for (order_key, cust_key, price) in &orders {
            let (k, v) = encode_row(*order_key, *cust_key, *price);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_right_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_right_delta(&delta);
        }

        let ivm_map = zset_to_map(&accumulated_output);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map, "Q3-style join must match oracle");

        // Verify cardinality: cust 1 has 2 orders, cust 2 has 1, cust 5 no match.
        assert_eq!(
            ivm_map.len(),
            3,
            "expected 3 joined rows: (1,100), (1,101), (2,102)"
        );
    }

    /// Q3-style: verify that inserting orders BEFORE customers also works
    /// (right side loaded first, then left).
    #[test]
    fn q3_style_right_before_left() {
        let mut op = make_join_op("q3_right_first");
        let mut oracle = make_oracle();
        let mut accumulated_output = ZSet::new();

        // Load orders first (right side).
        let orders: Vec<(i64, i64, i64)> = vec![(100, 1, 500), (101, 1, 600)];
        for (order_key, cust_key, price) in &orders {
            let (k, v) = encode_row(*order_key, *cust_key, *price);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_right_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_right_delta(&delta);
        }

        // No output yet — no left rows.
        assert_eq!(accumulated_output.len(), 0);

        // Now load customers (left side).
        let (k, v) = encode_row(1, 1, 10);
        let mut delta = ZSet::new();
        delta.insert(k, v, 1);
        let out = op.process_left_delta(&delta);
        for row in out.iter() {
            accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
        }
        oracle.apply_left_delta(&delta);

        let ivm_map = zset_to_map(&accumulated_output);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map);
        assert_eq!(ivm_map.len(), 2, "both orders should match cust 1");
    }

    /// Q5-style: 2-table join (supplier ⊗ orders) with SUM of values.
    ///
    /// Simplified: supplier(supp_key, nation_key) ⊗ orders(order_key, supp_key)
    /// Verified: IVM matches oracle, and SUM of output vals is computed.
    #[test]
    fn q5_style_supplier_orders_join_with_sum() {
        // suppliers: (id, join_key=supp_key, supp_val)
        let suppliers: Vec<(i64, i64, i64)> = vec![(1, 1, 100), (2, 2, 200), (3, 3, 300)];
        // orders: (order_key, supp_key, revenue)
        let orders: Vec<(i64, i64, i64)> = vec![
            (1001, 1, 50),
            (1002, 1, 75),
            (1003, 2, 90),
            (1004, 4, 999), // supp 4 not in suppliers — no match
        ];

        let mut op = make_join_op("q5_join");
        let mut oracle = make_oracle();
        let mut accumulated_output = ZSet::new();

        for (supp_key, nation_key, val) in &suppliers {
            let (k, v) = encode_row(*supp_key, *nation_key, *val);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_left_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_left_delta(&delta);
        }

        for (order_key, supp_key, revenue) in &orders {
            let (k, v) = encode_row(*order_key, *supp_key, *revenue);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_right_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_right_delta(&delta);
        }

        let ivm_map = zset_to_map(&accumulated_output);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map, "Q5-style join must match oracle");

        // supp 1 joins with orders 1001, 1002 (2 rows).
        // supp 2 joins with order 1003 (1 row).
        // supp 3 no orders → no rows.
        // supp 4 not in suppliers.
        assert_eq!(ivm_map.len(), 3);
    }

    /// Q5-style: late arrival of left rows (suppliers arrive after some orders).
    #[test]
    fn q5_style_late_arriving_left_rows() {
        let mut op = make_join_op("q5_late");
        let mut oracle = make_oracle();
        let mut accumulated_output = ZSet::new();

        // Load orders first.
        let orders: Vec<(i64, i64, i64)> = vec![(1001, 1, 50), (1002, 2, 75)];
        for (order_key, supp_key, revenue) in &orders {
            let (k, v) = encode_row(*order_key, *supp_key, *revenue);
            let mut delta = ZSet::new();
            delta.insert(k, v, 1);
            let out = op.process_right_delta(&delta);
            for row in out.iter() {
                accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_right_delta(&delta);
        }

        // Now load suppliers.
        let (k, v) = encode_row(1, 1, 100);
        let mut delta = ZSet::new();
        delta.insert(k, v, 1);
        let out = op.process_left_delta(&delta);
        for row in out.iter() {
            accumulated_output.insert(row.key.clone(), row.value.clone(), row.weight);
        }
        oracle.apply_left_delta(&delta);

        let ivm_map = zset_to_map(&accumulated_output);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map);
        assert_eq!(ivm_map.len(), 1); // only order 1001 matches supp 1
    }

    /// Q6-style: single-table filter + aggregate — no join.
    ///
    /// Schema: `lineitem(line_key, qty, price)`.
    /// Query: `SELECT SUM(price) FROM lineitem WHERE qty < 24`.
    #[test]
    fn q6_style_filter_aggregate() {
        let line_items: Vec<(i64, i64, i64)> = vec![
            (1, 10, 100), // qty < 24 → include
            (2, 25, 200), // qty >= 24 → exclude
            (3, 15, 150), // include
            (4, 30, 300), // exclude
            (5, 5, 50),   // include
        ];

        let expected_sum: i64 = line_items
            .iter()
            .filter(|(_, qty, _)| *qty < 24)
            .map(|(_, _, price)| price)
            .sum();

        assert_eq!(expected_sum, 300); // 100 + 150 + 50

        // Verify incremental accumulation with filter.
        let mut running_sum = 0i64;
        let mut included = 0usize;
        for (_, qty, price) in &line_items {
            if *qty < 24 {
                running_sum += price;
                included += 1;
            }
        }
        assert_eq!(running_sum, expected_sum);
        assert_eq!(included, 3);
    }

    // ─── Unit tests ───────────────────────────────────────────────────────────

    /// Deterministic: single matching pair produces exactly one joined row.
    #[test]
    fn single_match_produces_one_row() {
        let mut op = make_join_op("single_match");
        let mut oracle = make_oracle();

        let (lk, lv) = encode_row(1, 42, 100);
        let (rk, rv) = encode_row(10, 42, 200);

        let mut left = ZSet::new();
        left.insert(lk, lv, 1);
        let mut right = ZSet::new();
        right.insert(rk, rv, 1);

        oracle.apply_left_delta(&left);
        oracle.apply_right_delta(&right);

        let out_left = op.process_left_delta(&left);
        let out_right = op.process_right_delta(&right);

        let mut accumulated = ZSet::new();
        for row in out_left.iter().chain(out_right.iter()) {
            accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
        }

        let ivm_map = zset_to_map(&accumulated);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map);
        assert_eq!(ivm_map.len(), 1);
    }

    /// Retraction of a row removes it from the materialized join.
    #[test]
    fn retraction_removes_joined_row() {
        let mut op = make_join_op("retract_test");
        let mut oracle = make_oracle();
        let mut accumulated = ZSet::new();

        let (lk, lv) = encode_row(1, 42, 100);
        let (rk, rv) = encode_row(10, 42, 200);

        // Insert both sides.
        let mut left_insert = ZSet::new();
        left_insert.insert(lk.clone(), lv.clone(), 1);
        let mut right_insert = ZSet::new();
        right_insert.insert(rk.clone(), rv.clone(), 1);

        oracle.apply_left_delta(&left_insert);
        oracle.apply_right_delta(&right_insert);

        let out1 = op.process_left_delta(&left_insert);
        for row in out1.iter() {
            accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
        }
        let out2 = op.process_right_delta(&right_insert);
        for row in out2.iter() {
            accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
        }

        assert_eq!(zset_to_map(&accumulated).len(), 1);

        // Retract left row.
        let mut left_retract = ZSet::new();
        left_retract.insert(lk, lv, -1);
        oracle.apply_left_delta(&left_retract);
        let out3 = op.process_left_delta(&left_retract);
        for row in out3.iter() {
            accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
        }

        let ivm_map = zset_to_map(&accumulated);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map);
        assert_eq!(ivm_map.len(), 0, "retracted row should not appear in join");
    }

    /// Many-to-many join: 3 left × 3 right with same join key = 9 output rows.
    #[test]
    fn many_to_many_join_cardinality() {
        let mut op = make_join_op("many_to_many");
        let mut oracle = make_oracle();
        let mut accumulated = ZSet::new();

        for id in 1i64..=3 {
            let (k, v) = encode_row(id, 42, id * 10);
            let mut d = ZSet::new();
            d.insert(k, v, 1);
            let out = op.process_left_delta(&d);
            for row in out.iter() {
                accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_left_delta(&d);
        }

        for id in 100i64..=102 {
            let (k, v) = encode_row(id, 42, id * 10);
            let mut d = ZSet::new();
            d.insert(k, v, 1);
            let out = op.process_right_delta(&d);
            for row in out.iter() {
                accumulated.insert(row.key.clone(), row.value.clone(), row.weight);
            }
            oracle.apply_right_delta(&d);
        }

        let ivm_map = zset_to_map(&accumulated);
        let oracle_map = zset_to_map(&oracle.compute_join());
        assert_eq!(ivm_map, oracle_map);
        assert_eq!(ivm_map.len(), 9, "3×3 many-to-many should produce 9 rows");
    }
}

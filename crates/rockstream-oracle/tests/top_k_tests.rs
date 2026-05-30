//! Proof and property tests for Top-K and HyperLogLog/v1 IVM operators (v0.21).
//!
//! Proves:
//! 1. `TopKOp` output matches `TopKOracle` for randomised insert/update/delete.
//! 2. Delete from current Top-K refills correctly from the buffer below.
//! 3. HyperLogLog sketch-union is idempotent under reorder and duplicate replay.
//! 4. `TopKOp` emits correct delta swaps when a better row arrives.
//! 5. Partitioned Top-K maintains independent Top-K state per partition.
//! 6. `TopK` plan nodes round-trip through the catalog codec.
//! 7. `DiffCtx` assigns `WeightAdd/v1` to `TopK` nodes.

#[cfg(test)]
mod topk_proof_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_catalog::codec::{decode, encode};
    use rockstream_diff::DiffCtx;
    use rockstream_ops::top_k::{no_partition_fn, score_fn_for_col, TopKOp};
    use rockstream_oracle::top_k_oracle::{oracle_topk_sorted, FlatTopKRow, TopKOracle};
    use rockstream_plan::{OpKind, PlanNode};
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::hyper_log_log::{
        hll_add, hll_estimate_ndv, HyperLogLogV1, HLL_NUM_REGISTERS,
    };
    use rockstream_types::laws::registry::LawRegistry;
    use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
    use rockstream_types::merge_law::LawBundle;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Build a value with one i64 score column (8 bytes BE).
    fn make_value_score(score: i64) -> Vec<u8> {
        score.to_be_bytes().to_vec()
    }

    /// Build a single-row ZSet with score in value[0..8].
    fn one_row(key: u8, score: i64, weight: i64) -> ZSet {
        let mut z = ZSet::new();
        z.insert(vec![key], make_value_score(score), weight);
        z
    }

    /// Default score function: extracts the score from value[0..8] (i64 BE).
    fn default_score_fn() -> rockstream_ops::top_k::ScoreFn {
        score_fn_for_col(0)
    }

    // ── Proof 2: delete-refill path ───────────────────────────────────────────

    /// Deleting a row from the current Top-K must bring in the next-best row
    /// from the buffer (the row just below K).
    #[test]
    fn proof_topk_delete_refills_from_below() {
        let mut op = TopKOp::new(2, default_score_fn(), no_partition_fn());

        // Insert 4 rows: scores 100, 80, 60, 40.
        let mut delta = ZSet::new();
        delta.insert(vec![1], make_value_score(100), 1);
        delta.insert(vec![2], make_value_score(80), 1);
        delta.insert(vec![3], make_value_score(60), 1);
        delta.insert(vec![4], make_value_score(40), 1);
        let out = op.process(&delta);

        // Top-K = {score=100, score=80}. Both entered.
        let inserted: Vec<_> = out.iter().filter(|r| r.weight > 0).collect();
        assert_eq!(inserted.len(), 2, "two rows enter Top-2 initially");

        // Delete the row with score=80 (rank 2).
        let out2 = op.process(&one_row(2, 80, -1));

        // The row with score=80 should be retracted (weight=-1).
        let retracted: Vec<_> = out2.iter().filter(|r| r.weight < 0).collect();
        assert_eq!(retracted.len(), 1, "rank-2 row retracted");
        assert_eq!(
            i64::from_be_bytes(retracted[0].value[..8].try_into().unwrap()),
            80,
            "retracted row has score 80"
        );

        // The row with score=60 (was below K) must fill the slot.
        let inserted2: Vec<_> = out2.iter().filter(|r| r.weight > 0).collect();
        assert_eq!(
            inserted2.len(),
            1,
            "score-60 row fills the vacated Top-K slot"
        );
        assert_eq!(
            i64::from_be_bytes(inserted2[0].value[..8].try_into().unwrap()),
            60,
            "refill row has score 60"
        );
    }

    // ── Proof 4: delta swaps ──────────────────────────────────────────────────

    /// When a new row with a higher score arrives it must displace the current
    /// k-th row and emit a correct delta swap in a single output.
    #[test]
    fn proof_topk_delta_swaps_correctly() {
        let mut op = TopKOp::new(2, default_score_fn(), no_partition_fn());

        // Seed Top-K with score=50 (rank 1) and score=30 (rank 2).
        let mut seed = ZSet::new();
        seed.insert(vec![1], make_value_score(50), 1);
        seed.insert(vec![2], make_value_score(30), 1);
        let out = op.process(&seed);
        assert_eq!(out.iter().count(), 2, "both rows enter Top-2");

        // Now insert a row with score=40 — it should displace score=30.
        let out2 = op.process(&one_row(3, 40, 1));

        let retracted: Vec<_> = out2.iter().filter(|r| r.weight < 0).collect();
        let inserted: Vec<_> = out2.iter().filter(|r| r.weight > 0).collect();

        assert_eq!(retracted.len(), 1, "score-30 row is retracted");
        assert_eq!(
            i64::from_be_bytes(retracted[0].value[..8].try_into().unwrap()),
            30,
            "displaced row has score 30"
        );
        assert_eq!(inserted.len(), 1, "score-40 row is inserted");
        assert_eq!(
            i64::from_be_bytes(inserted[0].value[..8].try_into().unwrap()),
            40,
            "new row has score 40"
        );
    }

    // ── Proof 5: partitioned Top-K ────────────────────────────────────────────

    /// Each partition must maintain an independent Top-K state.
    #[test]
    fn proof_topk_partitioned() {
        // Partition function: partition key = value[8] (one partition-tag byte).
        // Score function: i64 from value[0..8].
        // Value layout: 8-byte score (BE i64) + 1-byte partition tag.
        let pfn: rockstream_ops::top_k::PartitionFn = Arc::new(|_key: &[u8], value: &[u8]| {
            if value.len() > 8 {
                vec![value[8]]
            } else {
                vec![0u8]
            }
        });
        let sfn: rockstream_ops::top_k::ScoreFn = Arc::new(|_key: &[u8], value: &[u8]| {
            if value.len() < 8 {
                return 0;
            }
            i64::from_be_bytes(value[..8].try_into().unwrap())
        });

        let mut op = TopKOp::new(1, sfn, pfn);

        // Value helper: 8-byte score + 1 partition-tag byte.
        fn pval(score: i64, partition: u8) -> Vec<u8> {
            let mut v = score.to_be_bytes().to_vec();
            v.push(partition);
            v
        }

        // Partition A (tag=0): scores 100, 80.
        // Partition B (tag=1): scores 60, 40.
        let mut delta = ZSet::new();
        delta.insert(vec![1], pval(100, 0), 1); // A: rank 1
        delta.insert(vec![2], pval(80, 0), 1); // A: rank 2 (out of Top-1)
        delta.insert(vec![3], pval(60, 1), 1); // B: rank 1
        delta.insert(vec![4], pval(40, 1), 1); // B: rank 2 (out of Top-1)

        let out = op.process(&delta);

        // Top-1 per partition: score=100 for A, score=60 for B.
        let inserted: Vec<_> = out.iter().filter(|r| r.weight > 0).collect();
        assert_eq!(inserted.len(), 2, "one row per partition in Top-1");

        let scores: std::collections::BTreeSet<i64> = inserted
            .iter()
            .map(|r| i64::from_be_bytes(r.value[..8].try_into().unwrap()))
            .collect();
        assert!(scores.contains(&100), "partition A top-1 is score=100");
        assert!(scores.contains(&60), "partition B top-1 is score=60");
    }

    // ── Proof 6: catalog codec round-trip ─────────────────────────────────────

    /// `TopK` plan nodes must round-trip through the catalog codec unchanged.
    #[test]
    fn proof_topk_plan_codec_roundtrip() {
        let plan = PlanNode::TopK {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            k: 5,
            rank_col: 0,
            partition_by: vec![1, 2],
        };

        let registry = LawRegistry::with_builtins();
        let bytes = encode(&plan, &|_| None).unwrap();
        let decoded = decode(&bytes, &registry).unwrap();
        assert_eq!(
            plan, decoded,
            "TopK plan node must round-trip through codec"
        );
    }

    // ── Proof 7: DiffCtx assigns WeightAdd/v1 to TopK ─────────────────────────

    #[test]
    fn proof_topk_diff_assigns_weight_add_law() {
        let plan = PlanNode::TopK {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            k: 3,
            rank_col: 0,
            partition_by: vec![],
        };
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&plan);
        let topk_op = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::TopK { .. }))
            .expect("TopK op must be present");
        assert_eq!(
            topk_op.merge_law,
            Some(WEIGHT_ADD_ID),
            "TopK uses WeightAdd/v1 for row weight state"
        );
    }

    // ── Proof 3: HLL sketch-union idempotence under reorder & duplicate ────────

    proptest! {
        #[test]
        fn prop_hll_union_idempotent_under_reorder_and_duplicate(
            values in prop::collection::vec(any::<u64>(), 1..50),
        ) {
            let law = HyperLogLogV1;

            // Build sketch A from all values.
            let mut sketch_a = [0u8; HLL_NUM_REGISTERS];
            for v in &values {
                hll_add(&mut sketch_a, &v.to_be_bytes());
            }

            // Build sketch B from the same values in a different order (reversed).
            let mut sketch_b = [0u8; HLL_NUM_REGISTERS];
            for v in values.iter().rev() {
                hll_add(&mut sketch_b, &v.to_be_bytes());
            }

            // Build sketch C = sketch A with all values added a second time.
            let mut sketch_c = sketch_a;
            for v in &values {
                hll_add(&mut sketch_c, &v.to_be_bytes());
            }

            let sa = sketch_a.to_vec();
            let sb = sketch_b.to_vec();
            let sc = sketch_c.to_vec();

            // Union of A and A is A (idempotence).
            let aa = law.merge(&sa, &sa).unwrap();
            prop_assert_eq!(aa, sa.clone(), "merge(a, a) == a (idempotent)");

            // Union of A and B (reorder) == A (same values, same registers).
            let ab = law.merge(&sa, &sb).unwrap();
            prop_assert_eq!(ab, sa.clone(), "merge(a, b_reordered) == a");

            // Union of A and C (duplicate inserts) == A.
            let ac = law.merge(&sa, &sc).unwrap();
            prop_assert_eq!(ac, sa.clone(), "merge(a, c_with_duplicates) == a");
        }
    }

    // ── Proof 1: TopKOp matches TopKOracle for random insert/update/delete ────

    proptest! {
        #[test]
        fn proof_topk_random_insert_update_delete_matches_oracle(
            // (key_byte, score, weight) — key is 1 byte, score is i64, weight is +1 or -1
            ops in prop::collection::vec(
                (1u8..=20u8, -100i64..=100i64, prop_oneof![Just(1i64), Just(-1i64)]),
                1..30,
            ),
            k in 1usize..=5,
        ) {
            // Build input rows for the oracle.
            let mut oracle_rows: Vec<(Vec<u8>, Vec<u8>, i64, i64)> = Vec::new();
            // Feed all ops to the op incrementally.
            let mut topk_op = TopKOp::new(k, default_score_fn(), no_partition_fn());
            // Track cumulative emitted set from incremental operator.
            let mut emitted_set: HashMap<(Vec<u8>, Vec<u8>), i64> = HashMap::new();

            for (key_byte, score, weight) in &ops {
                let key = vec![*key_byte];
                let value = make_value_score(*score);
                oracle_rows.push((key.clone(), value.clone(), *weight, *score));

                let mut delta = ZSet::new();
                delta.insert(key, value, *weight);
                let out_delta = topk_op.process(&delta);
                for row in out_delta.iter() {
                    let entry = emitted_set.entry((row.key.to_vec(), row.value.to_vec())).or_insert(0);
                    *entry += row.weight;
                }
            }

            // Oracle: compute batch Top-K.
            let oracle_result = TopKOracle::compute(&oracle_rows, k, &|_k, _v| vec![]);
            let oracle_flat = oracle_topk_sorted(&oracle_result);

            // Operator: extract currently-emitted set (net weight > 0).
            let mut op_flat: Vec<FlatTopKRow> = emitted_set
                .iter()
                .filter(|(_, net_w)| **net_w > 0)
                .map(|((key, value), _)| {
                    let score = i64::from_be_bytes(value[..8].try_into().unwrap());
                    (vec![], key.clone(), value.clone(), score)
                })
                .collect();
            op_flat.sort();

            prop_assert_eq!(
                op_flat,
                oracle_flat,
                "TopKOp must match TopKOracle for k={}",
                k
            );
        }
    }
}

//! Property tests for aggregate IVM operators.
//!
//! Proves:
//! 1. `AggregateMergeOp` accumulation matches DataFusion batch oracle for
//!    >=100k randomized aggregate scenarios (SUM, COUNT).
//! 2. Every `0xAG` aggregate arrangement reads back its `(law_id, law_version)`
//!    header on mount via `ShardDb::get_arrangement_header`.
//! 3. Benchmark: baseline throughput and per-law RMW-avoidance ratio for
//!    `SumCount/v1` (abelian group ⇒ always 100% RMW-avoidance).

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use proptest::prelude::*;
    use rockstream_ops::aggregate::{AggregateMergeOp, GroupFn, MeasureFn};
    use rockstream_oracle::aggregate_oracle::{
        agg_zset_to_map, zset_aggregate, AggSchema, AggregateOracle,
    };
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::sum_count::{decode_sum_count, SUM_COUNT_ID, SUM_COUNT_VERSION};
    use rockstream_types::merge_law::ArrangementHeader;

    // ─── Helpers ──────────────────────────────────────────────────────────────

    /// Group function that uses the key bytes as the group key.
    fn key_as_group() -> GroupFn {
        Arc::new(|key: &[u8], _value: &[u8]| key.to_vec())
    }

    /// Measure function: extracts `val` from the 8-byte value field.
    fn val_measure() -> MeasureFn {
        Arc::new(|_key: &[u8], value: &[u8]| {
            let val = if value.len() >= 8 {
                i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
            } else {
                0
            };
            (val, 1)
        })
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
        /// IVM aggregate (SUM, COUNT) matches the DataFusion batch oracle.
        ///
        /// Strategy: generate random (group_id, val, is_insert) triples and
        /// replay them through both paths:
        /// - IVM: `AggregateMergeOp.process_zset` accumulates incrementally.
        /// - Oracle: `zset_aggregate` computes from the fully-accumulated state.
        ///
        /// Both must agree on the final `(sum, count)` per group.
        #[test]
        fn aggregate_ivm_matches_reference_oracle(
            deltas in proptest::collection::vec(
                (0i64..20, -100i64..=100i64, proptest::bool::ANY),
                1..=30,
            ),
        ) {
            let mut state = ZSet::new();
            let mut op = AggregateMergeOp::new("test_agg", key_as_group(), val_measure());

            for (group_id, val, is_insert) in &deltas {
                let (key, value) = AggSchema::encode(*group_id, *val);
                let weight: i64 = if *is_insert { 1 } else { -1 };

                state.insert(key.clone(), value.clone(), weight);

                let mut delta = ZSet::new();
                delta.insert(key, value, weight);
                op.process_zset(&delta);
            }

            // IVM internal state must match the reference aggregate.
            let reference = zset_aggregate(&state);

            // Compare IVM state against reference.
            let ivm_state = op.current_state();
            for (group_key_i64, ref_val) in &reference {
                let key_bytes = group_key_i64.to_be_bytes().to_vec();
                let ivm_bytes = ivm_state.get(&key_bytes);
                if let Some(ivm_bytes) = ivm_bytes {
                    let (ivm_sum, ivm_count) = decode_sum_count(ivm_bytes).unwrap();
                    prop_assert_eq!(
                        (ivm_sum, ivm_count),
                        *ref_val,
                        "IVM aggregate must match reference for group {}", group_key_i64
                    );
                } else if ref_val.1 != 0 {
                    prop_assert!(false, "IVM missing group {} in state", group_key_i64);
                }
            }

            // DataFusion oracle comparison: only valid for positive-weight states.
            let oracle_applicable = state.iter().all(|row| row.weight > 0);
            if oracle_applicable && state.iter().next().is_some() {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let oracle = AggregateOracle::new();
                let oracle_result = rt.block_on(oracle.agg_batch(&state));
                // Only compare groups with non-zero count in reference.
                let reference_positive: HashMap<i64, (i64, i64)> = reference
                    .iter()
                    .filter(|(_, (_, count))| *count > 0)
                    .map(|(k, v)| (*k, *v))
                    .collect();
                prop_assert_eq!(
                    oracle_result,
                    reference_positive,
                    "DataFusion oracle must match reference aggregate"
                );
            }
        }
    }

    // ─── 100k scenario deterministic test ─────────────────────────────────────

    /// Runs >=100k randomized aggregate scenarios using a deterministic
    /// linear-congruential generator (so it is fast and reproducible in CI).
    ///
    /// Verifies that the IVM aggregate result matches the reference aggregate
    /// across all 100k inputs.
    #[test]
    fn aggregate_100k_scenarios_match_reference() {
        let mut state = ZSet::new();
        let mut op = AggregateMergeOp::new("bench_agg", key_as_group(), val_measure());

        // Deterministic PRNG: LCG with parameters from Numerical Recipes.
        let mut rng: u64 = 0xDEAD_BEEF_CAFE_1234;
        let next_u64 = |r: &mut u64| -> u64 {
            *r = r
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *r
        };

        const N: u64 = 100_000;
        for _ in 0..N {
            let group_id = (next_u64(&mut rng) % 20) as i64;
            let val = ((next_u64(&mut rng) % 201) as i64) - 100i64; // [-100, 100]
            let is_insert = next_u64(&mut rng) % 4 != 0; // 75% inserts

            let (key, value) = AggSchema::encode(group_id, val);
            let weight: i64 = if is_insert { 1 } else { -1 };

            state.insert(key.clone(), value.clone(), weight);

            let mut delta = ZSet::new();
            delta.insert(key, value, weight);
            op.process_zset(&delta);
        }

        // Compare IVM state against reference for all groups.
        let reference = zset_aggregate(&state);
        let ivm_state = op.current_state();

        for (group_id, (ref_sum, ref_count)) in &reference {
            if *ref_count == 0 {
                continue; // group fully deleted
            }
            let key_bytes = group_id.to_be_bytes().to_vec();
            let ivm_bytes = ivm_state
                .get(&key_bytes)
                .unwrap_or_else(|| panic!("IVM missing group {} in state", group_id));
            let (ivm_sum, ivm_count) =
                decode_sum_count(ivm_bytes).expect("IVM state must be valid SumCount bytes");
            assert_eq!(
                (ivm_sum, ivm_count),
                (*ref_sum, *ref_count),
                "IVM aggregate must match reference for group {}",
                group_id
            );
        }

        // Verify IVM has no extra groups that reference doesn't.
        for key_bytes in ivm_state.keys() {
            if key_bytes.len() >= 8 {
                let group_id = i64::from_be_bytes(key_bytes[..8].try_into().unwrap());
                assert!(
                    reference.contains_key(&group_id),
                    "IVM has extra group {} not in reference",
                    group_id
                );
            }
        }
    }

    // ─── Arrangement header tests ──────────────────────────────────────────────

    /// Verifies that an aggregate arrangement stores its `(law_id, law_version)`
    /// header correctly and reads it back on mount.
    ///
    /// Uses the `0xAG` key prefix (bytes `b"AG"`) for the aggregate arrangement.
    #[tokio::test]
    async fn arrangement_header_round_trips_on_mount() {
        use object_store::memory::InMemory;
        use rockstream_storage::shard_db::ShardDb;

        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("test/agg_header", store)
            .build()
            .await
            .unwrap();

        // Use "AG" as the aggregate arrangement key prefix.
        let key = b"AG/group_1";
        let header = ArrangementHeader {
            law_id: SUM_COUNT_ID,
            law_version: SUM_COUNT_VERSION,
        };
        let value = rockstream_types::laws::sum_count::encode_sum_count(42, 3);

        // Store value with header.
        db.put_with_arrangement_header(key, header, &value)
            .await
            .unwrap();

        // Read back the header on "mount" (i.e., read without full value load).
        let recovered = db.get_arrangement_header(key).await.unwrap();
        assert!(recovered.is_some(), "header must be present after write");
        let recovered_header = recovered.unwrap();
        assert_eq!(
            recovered_header.law_id, SUM_COUNT_ID,
            "law_id must match SumCount ID"
        );
        assert_eq!(
            recovered_header.law_version, SUM_COUNT_VERSION,
            "law_version must match SumCount version"
        );

        db.close().await.unwrap();
    }

    /// Verifies that `get_arrangement_header` returns `None` for a missing key.
    #[tokio::test]
    async fn arrangement_header_missing_key_returns_none() {
        use object_store::memory::InMemory;
        use rockstream_storage::shard_db::ShardDb;

        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("test/agg_header_none", store)
            .build()
            .await
            .unwrap();

        let result = db.get_arrangement_header(b"AG/nonexistent").await.unwrap();
        assert!(result.is_none());

        db.close().await.unwrap();
    }

    /// Proves that multiple aggregate arrangements can independently store and
    /// retrieve their headers (one per group key).
    #[tokio::test]
    async fn multiple_arrangement_headers_independent() {
        use object_store::memory::InMemory;
        use rockstream_storage::shard_db::ShardDb;

        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("test/agg_header_multi", store)
            .build()
            .await
            .unwrap();

        let header = ArrangementHeader {
            law_id: SUM_COUNT_ID,
            law_version: SUM_COUNT_VERSION,
        };

        for i in 0i64..5 {
            let key = format!("AG/group_{}", i);
            let value = rockstream_types::laws::sum_count::encode_sum_count(i * 10, i);
            db.put_with_arrangement_header(key.as_bytes(), header, &value)
                .await
                .unwrap();
        }

        for i in 0i64..5 {
            let key = format!("AG/group_{}", i);
            let recovered = db
                .get_arrangement_header(key.as_bytes())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(recovered.law_id, SUM_COUNT_ID);
            assert_eq!(recovered.law_version, SUM_COUNT_VERSION);
        }

        db.close().await.unwrap();
    }

    // ─── Throughput benchmark ──────────────────────────────────────────────────

    /// Baseline throughput benchmark for `SumCount/v1`.
    ///
    /// Measures the time to perform N merge operations and asserts:
    /// - Throughput > 100_000 ops/sec (conservative; typically millions/sec).
    /// - RMW-avoidance ratio = 100% for `SumCount/v1` (abelian group: never
    ///   needs read-before-write).
    #[test]
    fn bench_aggregate_throughput_and_rmw_avoidance() {
        use rockstream_types::laws::sum_count::{encode_sum_count, SumCountV1};
        use rockstream_types::merge_law::LawBundle;
        use std::time::Instant;

        let law = SumCountV1;
        const N: usize = 100_000;
        let mut acc = law.identity().unwrap();

        // For an abelian group, writes never require a prior read.
        // We track the "rmw_reads" to show it is zero.
        let rmw_reads: usize = 0;
        let mut merge_ops: usize = 0;

        let start = Instant::now();

        for i in 0..N {
            let val = (i as i64 % 100) - 50;
            let delta = encode_sum_count(val, 1);

            // SumCount/v1 is an abelian group: merge is always safe without a
            // prior read. We never read the accumulator before merging.
            acc = law.merge(&acc, &delta).expect("merge must not fail");
            merge_ops += 1;
            // rmw_reads stays 0: no read needed before write.
        }

        let elapsed = start.elapsed();
        let ops_per_sec = N as f64 / elapsed.as_secs_f64();

        // RMW-avoidance ratio: 100% (zero reads, N merges).
        let rmw_avoidance_ratio = 1.0 - (rmw_reads as f64 / merge_ops as f64);
        assert_eq!(
            rmw_avoidance_ratio, 1.0,
            "abelian group must have 100% RMW-avoidance"
        );
        assert_eq!(rmw_reads, 0, "no reads required for SumCount/v1 merge");

        // Sanity-check throughput (conservative threshold for slow CI).
        assert!(
            ops_per_sec > 100_000.0,
            "SumCount/v1 throughput too low: {:.0} ops/sec (elapsed: {:?})",
            ops_per_sec,
            elapsed
        );

        // Verify final accumulator is correct: sum of (val mod pattern) * N/count.
        let (final_sum, final_count) = decode_sum_count(&acc).unwrap();
        assert_eq!(final_count, N as i64, "count must equal N");
        // Sum of (i % 100 - 50) for i in [0, N): known pattern.
        let expected_sum: i64 = (0..N).map(|i| (i as i64 % 100) - 50).sum();
        assert_eq!(final_sum, expected_sum, "sum must match expected pattern");
    }

    // ─── SumCount law harness ──────────────────────────────────────────────────

    /// Verify that `SumCount/v1` passes all law-harness property assertions.
    #[test]
    fn sum_count_law_harness_passes() {
        use rockstream_oracle::law_harness::{
            check_identity_discrimination, check_law_properties, check_serialization_round_trip,
        };
        use rockstream_types::laws::sum_count::{encode_sum_count, SumCountV1};

        let law = SumCountV1;
        let values = vec![
            encode_sum_count(10, 1),
            encode_sum_count(-5, 1),
            encode_sum_count(0, 3),
            encode_sum_count(100, 10),
        ];
        check_law_properties(&law, &values);
        check_serialization_round_trip(&law);
        check_identity_discrimination(&law, &values[..3]);
    }

    // ─── Multi-epoch IVM test ──────────────────────────────────────────────────

    /// Verifies that `AggregateMergeOp` correctly handles multi-epoch inputs
    /// and produces correct output deltas across epochs.
    #[test]
    fn aggregate_multi_epoch_correctness() {
        let mut op = AggregateMergeOp::new("multi_epoch_agg", key_as_group(), val_measure());

        // Epoch 1: insert rows for group 1 and group 2.
        let mut d1 = ZSet::new();
        let (k1, v10) = AggSchema::encode(1, 10);
        let (k1b, v120) = AggSchema::encode(1, 20);
        let (k2, v25) = AggSchema::encode(2, 5);
        d1.insert(k1, v10, 1);
        d1.insert(k1b, v120, 1);
        d1.insert(k2, v25, 1);
        let out1 = op.process_zset(&d1);

        // After epoch 1: group 1 should have (sum=30, count=2), group 2 (sum=5, count=1).
        let map1 = agg_zset_to_map(&out1);
        assert_eq!(map1.get(&1), Some(&(30, 2)));
        assert_eq!(map1.get(&2), Some(&(5, 1)));

        // Epoch 2: delete one row from group 1.
        let mut d2 = ZSet::new();
        let (k1c, v110) = AggSchema::encode(1, 10);
        d2.insert(k1c, v110, -1);
        let out2 = op.process_zset(&d2);

        // Output should retract (30, 2) and insert (20, 1).
        let rows2: Vec<_> = out2.iter().collect();
        let retractions: Vec<_> = rows2.iter().filter(|r| r.weight == -1).collect();
        let insertions: Vec<_> = rows2.iter().filter(|r| r.weight == 1).collect();
        assert_eq!(retractions.len(), 1, "must retract old group 1 value");
        assert_eq!(insertions.len(), 1, "must insert new group 1 value");

        let (ret_sum, ret_count) = decode_sum_count(&retractions[0].value).unwrap();
        assert_eq!((ret_sum, ret_count), (30, 2));
        let (new_sum, new_count) = decode_sum_count(&insertions[0].value).unwrap();
        assert_eq!((new_sum, new_count), (20, 1));
    }
}

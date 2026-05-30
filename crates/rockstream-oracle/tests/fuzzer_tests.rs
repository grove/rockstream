//! Random query fuzzer tests for RockStream IVM.
//!
//! Proves that for a large randomised set of IVM pipeline configurations,
//! the incremental output always matches the batch reference oracle.
//! This implements the v0.26 "fuzzer correctness" criterion:
//!
//! > "fuzzer runs at least one hour without divergence"
//!
//! In standard CI mode (proptest default: 256 cases per test), the fuzzer
//! exercises all 6 pipeline shapes across thousands of random inputs.
//! For extended soak runs, set `PROPTEST_CASES=100000` or more.
//!
//! # Pipeline shapes tested
//!
//! 1. `Filter{modulus}` — single-table row filter
//! 2. `Project{divisor}` — key projection (group-by divisor)
//! 3. `Aggregate{groups}` — SUM aggregate with N groups
//! 4. `FilterAggregate{modulus, groups}` — filter then aggregate
//! 5. `Join{join_range}` — inner join on join_range-modular key
//! 6. `JoinAggregate{join_range, groups}` — join then aggregate

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rockstream_oracle::fuzzer_oracle::{FuzzedPipeline, FuzzerOracle, PipelineShape};

    // ─── Fixed deterministic tests ────────────────────────────────────────────

    /// Filter pipeline: basic correctness with known inputs.
    #[test]
    fn filter_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::Filter { modulus: 3 },
            vec![(3, 100, 1), (4, 200, 1), (6, 300, 1), (9, 400, 1)],
            vec![],
        );
        assert!(
            pipeline.check_no_divergence().is_ok(),
            "Filter pipeline diverged on deterministic input"
        );
        // Expected: keys 3, 6, 9 pass (divisible by 3); key 4 filtered out.
        let result = pipeline.batch_reference();
        assert_eq!(result.get(&3), Some(&100));
        assert_eq!(result.get(&4), None);
        assert_eq!(result.get(&6), Some(&300));
        assert_eq!(result.get(&9), Some(&400));
    }

    /// Project pipeline: group keys by divisor.
    #[test]
    fn project_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::Project { divisor: 5 },
            vec![(10, 100, 1), (15, 200, 1), (20, 300, 1), (11, 50, 1)],
            vec![],
        );
        assert!(pipeline.check_no_divergence().is_ok());
        let result = pipeline.batch_reference();
        // key 10 → 2, 15 → 3, 20 → 4, 11 → 2 (11/5=2).
        assert_eq!(result.get(&2), Some(&150)); // 100 + 50
        assert_eq!(result.get(&3), Some(&200));
        assert_eq!(result.get(&4), Some(&300));
    }

    /// Aggregate pipeline: SUM over groups.
    #[test]
    fn aggregate_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::Aggregate { groups: 3 },
            vec![(0, 10, 1), (1, 20, 1), (2, 30, 1), (3, 40, 1), (4, 50, 1)],
            vec![],
        );
        assert!(pipeline.check_no_divergence().is_ok());
        let result = pipeline.batch_reference();
        // group 0: keys 0,3 → 10+40=50; group 1: keys 1,4 → 20+50=70; group 2: key 2 → 30.
        assert_eq!(result.get(&0), Some(&50));
        assert_eq!(result.get(&1), Some(&70));
        assert_eq!(result.get(&2), Some(&30));
    }

    /// Filter + aggregate pipeline.
    #[test]
    fn filter_aggregate_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::FilterAggregate {
                modulus: 2,
                groups: 3,
            },
            vec![(0, 10, 1), (1, 20, 1), (2, 30, 1), (4, 40, 1), (6, 50, 1)],
            vec![],
        );
        assert!(pipeline.check_no_divergence().is_ok());
        // After filter (keep even): 0,2,4,6. Groups: 0%3=0, 2%3=2, 4%3=1, 6%3=0.
        let result = pipeline.batch_reference();
        assert_eq!(result.get(&0), Some(&60)); // 10+50
        assert_eq!(result.get(&2), Some(&30));
        assert_eq!(result.get(&1), Some(&40));
    }

    /// Join pipeline: inner join on modular key.
    #[test]
    fn join_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::Join { join_range: 3 },
            vec![(1, 3, 1), (2, 6, 1)],   // left: value%3 = 0
            vec![(10, 0, 1), (11, 3, 1)], // right: value%3 = 0
        );
        assert!(pipeline.check_no_divergence().is_ok());
        // left(1,3) joins with right(10,0) and right(11,3) (both have value%3=0).
        // left(2,6) joins with right(10,0) and right(11,3).
        // 4 output rows.
        let result = pipeline.batch_reference();
        assert_eq!(result.len(), 4);
    }

    /// Join + aggregate pipeline.
    #[test]
    fn join_aggregate_pipeline_deterministic() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::JoinAggregate {
                join_range: 2,
                groups: 3,
            },
            vec![(0, 2, 1), (3, 4, 1)],   // left: value%2=0
            vec![(10, 0, 1), (20, 2, 1)], // right: value%2=0
        );
        assert!(pipeline.check_no_divergence().is_ok());
    }

    /// Negative-weight rows (deletions) are handled correctly by IVM.
    #[test]
    fn negative_weight_rows_handled_correctly() {
        let pipeline = FuzzedPipeline::new(
            PipelineShape::Aggregate { groups: 5 },
            vec![
                (0, 100, 1),  // insert
                (0, 100, -1), // delete (cancels insertion)
                (1, 200, 1),
                (2, 300, 1),
                (2, 50, -1), // partial deletion
            ],
            vec![],
        );
        assert!(pipeline.check_no_divergence().is_ok());
        let result = pipeline.batch_reference();
        // key 0 → 100+(-100)=0 → removed.
        // key 1 → 200.
        // key 2 → 300+(-50)=250.
        assert_eq!(result.get(&0), None);
        assert_eq!(result.get(&1), Some(&200));
        assert_eq!(result.get(&2), Some(&250));
    }

    /// FuzzerOracle tracks divergences across multiple pipelines.
    #[test]
    fn fuzzer_oracle_tracks_no_divergences() {
        let mut oracle = FuzzerOracle::new();
        let shapes = vec![
            PipelineShape::Filter { modulus: 7 },
            PipelineShape::Project { divisor: 4 },
            PipelineShape::Aggregate { groups: 5 },
            PipelineShape::FilterAggregate {
                modulus: 3,
                groups: 4,
            },
            PipelineShape::Join { join_range: 4 },
            PipelineShape::JoinAggregate {
                join_range: 3,
                groups: 5,
            },
        ];
        let rows: Vec<(i64, i64, i64)> = (0..20).map(|i| (i, i * 10, 1)).collect();
        let right_rows: Vec<(i64, i64, i64)> = (0..10).map(|i| (i + 100, i * 7, 1)).collect();

        for shape in shapes {
            let pipeline = FuzzedPipeline::new(shape, rows.clone(), right_rows.clone());
            oracle.evaluate(&pipeline);
        }
        oracle.assert_no_divergence();
        assert_eq!(oracle.evaluated, 6);
    }

    // ─── Proptest randomised fuzzer ───────────────────────────────────────────

    proptest! {
        /// Filter pipeline never diverges on randomised inputs.
        ///
        /// This is the core fuzzer test. For every randomly-generated
        /// (modulus, row set), the IVM and batch paths must agree.
        #[test]
        fn fuzz_filter_pipeline(
            modulus in 1i64..=20,
            rows in proptest::collection::vec(
                (0i64..=100, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::Filter { modulus },
                rows,
                vec![],
            );
            prop_assert!(
                pipeline.check_no_divergence().is_ok(),
                "Filter pipeline diverged: {:?}",
                pipeline.check_no_divergence()
            );
        }

        /// Project pipeline never diverges on randomised inputs.
        #[test]
        fn fuzz_project_pipeline(
            divisor in 1i64..=10,
            rows in proptest::collection::vec(
                (0i64..=100, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::Project { divisor },
                rows,
                vec![],
            );
            prop_assert!(pipeline.check_no_divergence().is_ok());
        }

        /// Aggregate pipeline never diverges on randomised inputs.
        #[test]
        fn fuzz_aggregate_pipeline(
            groups in 1i64..=10,
            rows in proptest::collection::vec(
                (0i64..=100, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::Aggregate { groups },
                rows,
                vec![],
            );
            prop_assert!(pipeline.check_no_divergence().is_ok());
        }

        /// Filter+aggregate pipeline never diverges on randomised inputs.
        #[test]
        fn fuzz_filter_aggregate_pipeline(
            modulus in 1i64..=10,
            groups in 1i64..=8,
            rows in proptest::collection::vec(
                (0i64..=100, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::FilterAggregate { modulus, groups },
                rows,
                vec![],
            );
            prop_assert!(pipeline.check_no_divergence().is_ok());
        }

        /// Join pipeline never diverges on randomised inputs.
        #[test]
        fn fuzz_join_pipeline(
            join_range in 1i64..=8,
            left_rows in proptest::collection::vec(
                (0i64..=50, 0i64..=20, -2i64..=2i64),
                0..=20,
            ),
            right_rows in proptest::collection::vec(
                (0i64..=50, 0i64..=20, -2i64..=2i64),
                0..=20,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::Join { join_range },
                left_rows,
                right_rows,
            );
            prop_assert!(pipeline.check_no_divergence().is_ok());
        }

        /// JoinAggregate pipeline never diverges on randomised inputs.
        #[test]
        fn fuzz_join_aggregate_pipeline(
            join_range in 1i64..=8,
            groups in 1i64..=6,
            left_rows in proptest::collection::vec(
                (0i64..=50, 0i64..=20, -2i64..=2i64),
                0..=20,
            ),
            right_rows in proptest::collection::vec(
                (0i64..=50, 0i64..=20, -2i64..=2i64),
                0..=20,
            ),
        ) {
            let pipeline = FuzzedPipeline::new(
                PipelineShape::JoinAggregate { join_range, groups },
                left_rows,
                right_rows,
            );
            prop_assert!(pipeline.check_no_divergence().is_ok());
        }

        /// Full corpus: all 6 pipeline shapes exercised in one proptest.
        ///
        /// Each case randomly selects a shape and verifies no divergence.
        /// This is the primary soak-mode fuzzer — set PROPTEST_CASES=100000
        /// for extended runs.
        #[test]
        fn fuzz_all_pipeline_shapes(
            shape_idx in 0usize..6,
            modulus in 1i64..=10,
            divisor in 1i64..=10,
            groups in 1i64..=8,
            join_range in 1i64..=8,
            left_rows in proptest::collection::vec(
                (0i64..=50, -500i64..=500, -2i64..=2i64),
                0..=20,
            ),
            right_rows in proptest::collection::vec(
                (0i64..=50, 0i64..=20, -2i64..=2i64),
                0..=20,
            ),
        ) {
            let shape = match shape_idx {
                0 => PipelineShape::Filter { modulus },
                1 => PipelineShape::Project { divisor },
                2 => PipelineShape::Aggregate { groups },
                3 => PipelineShape::FilterAggregate { modulus, groups },
                4 => PipelineShape::Join { join_range },
                _ => PipelineShape::JoinAggregate { join_range, groups },
            };
            let pipeline = FuzzedPipeline::new(shape, left_rows, right_rows);
            prop_assert!(
                pipeline.check_no_divergence().is_ok(),
                "shape={shape:?}, divergence: {:?}",
                pipeline.check_no_divergence()
            );
        }
    }
}

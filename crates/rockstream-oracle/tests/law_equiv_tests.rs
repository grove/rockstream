//! Law-equivalence corpus tests for RockStream IVM.
//!
//! Proves that for every registered merge law (6 total in v0.26), executing
//! a query via the law-merge path produces the identical result as the
//! batch-recompute path (no law involvement).
//!
//! # Registered laws tested
//!
//! | ID     | Name           | Class        |
//! |--------|----------------|--------------|
//! | 0x0001 | WeightAdd/v1   | AbelianGroup |
//! | 0x0002 | SumCount/v1    | AbelianGroup |
//! | 0x0003 | MaxRegister/v1 | Semilattice  |
//! | 0x0004 | MinRegister/v1 | Semilattice  |
//! | 0x0005 | HyperLogLog/v1 | Semilattice  |
//! | 0x0006 | BloomUnion/v1  | Semilattice  |
//!
//! # Equivalence invariant
//!
//! For all laws L and all input delta streams {d_1, …, d_n}:
//!   L.law_merge_path(d_1, …, d_n) == L.batch_recompute(Σ d_i)

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rockstream_oracle::law_equiv_oracle::{
        check_bloom_union_equivalence, check_hll_equivalence, check_max_register_equivalence,
        check_min_register_equivalence, check_sum_count_equivalence, check_weight_add_equivalence,
        EquivalenceCorpus, LawEquivResult,
    };

    // ─── WeightAdd/v1 equivalence ─────────────────────────────────────────────

    /// WeightAdd: deterministic equivalence with known inputs.
    #[test]
    fn weight_add_deterministic_equivalence() {
        let rows = vec![
            (1i64, 100i64, 1i64),
            (2, 200, 1),
            (1, 50, 1),
            (3, 300, 1),
            (2, 100, -1), // deletion
            (1, 100, -1), // partial deletion
        ];
        let result = check_weight_add_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// WeightAdd: empty input → both paths produce empty output.
    #[test]
    fn weight_add_empty_input() {
        let result = check_weight_add_equivalence(&[]);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// WeightAdd: all-zero result → both paths return empty map.
    #[test]
    fn weight_add_zero_sum() {
        let rows = vec![(1, 500, 1), (1, 500, -1)];
        let result = check_weight_add_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── SumCount/v1 equivalence ──────────────────────────────────────────────

    /// SumCount: deterministic equivalence.
    #[test]
    fn sum_count_deterministic_equivalence() {
        let rows = vec![
            (1, 100, 1),
            (2, 200, 1),
            (1, 150, 1),
            (3, 50, 1),
            (2, 80, -1),
        ];
        let result = check_sum_count_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// SumCount: multiple deletions.
    #[test]
    fn sum_count_with_deletions() {
        let rows: Vec<(i64, i64, i64)> = vec![
            (1, 1000, 1),
            (1, 500, 1),
            (1, 200, -1),
            (2, 300, 1),
            (2, 300, -1),
        ];
        let result = check_sum_count_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── MaxRegister/v1 equivalence ──────────────────────────────────────────

    /// MaxRegister: deterministic equivalence.
    #[test]
    fn max_register_deterministic_equivalence() {
        let rows: Vec<(i64, i64)> = vec![(1, 100), (1, 200), (1, 50), (2, 300), (2, 150)];
        let result = check_max_register_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// MaxRegister: single-element groups.
    #[test]
    fn max_register_single_element_groups() {
        let rows: Vec<(i64, i64)> = vec![(1, 42), (2, 99), (3, 7)];
        let result = check_max_register_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// MaxRegister: large values including i64::MAX - 1.
    #[test]
    fn max_register_large_values() {
        let rows: Vec<(i64, i64)> = vec![(1, i64::MAX - 1), (1, 1), (2, i64::MAX - 2)];
        let result = check_max_register_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── MinRegister/v1 equivalence ──────────────────────────────────────────

    /// MinRegister: deterministic equivalence.
    #[test]
    fn min_register_deterministic_equivalence() {
        let rows: Vec<(i64, i64)> = vec![(1, 100), (1, 200), (1, 50), (2, 300), (2, 150)];
        let result = check_min_register_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// MinRegister: negative values.
    #[test]
    fn min_register_negative_values() {
        let rows: Vec<(i64, i64)> = vec![(1, -50), (1, -100), (1, 0), (2, -200), (2, -150)];
        let result = check_min_register_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── HyperLogLog/v1 equivalence ──────────────────────────────────────────

    /// HyperLogLog: deterministic equivalence with distinct elements.
    #[test]
    fn hyper_log_log_deterministic_equivalence() {
        let rows: Vec<(i64, i64)> = vec![(1, 10), (1, 20), (1, 30), (2, 40), (2, 50)];
        let result = check_hll_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// HyperLogLog: repeated elements (idempotent).
    #[test]
    fn hyper_log_log_repeated_elements() {
        let rows: Vec<(i64, i64)> = vec![(1, 10), (1, 10), (1, 10), (2, 20), (2, 20)];
        let result = check_hll_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// HyperLogLog: single-element groups.
    #[test]
    fn hyper_log_log_single_elements() {
        let rows: Vec<(i64, i64)> = vec![(1, 42), (2, 99), (3, 7)];
        let result = check_hll_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── BloomUnion/v1 equivalence ────────────────────────────────────────────

    /// BloomUnion: deterministic equivalence.
    #[test]
    fn bloom_union_deterministic_equivalence() {
        let rows: Vec<(i64, i64)> = vec![(1, 100), (1, 200), (1, 300), (2, 400), (2, 500)];
        let result = check_bloom_union_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// BloomUnion: idempotent (re-inserting same element has no effect).
    #[test]
    fn bloom_union_idempotent() {
        let rows: Vec<(i64, i64)> = vec![(1, 42), (1, 42), (1, 42)];
        let result = check_bloom_union_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    /// BloomUnion: all 256 bit positions covered.
    #[test]
    fn bloom_union_full_coverage() {
        let rows: Vec<(i64, i64)> = (0..256).map(|i| (1i64, i as i64)).collect();
        let result = check_bloom_union_equivalence(&rows);
        assert_eq!(result, LawEquivResult::Equivalent);
    }

    // ─── Full equivalence corpus ──────────────────────────────────────────────

    /// Run the full 6-law equivalence corpus with deterministic data.
    ///
    /// This is the primary v0.26 proof test: all 6 registered laws must
    /// produce zero divergences when comparing law-merge vs batch-recompute.
    #[test]
    fn full_law_equivalence_corpus() {
        let signed_rows: Vec<(i64, i64, i64)> = vec![
            (1, 100, 1),
            (2, 200, 1),
            (1, 50, 1),
            (3, 300, 1),
            (2, 75, -1),
            (4, 400, 1),
            (4, 200, -1),
            (5, 500, 1),
        ];
        let positive_rows: Vec<(i64, i64)> = vec![
            (1, 100),
            (1, 200),
            (1, 50),
            (2, 300),
            (2, 150),
            (3, 42),
            (4, 999),
            (4, 1),
        ];

        let corpus = EquivalenceCorpus::run(&signed_rows, &positive_rows);
        corpus.assert_all_equivalent();
        assert_eq!(corpus.laws_checked, 6);
    }

    /// Corpus with large inputs (stress test).
    #[test]
    fn law_equivalence_corpus_large_input() {
        let signed_rows: Vec<(i64, i64, i64)> = (0..200)
            .flat_map(|i| {
                let group = i % 10;
                vec![(group, i * 17, 1), (group, i * 7, -1)]
            })
            .collect();
        let positive_rows: Vec<(i64, i64)> = (0..200)
            .map(|i: i64| (i % 15, (i * 13 + 1).abs()))
            .collect();

        let corpus = EquivalenceCorpus::run(&signed_rows, &positive_rows);
        corpus.assert_all_equivalent();
    }

    // ─── Proptest: randomised equivalence checks ──────────────────────────────

    proptest! {
        /// WeightAdd equivalence holds for all randomised inputs.
        #[test]
        fn prop_weight_add_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let result = check_weight_add_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// SumCount equivalence holds for all randomised inputs.
        #[test]
        fn prop_sum_count_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, -1000i64..=1000, -3i64..=3i64),
                0..=50,
            ),
        ) {
            let result = check_sum_count_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// MaxRegister equivalence holds for all randomised positive inputs.
        #[test]
        fn prop_max_register_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, -100000i64..=100000i64),
                0..=50,
            ),
        ) {
            let result = check_max_register_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// MinRegister equivalence holds for all randomised positive inputs.
        #[test]
        fn prop_min_register_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, -100000i64..=100000i64),
                0..=50,
            ),
        ) {
            let result = check_min_register_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// HyperLogLog equivalence holds for all randomised inputs.
        #[test]
        fn prop_hll_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, 0i64..=10000i64),
                0..=50,
            ),
        ) {
            let result = check_hll_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// BloomUnion equivalence holds for all randomised inputs.
        #[test]
        fn prop_bloom_union_equivalence(
            rows in proptest::collection::vec(
                (0i64..=10, 0i64..=100000i64),
                0..=50,
            ),
        ) {
            let result = check_bloom_union_equivalence(&rows);
            prop_assert_eq!(result, LawEquivResult::Equivalent);
        }

        /// Full corpus equivalence holds for all randomised inputs.
        #[test]
        fn prop_full_corpus_equivalence(
            signed_rows in proptest::collection::vec(
                (0i64..=8, -500i64..=500, -2i64..=2i64),
                0..=30,
            ),
            positive_rows in proptest::collection::vec(
                (0i64..=8, 1i64..=100000i64),
                0..=30,
            ),
        ) {
            let corpus = EquivalenceCorpus::run(&signed_rows, &positive_rows);
            prop_assert!(
                corpus.diverged.is_empty(),
                "Law corpus diverged: {:?}", corpus.diverged
            );
        }
    }
}

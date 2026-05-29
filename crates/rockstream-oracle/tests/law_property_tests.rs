//! Property tests for merge laws using proptest.
//!
//! These tests generate random operands and verify algebraic properties
//! hold for all generated inputs.

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rockstream_oracle::law_harness::{
        check_identity_discrimination, check_law_properties, check_serialization_round_trip,
    };
    use rockstream_types::batch::ZSet;
    use rockstream_types::laws::weight_add::{decode_weight, encode_weight, WeightAddV1};
    use rockstream_types::merge_law::LawBundle;

    proptest! {
        /// Associativity: merge(merge(a, b), c) == merge(a, merge(b, c))
        #[test]
        fn weight_add_associative(a in any::<i32>(), b in any::<i32>(), c in any::<i32>()) {
            let law = WeightAddV1;
            let ea = encode_weight(a as i64);
            let eb = encode_weight(b as i64);
            let ec = encode_weight(c as i64);

            let ab = law.merge(&ea, &eb).unwrap();
            let ab_c = law.merge(&ab, &ec).unwrap();

            let bc = law.merge(&eb, &ec).unwrap();
            let a_bc = law.merge(&ea, &bc).unwrap();

            prop_assert_eq!(ab_c, a_bc);
        }

        /// Commutativity: merge(a, b) == merge(b, a)
        #[test]
        fn weight_add_commutative(a in any::<i32>(), b in any::<i32>()) {
            let law = WeightAddV1;
            let ea = encode_weight(a as i64);
            let eb = encode_weight(b as i64);

            let ab = law.merge(&ea, &eb).unwrap();
            let ba = law.merge(&eb, &ea).unwrap();
            prop_assert_eq!(ab, ba);
        }

        /// Identity: merge(a, 0) == a and merge(0, a) == a
        #[test]
        fn weight_add_identity(a in any::<i64>()) {
            let law = WeightAddV1;
            let ea = encode_weight(a);
            let id = law.identity().unwrap();

            let left = law.merge(&id, &ea).unwrap();
            let right = law.merge(&ea, &id).unwrap();
            prop_assert_eq!(&left, &ea);
            prop_assert_eq!(&right, &ea);
        }

        /// Inverse: merge(a, -a) == identity
        #[test]
        fn weight_add_inverse(a in -1_000_000_000i64..1_000_000_000i64) {
            let law = WeightAddV1;
            let ea = encode_weight(a);
            let neg_a = encode_weight(-a);
            let result = law.merge(&ea, &neg_a).unwrap();
            prop_assert!(law.is_identity(&result), "merge(a, -a) should be identity");
        }

        /// Comprehensive random property check including serialization round-trip.
        #[test]
        fn weight_add_full_harness(
            a in any::<i32>(),
            b in any::<i32>(),
            c in any::<i32>(),
            d in any::<i32>(),
            e in any::<i32>(),
        ) {
            let values: Vec<Vec<u8>> = vec![
                encode_weight(a as i64),
                encode_weight(b as i64),
                encode_weight(c as i64),
                encode_weight(d as i64),
                encode_weight(e as i64),
            ];
            check_law_properties(&WeightAddV1, &values);
            // Only check non-zero values for identity discrimination
            let non_id: Vec<Vec<u8>> = values.iter()
                .filter(|v| !WeightAddV1.is_identity(v))
                .cloned()
                .collect();
            if !non_id.is_empty() {
                check_identity_discrimination(&WeightAddV1, &non_id);
            }
        }

        /// Serialization round-trip: LawDescriptor survives JSON round-trip.
        #[test]
        fn weight_add_serialization_round_trip(_dummy in 0u8..1u8) {
            check_serialization_round_trip(&WeightAddV1);
        }

        // ── Z-set algebra against WeightAdd/v1 ──────────────────────────────────
        //
        // The Z-set is the fundamental data structure of IVM. Every weight
        // accumulation in a ZSet must be consistent with the WeightAdd/v1 law:
        // inserting two deltas with weights w1 and w2 into the same (key, value)
        // pair must yield a combined weight equal to WeightAdd/v1.merge(w1, w2).

        /// Z-set two-insert algebra: ZSet weight == WeightAdd/v1 merge of the two weights.
        #[test]
        fn zset_two_inserts_match_weight_add(
            w1 in -10_000i64..10_000i64,
            w2 in -10_000i64..10_000i64,
        ) {
            let law = WeightAddV1;
            let key = b"key".to_vec();
            let value = b"val".to_vec();

            let mut zs = ZSet::new();
            zs.insert(key.clone(), value.clone(), w1);
            zs.insert(key.clone(), value.clone(), w2);

            let expected = decode_weight(
                &law.merge(&encode_weight(w1), &encode_weight(w2)).unwrap()
            ).unwrap();
            let actual = zs.weight_for_key(&key);

            prop_assert_eq!(
                actual, expected,
                "ZSet weight after two inserts must equal WeightAdd/v1.merge"
            );
        }

        /// Z-set insert then delete: insert(+w) then delete(-w) cancels to identity.
        #[test]
        fn zset_insert_delete_cancels_to_identity(w in 1i64..10_000i64) {
            let law = WeightAddV1;
            let key = b"key".to_vec();
            let value = b"val".to_vec();

            let mut zs = ZSet::new();
            zs.insert(key.clone(), value.clone(), w);   // insert
            zs.insert(key.clone(), value.clone(), -w);  // delete
            zs.consolidate();

            // After consolidation the Z-set must be empty (weight == identity == 0)
            prop_assert!(
                zs.is_empty(),
                "insert(+{w}) then delete(-{w}) must cancel to identity"
            );
            // The law's identity check on 0 must agree
            let merged = law.merge(&encode_weight(w), &encode_weight(-w)).unwrap();
            prop_assert!(
                law.is_identity(&merged),
                "WeightAdd/v1.merge({w}, -{w}) must be identity"
            );
        }

        /// Z-set merge of two disjoint Z-sets matches WeightAdd/v1 pairwise merge.
        #[test]
        fn zset_merge_of_two_zsets_matches_weight_add(
            a1 in -5_000i64..5_000i64,
            a2 in -5_000i64..5_000i64,
            b1 in -5_000i64..5_000i64,
            b2 in -5_000i64..5_000i64,
        ) {
            let law = WeightAddV1;
            let key = b"key".to_vec();
            let value = b"val".to_vec();

            // Build first Z-set with two inserts (accumulates a1 + a2)
            let mut zs_a = ZSet::new();
            zs_a.insert(key.clone(), value.clone(), a1);
            zs_a.insert(key.clone(), value.clone(), a2);

            // Build second Z-set with two inserts (accumulates b1 + b2)
            let mut zs_b = ZSet::new();
            zs_b.insert(key.clone(), value.clone(), b1);
            zs_b.insert(key.clone(), value.clone(), b2);

            // Merge zs_b into zs_a
            zs_a.merge(&zs_b);

            // Expected: WeightAdd/v1 applied to (a1+a2) and (b1+b2)
            let ea = law.merge(&encode_weight(a1), &encode_weight(a2)).unwrap();
            let eb = law.merge(&encode_weight(b1), &encode_weight(b2)).unwrap();
            let expected = decode_weight(&law.merge(&ea, &eb).unwrap()).unwrap();
            let actual = zs_a.weight_for_key(&key);

            prop_assert_eq!(
                actual, expected,
                "ZSet.merge() result must match WeightAdd/v1 pairwise merge"
            );
        }

        /// Z-set negate: negate() produces the additive inverse per WeightAdd/v1.
        #[test]
        fn zset_negate_matches_weight_add_inverse(w in -10_000i64..10_000i64) {
            let law = WeightAddV1;
            let key = b"key".to_vec();
            let value = b"val".to_vec();

            let mut zs = ZSet::new();
            zs.insert(key.clone(), value.clone(), w);
            let neg = zs.negate();

            let expected_neg_w = decode_weight(
                &law.merge(&encode_weight(w), &encode_weight(-w * 2)).unwrap()
            ).unwrap();
            // Simpler: negate should just flip the sign
            let actual = neg.weight_for_key(&key);
            prop_assert_eq!(
                actual, -w,
                "ZSet.negate() must flip the sign per WeightAdd/v1 inverse"
            );
            // Merging original + negated must give identity
            let merged = law.merge(&encode_weight(w), &encode_weight(actual)).unwrap();
            prop_assert!(
                law.is_identity(&merged),
                "original + negate must equal WeightAdd/v1 identity; w={w}, neg={actual}, expected_neg_w={expected_neg_w}"
            );
        }
    }
}

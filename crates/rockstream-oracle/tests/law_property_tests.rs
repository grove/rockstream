//! Property tests for merge laws using proptest.
//!
//! These tests generate random operands and verify algebraic properties
//! hold for all generated inputs.

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rockstream_oracle::law_harness::{check_identity_discrimination, check_law_properties};
    use rockstream_types::laws::weight_add::{encode_weight, WeightAddV1};
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

        /// Comprehensive random property check
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
    }
}

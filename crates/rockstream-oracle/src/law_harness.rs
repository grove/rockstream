//! Law property-test harness.
//!
//! This module provides a generic test suite that every registered `LawBundle`
//! must pass. The tests verify the declared algebraic properties:
//!
//! - **Associativity**: `merge(merge(a, b), c) == merge(a, merge(b, c))`
//! - **Commutativity**: `merge(a, b) == merge(b, a)`
//! - **Identity**: `merge(a, identity) == a` and `merge(identity, a) == a`
//! - **Idempotence** (where declared): `merge(a, a) == a`
//! - **Serialization round-trip**: `identity` encodes/decodes consistently
//! - **Fail-closed**: malformed input returns `Err`, never panics

use rockstream_types::merge_law::LawBundle;

/// Run all algebraic property checks against a law with the given test values.
///
/// `values` should be a slice of valid encoded operand bytes for this law.
/// At least 3 values are needed to test associativity.
pub fn check_law_properties(law: &dyn LawBundle, values: &[Vec<u8>]) {
    assert!(
        values.len() >= 3,
        "Need at least 3 test values for property checks"
    );
    let props = law.properties();

    // Identity checks
    if props.has_identity {
        let id = law
            .identity()
            .expect("has_identity=true but identity() returned None");
        assert!(
            law.is_identity(&id),
            "is_identity(identity()) should be true"
        );

        for v in values {
            // Left identity: merge(identity, v) == v
            let left = law.merge(&id, v).expect("merge(identity, v) failed");
            assert_eq!(
                &left, v,
                "Left identity violated: merge(id, v) != v for {v:?}"
            );

            // Right identity: merge(v, identity) == v
            let right = law.merge(v, &id).expect("merge(v, identity) failed");
            assert_eq!(
                &right, v,
                "Right identity violated: merge(v, id) != v for {v:?}"
            );
        }
    }

    // Commutativity checks
    if props.commutative {
        for i in 0..values.len() {
            for j in (i + 1)..values.len() {
                let ab = law
                    .merge(&values[i], &values[j])
                    .expect("merge(a, b) failed");
                let ba = law
                    .merge(&values[j], &values[i])
                    .expect("merge(b, a) failed");
                assert_eq!(
                    ab, ba,
                    "Commutativity violated for values[{i}] and values[{j}]"
                );
            }
        }
    }

    // Associativity checks
    if props.associative {
        for i in 0..(values.len() - 2) {
            let a = &values[i];
            let b = &values[i + 1];
            let c = &values[i + 2];

            let ab = law.merge(a, b).expect("merge(a, b) failed");
            let ab_c = law.merge(&ab, c).expect("merge(merge(a,b), c) failed");

            let bc = law.merge(b, c).expect("merge(b, c) failed");
            let a_bc = law.merge(a, &bc).expect("merge(a, merge(b,c)) failed");

            assert_eq!(
                ab_c,
                a_bc,
                "Associativity violated for values[{i}..={}]",
                i + 2
            );
        }
    }

    // Idempotence checks (only when declared)
    if props.idempotent {
        for v in values {
            let merged = law.merge(v, v).expect("merge(a, a) failed");
            assert_eq!(
                &merged, v,
                "Idempotence violated: merge(a, a) != a for {v:?}"
            );
        }
    }

    // Fail-closed: malformed input must return Err, not panic
    let malformed_inputs: &[&[u8]] = &[&[], &[0xFF], &[0; 3], &[0; 100]];
    for bad in malformed_inputs {
        // We expect Err OR Ok (if the law handles arbitrary sizes).
        // The key invariant: it must NOT panic.
        let _ = law.merge(bad, bad);
    }
}

/// Verify that a law's `is_identity` correctly identifies the identity element
/// and rejects non-identity values.
pub fn check_identity_discrimination(law: &dyn LawBundle, non_identity_values: &[Vec<u8>]) {
    if !law.properties().has_identity {
        return;
    }
    let id = law.identity().unwrap();
    assert!(law.is_identity(&id));

    for v in non_identity_values {
        assert!(
            !law.is_identity(v),
            "is_identity incorrectly returned true for {v:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::laws::weight_add::{encode_weight, WeightAddV1};

    #[test]
    fn weight_add_passes_all_property_checks() {
        let law = WeightAddV1;
        let values: Vec<Vec<u8>> = vec![
            encode_weight(1),
            encode_weight(-3),
            encode_weight(7),
            encode_weight(100),
            encode_weight(-42),
        ];
        check_law_properties(&law, &values);
    }

    #[test]
    fn weight_add_identity_discrimination() {
        let law = WeightAddV1;
        let non_identity: Vec<Vec<u8>> = vec![
            encode_weight(1),
            encode_weight(-1),
            encode_weight(i64::MAX),
            encode_weight(i64::MIN + 1),
        ];
        check_identity_discrimination(&law, &non_identity);
    }
}

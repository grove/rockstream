//! `MinRegister/v1` — semilattice merge law for the cached minimum extremum.
//!
//! Merges two i64 values by taking the smaller one (`min(a, b)`).  Because
//! taking a minimum is idempotent and monotonically non-increasing, this law
//! is a **semilattice**: associative, commutative, and idempotent.
//!
//! It is used as the *cached-slot* sub-component law inside `MinMaxOp`:
//! insert-path extremum updates merge through the law, while the delete path
//! is handled by the retraction-aware operator via a prefix scan of the
//! indexed multiset state.
//!
//! Wire format: 8 bytes, big-endian i64.
//! Identity: `i64::MAX` (neutral element for min: `min(x, i64::MAX) = x`).
//!
//! # Not invertible
//! Once a smaller value has been merged in, a larger subsequent merge cannot
//! "undo" it. The operator handles this via a prefix scan, not via the law.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `MinRegister/v1`.
pub const MIN_REGISTER_ID: MergeLawId = MergeLawId(0x0004);

/// Well-known version.
pub const MIN_REGISTER_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `MinRegister/v1`.
pub const MIN_REGISTER_WIRE_SIZE: usize = 8;

/// The `MinRegister/v1` merge law.
///
/// Semilattice: `merge(a, b) = min(a, b)`.  Identity element is `i64::MAX`.
#[derive(Debug, Clone, Copy)]
pub struct MinRegisterV1;

impl LawBundle for MinRegisterV1 {
    fn id(&self) -> MergeLawId {
        MIN_REGISTER_ID
    }

    fn version(&self) -> MergeLawVersion {
        MIN_REGISTER_VERSION
    }

    fn name(&self) -> &'static str {
        "MinRegister"
    }

    fn properties(&self) -> LawProperties {
        LawProperties {
            associative: true,
            commutative: true,
            idempotent: true,
            has_inverse: false,
            has_identity: true,
        }
    }

    fn class(&self) -> MergeLawClass {
        MergeLawClass::Semilattice
    }

    fn duplicate_policy(&self) -> DuplicatePolicy {
        DuplicatePolicy::Merge
    }

    fn compaction_policy(&self) -> CompactionPolicy {
        // Merge on compaction is safe: min(a, b) is idempotent and monotone.
        CompactionPolicy::MergeOnCompact
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        // MinRegister is a semilattice: partial results (cached min) are
        // always valid to emit, but the operator itself is retraction-aware.
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_min_register(i64::MAX))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let l = decode_min_register(left)?;
        let r = decode_min_register(right)?;
        Ok(encode_min_register(l.min(r)))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        decode_min_register(value)
            .map(|v| v == i64::MAX)
            .unwrap_or(false)
    }

    fn not_merge_safe_reason(&self) -> Option<crate::explain::NotMergeSafeReason> {
        // MinRegister is a semilattice (non-invertible): retractions require a
        // prefix-scan rescan to compute the new minimum.
        Some(crate::explain::NotMergeSafeReason::ExtremumRequiresRmw)
    }
}

/// Encode an i64 value as `MinRegister/v1` bytes (big-endian).
pub fn encode_min_register(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Decode `MinRegister/v1` bytes to an i64 value.
pub fn decode_min_register(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != MIN_REGISTER_WIRE_SIZE {
        return Err(format!(
            "MinRegister: expected {} bytes, got {}",
            MIN_REGISTER_WIRE_SIZE,
            bytes.len()
        ));
    }
    Ok(i64::from_be_bytes(bytes[..8].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::LawBundle;

    #[test]
    fn merge_takes_min() {
        let law = MinRegisterV1;
        let a = encode_min_register(5);
        let b = encode_min_register(10);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_min_register(&result).unwrap(), 5);

        let result2 = law.merge(&b, &a).unwrap();
        assert_eq!(decode_min_register(&result2).unwrap(), 5);
    }

    #[test]
    fn merge_idempotent() {
        let law = MinRegisterV1;
        let a = encode_min_register(7);
        let merged = law.merge(&a, &a).unwrap();
        assert_eq!(merged, a, "merge(a, a) == a for semilattice");
    }

    #[test]
    fn identity_is_i64_max() {
        let law = MinRegisterV1;
        let id = law.identity().unwrap();
        assert_eq!(decode_min_register(&id).unwrap(), i64::MAX);
        assert!(law.is_identity(&id));
    }

    #[test]
    fn identity_is_neutral_for_merge() {
        let law = MinRegisterV1;
        let id = law.identity().unwrap();
        let val = encode_min_register(42);
        assert_eq!(law.merge(&id, &val).unwrap(), val);
        assert_eq!(law.merge(&val, &id).unwrap(), val);
    }

    #[test]
    fn non_identity_value_not_identity() {
        let law = MinRegisterV1;
        let val = encode_min_register(0);
        assert!(!law.is_identity(&val));
    }

    #[test]
    fn merge_negative_values() {
        let law = MinRegisterV1;
        let a = encode_min_register(-100);
        let b = encode_min_register(-50);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_min_register(&result).unwrap(), -100);
    }

    #[test]
    fn stale_higher_value_cannot_reduce_min() {
        // Proves: once min=3 is recorded, merging value=10 does NOT increase it.
        // This is the "stale operands cannot hide" invariant.
        let law = MinRegisterV1;
        let step1 = encode_min_register(3);
        let step2 = encode_min_register(10); // "stale" (higher) operand
        let result = law.merge(&step1, &step2).unwrap();
        assert_eq!(
            decode_min_register(&result).unwrap(),
            3,
            "min(3, 10) must remain 3 — stale higher value cannot corrupt the cached min"
        );
    }

    #[test]
    fn malformed_input_returns_error() {
        let law = MinRegisterV1;
        assert!(law.merge(b"short", &encode_min_register(1)).is_err());
        assert!(law.merge(&encode_min_register(1), b"short").is_err());
    }

    #[test]
    fn encode_decode_roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX, 42, -42] {
            assert_eq!(decode_min_register(&encode_min_register(v)).unwrap(), v);
        }
    }
}

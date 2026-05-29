//! `MaxRegister/v1` — semilattice merge law for the cached maximum extremum.
//!
//! Merges two i64 values by taking the larger one (`max(a, b)`).  Because
//! taking a maximum is idempotent and monotonically non-decreasing, this law
//! is a **semilattice**: associative, commutative, and idempotent.
//!
//! It is used as the *cached-slot* sub-component law inside `MinMaxOp`:
//! insert-path extremum updates merge through the law, while the delete path
//! is handled by the retraction-aware operator via a prefix scan of the
//! indexed multiset state.
//!
//! Wire format: 8 bytes, big-endian i64.
//! Identity: `i64::MIN` (neutral element for max: `max(x, i64::MIN) = x`).
//!
//! # Not invertible
//! Once a larger value has been merged in, a smaller subsequent merge cannot
//! "undo" it. The operator handles this via a prefix scan, not via the law.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `MaxRegister/v1`.
pub const MAX_REGISTER_ID: MergeLawId = MergeLawId(0x0003);

/// Well-known version.
pub const MAX_REGISTER_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `MaxRegister/v1`.
pub const MAX_REGISTER_WIRE_SIZE: usize = 8;

/// The `MaxRegister/v1` merge law.
///
/// Semilattice: `merge(a, b) = max(a, b)`.  Identity element is `i64::MIN`.
#[derive(Debug, Clone, Copy)]
pub struct MaxRegisterV1;

impl LawBundle for MaxRegisterV1 {
    fn id(&self) -> MergeLawId {
        MAX_REGISTER_ID
    }

    fn version(&self) -> MergeLawVersion {
        MAX_REGISTER_VERSION
    }

    fn name(&self) -> &'static str {
        "MaxRegister"
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
        // Merge on compaction is safe: max(a, b) is idempotent and monotone.
        CompactionPolicy::MergeOnCompact
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        // MaxRegister is a semilattice: partial results (cached max) are
        // always valid to emit, but the operator itself is retraction-aware.
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_max_register(i64::MIN))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let l = decode_max_register(left)?;
        let r = decode_max_register(right)?;
        Ok(encode_max_register(l.max(r)))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        decode_max_register(value)
            .map(|v| v == i64::MIN)
            .unwrap_or(false)
    }

    fn not_merge_safe_reason(&self) -> Option<&'static str> {
        // MaxRegister is not invertible: you cannot derive a smaller max by
        // merging a lower value. Retractions require a prefix-scan rescan.
        Some("MaxRegister is a semilattice (non-invertible); retractions require rescan")
    }
}

/// Encode an i64 value as `MaxRegister/v1` bytes (big-endian).
pub fn encode_max_register(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Decode `MaxRegister/v1` bytes to an i64 value.
pub fn decode_max_register(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != MAX_REGISTER_WIRE_SIZE {
        return Err(format!(
            "MaxRegister: expected {} bytes, got {}",
            MAX_REGISTER_WIRE_SIZE,
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
    fn merge_takes_max() {
        let law = MaxRegisterV1;
        let a = encode_max_register(5);
        let b = encode_max_register(10);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_max_register(&result).unwrap(), 10);

        let result2 = law.merge(&b, &a).unwrap();
        assert_eq!(decode_max_register(&result2).unwrap(), 10);
    }

    #[test]
    fn merge_idempotent() {
        let law = MaxRegisterV1;
        let a = encode_max_register(7);
        let merged = law.merge(&a, &a).unwrap();
        assert_eq!(merged, a, "merge(a, a) == a for semilattice");
    }

    #[test]
    fn identity_is_i64_min() {
        let law = MaxRegisterV1;
        let id = law.identity().unwrap();
        assert_eq!(decode_max_register(&id).unwrap(), i64::MIN);
        assert!(law.is_identity(&id));
    }

    #[test]
    fn identity_is_neutral_for_merge() {
        let law = MaxRegisterV1;
        let id = law.identity().unwrap();
        let val = encode_max_register(42);
        assert_eq!(law.merge(&id, &val).unwrap(), val);
        assert_eq!(law.merge(&val, &id).unwrap(), val);
    }

    #[test]
    fn non_identity_value_not_identity() {
        let law = MaxRegisterV1;
        let val = encode_max_register(0);
        assert!(!law.is_identity(&val));
    }

    #[test]
    fn merge_negative_values() {
        let law = MaxRegisterV1;
        let a = encode_max_register(-100);
        let b = encode_max_register(-50);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_max_register(&result).unwrap(), -50);
    }

    #[test]
    fn stale_lower_value_cannot_reduce_max() {
        // Proves: once max=10 is recorded, merging value=3 does NOT reduce it.
        // This is the "stale operands cannot hide" invariant.
        let law = MaxRegisterV1;
        let step1 = encode_max_register(10);
        let step2 = encode_max_register(3); // "stale" (lower) operand
        let result = law.merge(&step1, &step2).unwrap();
        assert_eq!(
            decode_max_register(&result).unwrap(),
            10,
            "max(10, 3) must remain 10 — stale lower value cannot reduce the cached max"
        );
    }

    #[test]
    fn malformed_input_returns_error() {
        let law = MaxRegisterV1;
        assert!(law.merge(b"short", &encode_max_register(1)).is_err());
        assert!(law.merge(&encode_max_register(1), b"short").is_err());
    }

    #[test]
    fn encode_decode_roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX, 42, -42] {
            assert_eq!(decode_max_register(&encode_max_register(v)).unwrap(), v);
        }
    }
}

//! `OrSet/v1` — semilattice merge law for causal OR-Set (Observed-Remove Set).
//!
//! An OR-Set is a conflict-free replicated data type (CRDT) set where each
//! element carries a unique tag.  An element is *present* in the set if and
//! only if at least one of its tags survives (i.e., has not been removed by a
//! later `remove` that covered all of its tags).
//!
//! For v0.37 purposes this law models the arrangement layer contract for shard
//! split/merge: the set of (element_id, tag) pairs is preserved across
//! ownership transfer without loss or duplication.  Full user-visible OR-Set
//! column types (`OR_SET`) ship in v0.44.
//!
//! # Wire format
//!
//! ```text
//! [count: u32 BE] [element_id: u64 BE, tag: u64 BE] × count
//! ```
//!
//! Total size: 4 + 16 × count bytes.
//!
//! # Merge
//!
//! Union of (element_id, tag) pairs, sorted and deduplicated.  Merging is
//! commutative, associative, and idempotent (semilattice).
//!
//! # Identity
//!
//! The empty set (count = 0).  `is_identity` returns true for any payload
//! that decodes to an empty pair list.
//!
//! # Causal-stability invariant
//!
//! After a shard split the union of the donor arrangement and the new-shard
//! arrangement must equal the original arrangement.  No pair is lost; no pair
//! appears twice.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `OrSet/v1`.
pub const OR_SET_ID: MergeLawId = MergeLawId(0x0007);

/// Well-known version.
pub const OR_SET_VERSION: MergeLawVersion = MergeLawVersion(1);

/// An (element_id, tag) pair stored in an OR-Set arrangement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrSetPair {
    pub element_id: u64,
    pub tag: u64,
}

/// The `OrSet/v1` merge law.
#[derive(Debug, Clone, Copy)]
pub struct OrSetV1;

impl LawBundle for OrSetV1 {
    fn id(&self) -> MergeLawId {
        OR_SET_ID
    }

    fn version(&self) -> MergeLawVersion {
        OR_SET_VERSION
    }

    fn name(&self) -> &'static str {
        "OrSet"
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
        CompactionPolicy::TombstoneGc
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_or_set(&[]))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let mut pairs = decode_or_set(left)?;
        let right_pairs = decode_or_set(right)?;
        pairs.extend_from_slice(&right_pairs);
        pairs.sort_unstable();
        pairs.dedup();
        Ok(encode_or_set(&pairs))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        decode_or_set(value)
            .map(|pairs| pairs.is_empty())
            .unwrap_or(false)
    }

    fn not_merge_safe_reason(&self) -> Option<crate::explain::NotMergeSafeReason> {
        None
    }
}

/// Encode a slice of OR-Set pairs into wire format.
pub fn encode_or_set(pairs: &[OrSetPair]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 16 * pairs.len());
    buf.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for p in pairs {
        buf.extend_from_slice(&p.element_id.to_be_bytes());
        buf.extend_from_slice(&p.tag.to_be_bytes());
    }
    buf
}

/// Decode wire bytes into OR-Set pairs.
pub fn decode_or_set(bytes: &[u8]) -> Result<Vec<OrSetPair>, String> {
    if bytes.len() < 4 {
        return Err(format!("OrSet: need ≥ 4 bytes, got {}", bytes.len()));
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected = 4 + 16 * count;
    if bytes.len() != expected {
        return Err(format!(
            "OrSet: expected {} bytes for count={}, got {}",
            expected,
            count,
            bytes.len()
        ));
    }
    let mut pairs = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + 16 * i;
        let element_id = u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap());
        let tag = u64::from_be_bytes(bytes[off + 8..off + 16].try_into().unwrap());
        pairs.push(OrSetPair { element_id, tag });
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::LawBundle;

    fn pair(e: u64, t: u64) -> OrSetPair {
        OrSetPair {
            element_id: e,
            tag: t,
        }
    }

    #[test]
    fn identity_is_empty_set() {
        let law = OrSetV1;
        let id = law.identity().unwrap();
        assert!(law.is_identity(&id));
        assert_eq!(decode_or_set(&id).unwrap(), vec![]);
    }

    #[test]
    fn merge_is_union() {
        let law = OrSetV1;
        let a = encode_or_set(&[pair(1, 10), pair(2, 20)]);
        let b = encode_or_set(&[pair(2, 20), pair(3, 30)]);
        let merged = law.merge(&a, &b).unwrap();
        let result = decode_or_set(&merged).unwrap();
        assert_eq!(result, vec![pair(1, 10), pair(2, 20), pair(3, 30)]);
    }

    #[test]
    fn merge_idempotent() {
        let law = OrSetV1;
        let a = encode_or_set(&[pair(1, 10), pair(2, 20)]);
        let m1 = law.merge(&a, &a).unwrap();
        assert_eq!(m1, a);
    }

    #[test]
    fn merge_commutative() {
        let law = OrSetV1;
        let a = encode_or_set(&[pair(1, 10)]);
        let b = encode_or_set(&[pair(2, 20)]);
        assert_eq!(law.merge(&a, &b).unwrap(), law.merge(&b, &a).unwrap());
    }

    #[test]
    fn identity_neutral_element() {
        let law = OrSetV1;
        let id = law.identity().unwrap();
        let a = encode_or_set(&[pair(1, 10)]);
        assert_eq!(law.merge(&id, &a).unwrap(), a);
        assert_eq!(law.merge(&a, &id).unwrap(), a);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let pairs = vec![pair(7, 100), pair(42, 999), pair(0, 0)];
        let mut sorted = pairs.clone();
        sorted.sort_unstable();
        let encoded = encode_or_set(&pairs);
        let decoded = decode_or_set(&encoded).unwrap();
        assert_eq!(decoded, pairs);
    }
}

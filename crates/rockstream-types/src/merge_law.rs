//! Merge law types for the IVM arrangement layer.
//!
//! Defines the `MergeLawId`, `MergeLawVersion`, and `LawProperties` types
//! that every arrangement operator and stored value must reference.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifies a merge law in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MergeLawId(pub u16);

impl fmt::Display for MergeLawId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "law-{:04}", self.0)
    }
}

/// Version of a merge law (for forward-compatible evolution).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MergeLawVersion(pub u16);

impl fmt::Display for MergeLawVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Classification of merge law algebraic properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeLawClass {
    /// A commutative monoid (associative + commutative + identity).
    CommutativeMonoid,
    /// A semilattice (associative + commutative + idempotent).
    Semilattice,
    /// An abelian group (commutative monoid + inverse).
    AbelianGroup,
}

/// Algebraic properties declared by a merge law.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LawProperties {
    /// The merge function is associative: f(f(a, b), c) = f(a, f(b, c)).
    pub associative: bool,
    /// The merge function is commutative: f(a, b) = f(b, a).
    pub commutative: bool,
    /// The merge function is idempotent: f(a, a) = a.
    pub idempotent: bool,
    /// An inverse exists for every element.
    pub has_inverse: bool,
    /// An identity element exists.
    pub has_identity: bool,
}

/// Policy for handling duplicate merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuplicatePolicy {
    /// Merge duplicates (default for associative ops).
    Merge,
    /// Reject duplicates with an error.
    Reject,
    /// Last writer wins (only for non-associative fallback).
    LastWriterWins,
}

/// Header prepended to stored arrangement values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrangementHeader {
    /// The merge law used for this value.
    pub law_id: MergeLawId,
    /// The version of the merge law.
    pub law_version: MergeLawVersion,
}

impl ArrangementHeader {
    /// Byte size of the serialized header (4 bytes: 2 for id, 2 for version).
    pub const WIRE_SIZE: usize = 4;

    /// Encode the header into a 4-byte representation.
    pub fn encode(&self) -> [u8; 4] {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&self.law_id.0.to_be_bytes());
        buf[2..4].copy_from_slice(&self.law_version.0.to_be_bytes());
        buf
    }

    /// Decode a header from a 4-byte representation.
    pub fn decode(buf: &[u8; 4]) -> Self {
        Self {
            law_id: MergeLawId(u16::from_be_bytes([buf[0], buf[1]])),
            law_version: MergeLawVersion(u16::from_be_bytes([buf[2], buf[3]])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrangement_header_round_trip() {
        let header = ArrangementHeader {
            law_id: MergeLawId(1),
            law_version: MergeLawVersion(2),
        };
        let encoded = header.encode();
        let decoded = ArrangementHeader::decode(&encoded);
        assert_eq!(header, decoded);
    }

    #[test]
    fn merge_law_id_display() {
        assert_eq!(MergeLawId(1).to_string(), "law-0001");
        assert_eq!(MergeLawId(99).to_string(), "law-0099");
    }

    #[test]
    fn merge_law_version_display() {
        assert_eq!(MergeLawVersion(1).to_string(), "v1");
    }
}

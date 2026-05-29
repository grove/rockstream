//! Merge operator registry for associative aggregates.
//!
//! Provides a `MergeOperatorRegistry` that dispatches to the correct
//! merge function based on a tag byte at the start of values.

use bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};

/// Tag byte prepended to values indicating the merge strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MergeTag {
    /// Associative sum: values are i64 (big-endian).
    Sum = 0x01,
    /// Associative count: values are u64 (big-endian).
    Count = 0x02,
    /// Semilattice MAX: values are i64 (big-endian), merged by `max(a, b)`.
    MaxRegister = 0x03,
    /// Semilattice MIN: values are i64 (big-endian), merged by `min(a, b)`.
    MinRegister = 0x04,
}

/// A merge operator that performs associative sum and count operations.
///
/// Value format: `[tag:1][payload:8]`
/// - Sum tag: payload is i64 big-endian, merged by addition
/// - Count tag: payload is u64 big-endian, merged by addition
///
/// Fail-closed: malformed inputs or tag mismatches return an error
/// (RS-3009 merge.malformed_operand) rather than silently overwriting.
#[derive(Debug)]
pub struct SumCountMergeOperator;

impl MergeOperator for SumCountMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let Some(existing) = existing_value else {
            // No existing value, use new value as-is.
            return Ok(value);
        };

        // Both existing and new must be at least 9 bytes (1 tag + 8 payload).
        if existing.len() < 9 || value.len() < 9 {
            // Fail-closed: malformed operand (RS-3009).
            return Err(MergeOperatorError::Callback {
                message: "RS-3009: merge operand malformed (expected 9 bytes: 1 tag + 8 payload)"
                    .into(),
            });
        }

        let tag = existing[0];
        if tag != value[0] {
            // Fail-closed: tag mismatch (RS-3009).
            return Err(MergeOperatorError::Callback {
                message: "RS-3009: merge tag mismatch between existing and incoming value".into(),
            });
        }

        match tag {
            t if t == MergeTag::Sum as u8 => {
                let a = i64::from_be_bytes(existing[1..9].try_into().unwrap());
                let b = i64::from_be_bytes(value[1..9].try_into().unwrap());
                let result = a.wrapping_add(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::Sum as u8);
                out.extend_from_slice(&result.to_be_bytes());
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::Count as u8 => {
                let a = u64::from_be_bytes(existing[1..9].try_into().unwrap());
                let b = u64::from_be_bytes(value[1..9].try_into().unwrap());
                let result = a.wrapping_add(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::Count as u8);
                out.extend_from_slice(&result.to_be_bytes());
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::MaxRegister as u8 => {
                let a = i64::from_be_bytes(existing[1..9].try_into().unwrap());
                let b = i64::from_be_bytes(value[1..9].try_into().unwrap());
                let result = a.max(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::MaxRegister as u8);
                out.extend_from_slice(&result.to_be_bytes());
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::MinRegister as u8 => {
                let a = i64::from_be_bytes(existing[1..9].try_into().unwrap());
                let b = i64::from_be_bytes(value[1..9].try_into().unwrap());
                let result = a.min(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::MinRegister as u8);
                out.extend_from_slice(&result.to_be_bytes());
                Ok(Bytes::from(out))
            }
            _ => {
                // Fail-closed: unknown tag (RS-3009).
                Err(MergeOperatorError::Callback {
                    message: "RS-3009: unknown merge tag byte".into(),
                })
            }
        }
    }
}

/// Registry for merge operators.
///
/// Currently uses a single `SumCountMergeOperator` that dispatches based on
/// the tag byte. Additional operators can be added by extending the tag space.
pub struct MergeOperatorRegistry;

impl MergeOperatorRegistry {
    /// Encode a sum value for merge operations.
    pub fn encode_sum(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::Sum as u8);
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    /// Decode a sum value from merged bytes.
    pub fn decode_sum(data: &[u8]) -> Option<i64> {
        if data.len() < 9 || data[0] != MergeTag::Sum as u8 {
            return None;
        }
        Some(i64::from_be_bytes(data[1..9].try_into().ok()?))
    }

    /// Encode a count value for merge operations.
    pub fn encode_count(value: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::Count as u8);
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    /// Decode a count value from merged bytes.
    pub fn decode_count(data: &[u8]) -> Option<u64> {
        if data.len() < 9 || data[0] != MergeTag::Count as u8 {
            return None;
        }
        Some(u64::from_be_bytes(data[1..9].try_into().ok()?))
    }

    /// Encode a max-register value for merge operations.
    pub fn encode_max(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::MaxRegister as u8);
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    /// Decode a max-register value from merged bytes.
    pub fn decode_max(data: &[u8]) -> Option<i64> {
        if data.len() < 9 || data[0] != MergeTag::MaxRegister as u8 {
            return None;
        }
        Some(i64::from_be_bytes(data[1..9].try_into().ok()?))
    }

    /// Encode a min-register value for merge operations.
    pub fn encode_min(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::MinRegister as u8);
        out.extend_from_slice(&value.to_be_bytes());
        out
    }

    /// Decode a min-register value from merged bytes.
    pub fn decode_min(data: &[u8]) -> Option<i64> {
        if data.len() < 9 || data[0] != MergeTag::MinRegister as u8 {
            return None;
        }
        Some(i64::from_be_bytes(data[1..9].try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Bytes {
        Bytes::from_static(b"key")
    }

    #[test]
    fn sum_merge_no_existing() {
        let op = SumCountMergeOperator;
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(42));
        let result = op.merge(&key(), None, value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(42));
    }

    #[test]
    fn sum_merge_addition() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(10));
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(32));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(42));
    }

    #[test]
    fn sum_merge_negative() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(100));
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(-30));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(70));
    }

    #[test]
    fn count_merge_addition() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_count(5));
        let value = Bytes::from(MergeOperatorRegistry::encode_count(3));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_count(&result), Some(8));
    }

    #[test]
    fn tag_mismatch_returns_error() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(100));
        let value = Bytes::from(MergeOperatorRegistry::encode_count(1));
        let result = op.merge(&key(), Some(existing), value);
        // Fail-closed: tag mismatch is an error (RS-3009).
        assert!(result.is_err());
    }

    #[test]
    fn malformed_existing_returns_error() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from_static(b"short");
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(99));
        let result = op.merge(&key(), Some(existing), value);
        // Fail-closed: malformed operand is an error (RS-3009).
        assert!(result.is_err());
    }

    #[test]
    fn sum_is_associative() {
        let op = SumCountMergeOperator;
        let k = Bytes::from_static(b"k");
        let a = Bytes::from(MergeOperatorRegistry::encode_sum(1));
        let b = Bytes::from(MergeOperatorRegistry::encode_sum(2));
        let c = Bytes::from(MergeOperatorRegistry::encode_sum(3));

        // (a + b) + c
        let ab = op.merge(&k, Some(a.clone()), b.clone()).unwrap();
        let abc_left = op.merge(&k, Some(ab), c.clone()).unwrap();

        // a + (b + c)
        let bc = op.merge(&k, Some(b), c).unwrap();
        let abc_right = op.merge(&k, Some(a), bc).unwrap();

        assert_eq!(abc_left, abc_right);
        assert_eq!(MergeOperatorRegistry::decode_sum(&abc_left), Some(6));
    }

    #[test]
    fn count_is_associative() {
        let op = SumCountMergeOperator;
        let k = Bytes::from_static(b"k");
        let a = Bytes::from(MergeOperatorRegistry::encode_count(10));
        let b = Bytes::from(MergeOperatorRegistry::encode_count(20));
        let c = Bytes::from(MergeOperatorRegistry::encode_count(30));

        let ab = op.merge(&k, Some(a.clone()), b.clone()).unwrap();
        let abc_left = op.merge(&k, Some(ab), c.clone()).unwrap();

        let bc = op.merge(&k, Some(b), c).unwrap();
        let abc_right = op.merge(&k, Some(a), bc).unwrap();

        assert_eq!(abc_left, abc_right);
        assert_eq!(MergeOperatorRegistry::decode_count(&abc_left), Some(60));
    }
}

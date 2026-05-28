//! Paired assertion helper for durable/network boundaries.
//!
//! The paired assertion pattern (inspired by TigerBeetle) ensures that every
//! durable write or network send has a corresponding verification check. This
//! catches silent corruption or lost messages at system boundaries.
//!
//! Before writing to durable storage, call `before_boundary(&data)` to get
//! a token. After reading back, call `paired_assert(&read_data, &original, "label")`
//! to verify integrity.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A token representing the expected state at a boundary crossing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssertionToken {
    /// Hash of the data at the boundary.
    pub hash: u64,
    /// Human-readable label for diagnostics.
    pub label: &'static str,
}

/// Create an assertion token before a durable boundary crossing.
pub fn before_boundary<T: Hash>(data: &T, label: &'static str) -> AssertionToken {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    AssertionToken {
        hash: hasher.finish(),
        label,
    }
}

/// Verify data after a boundary crossing matches the expected token.
/// Panics if the data does not match.
pub fn after_boundary<T: Hash>(data: &T, token: &AssertionToken) {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let actual_hash = hasher.finish();
    assert_eq!(
        actual_hash, token.hash,
        "Paired assertion failed at boundary '{}': data corruption detected \
         (expected hash {:016x}, got {actual_hash:016x})",
        token.label, token.hash
    );
}

/// Convenience function: assert that data round-trips correctly through
/// a serialize/deserialize boundary.
pub fn paired_assert<T: Hash + PartialEq>(
    original: &T,
    recovered: &T,
    label: &'static str,
) -> bool {
    let token = before_boundary(original, label);
    let mut hasher = DefaultHasher::new();
    recovered.hash(&mut hasher);
    let recovered_hash = hasher.finish();
    assert_eq!(
        recovered_hash, token.hash,
        "Paired assertion failed at boundary '{label}': data mismatch",
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_assert_success() {
        let data = vec![1u8, 2, 3, 4, 5];
        let token = before_boundary(&data, "test_write");
        after_boundary(&data, &token);
    }

    #[test]
    #[should_panic(expected = "Paired assertion failed")]
    fn paired_assert_detects_corruption() {
        let data = vec![1u8, 2, 3, 4, 5];
        let token = before_boundary(&data, "test_write");
        let corrupted = vec![1u8, 2, 3, 4, 99];
        after_boundary(&corrupted, &token);
    }

    #[test]
    fn paired_assert_convenience() {
        let original = "hello world".to_string();
        let recovered = "hello world".to_string();
        assert!(paired_assert(&original, &recovered, "string_roundtrip"));
    }

    #[test]
    #[should_panic(expected = "Paired assertion failed")]
    fn paired_assert_convenience_mismatch() {
        let original = "hello".to_string();
        let recovered = "world".to_string();
        paired_assert(&original, &recovered, "string_mismatch");
    }

    #[test]
    fn token_label_preserved() {
        let token = before_boundary(&42u64, "my_label");
        assert_eq!(token.label, "my_label");
    }
}

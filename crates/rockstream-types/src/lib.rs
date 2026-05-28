//! Shared types for RockStream.
//!
//! This crate defines core types used across the RockStream system:
//! timestamps, frontiers, Z-set rows, and schema definitions.

pub mod error_code;

/// Placeholder module for timestamp types.
pub mod timestamp {
    /// A logical epoch number.
    pub type Epoch = u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_u64() {
        let e: timestamp::Epoch = 42;
        assert_eq!(e, 42);
    }
}

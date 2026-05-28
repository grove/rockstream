//! Error-code registry for RockStream.
//!
//! Every user-visible or operator-visible failure carries an `RS-XXXX` code.
//! This module defines the canonical registry.

use std::fmt;

/// An error code in the `RS-XXXX` format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(u16);

impl ErrorCode {
    /// Create a new error code from a numeric value.
    pub const fn new(code: u16) -> Self {
        Self(code)
    }

    /// Get the numeric value.
    pub const fn value(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RS-{:04}", self.0)
    }
}

// ─── Registry ────────────────────────────────────────────────────────────────

// 0xxx: Internal / general
/// Internal error.
pub const RS_0001: ErrorCode = ErrorCode::new(1);
/// Configuration error.
pub const RS_0002: ErrorCode = ErrorCode::new(2);
/// Storage unavailable.
pub const RS_0003: ErrorCode = ErrorCode::new(3);

// 1xxx: Pipeline / plan
/// Pipeline not found.
pub const RS_1001: ErrorCode = ErrorCode::new(1001);
/// Incompatible schema change.
pub const RS_1002: ErrorCode = ErrorCode::new(1002);
/// Record decode error (DLQ).
pub const RS_1003: ErrorCode = ErrorCode::new(1003);
/// Pipeline already exists.
pub const RS_1004: ErrorCode = ErrorCode::new(1004);

// 2xxx: Gateway / query
/// View not found.
pub const RS_2001: ErrorCode = ErrorCode::new(2001);
/// Query timeout.
pub const RS_2002: ErrorCode = ErrorCode::new(2002);
/// Unsupported isolation level.
pub const RS_2003: ErrorCode = ErrorCode::new(2003);

// 4xxx: Connector
/// Source connection failed.
pub const RS_4001: ErrorCode = ErrorCode::new(4001);
/// Sink write failed.
pub const RS_4002: ErrorCode = ErrorCode::new(4002);

// 5xxx: Upgrade / migration
/// Incompatible storage format.
pub const RS_5001: ErrorCode = ErrorCode::new(5001);

/// Returns a human-readable description for a known error code.
pub fn description(code: ErrorCode) -> &'static str {
    match code.0 {
        1 => "Internal error",
        2 => "Configuration error",
        3 => "Storage unavailable",
        1001 => "Pipeline not found",
        1002 => "Incompatible schema change",
        1003 => "Record decode error",
        1004 => "Pipeline already exists",
        2001 => "View not found",
        2002 => "Query timeout",
        2003 => "Unsupported isolation level",
        4001 => "Source connection failed",
        4002 => "Sink write failed",
        5001 => "Incompatible storage format",
        _ => "Unknown error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(RS_0001.to_string(), "RS-0001");
        assert_eq!(RS_1002.to_string(), "RS-1002");
        assert_eq!(RS_5001.to_string(), "RS-5001");
    }

    #[test]
    fn error_code_value() {
        assert_eq!(RS_0001.value(), 1);
        assert_eq!(RS_2003.value(), 2003);
    }

    #[test]
    fn description_known_codes() {
        assert_eq!(description(RS_0001), "Internal error");
        assert_eq!(description(RS_1002), "Incompatible schema change");
        assert_eq!(description(RS_5001), "Incompatible storage format");
    }

    #[test]
    fn description_unknown_code() {
        assert_eq!(description(ErrorCode::new(9999)), "Unknown error");
    }

    #[test]
    fn all_codes_have_descriptions() {
        let codes = [
            RS_0001, RS_0002, RS_0003, RS_1001, RS_1002, RS_1003, RS_1004, RS_2001, RS_2002,
            RS_2003, RS_4001, RS_4002, RS_5001,
        ];
        for code in codes {
            assert_ne!(
                description(code),
                "Unknown error",
                "Code {code} has no description"
            );
        }
    }
}

//! Catalog error types with RS-XXXX error codes.

use rockstream_types::error_code::{ErrorCode, RS_1002, RS_5002};
use thiserror::Error;

/// Errors from the catalog layer.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Incompatible schema change attempted — `RS-1002`.
    ///
    /// Returned when a schema evolution operation would break existing
    /// consumers (e.g., removing a non-nullable column, changing a column
    /// type in an incompatible way, renaming a column).
    #[error("RS-1002: incompatible schema change: {reason}")]
    IncompatibleSchemaChange { reason: String },

    /// Unknown merge law referenced in a persisted plan — `RS-5002`.
    ///
    /// Returned when a plan is loaded from storage and references a
    /// `(law_id, law_version)` that is not registered in the current
    /// `LawRegistry`. The plan cannot be safely replayed until the law is
    /// registered or the arrangement is migrated.
    #[error("RS-5002: unknown merge law law-{law_id:04} v{law_version} in persisted plan for operator {operator_path:?}")]
    UnknownMergeLaw {
        law_id: u16,
        law_version: u16,
        operator_path: String,
    },

    /// Plan codec error (malformed bytes, unknown format, version mismatch).
    #[error("plan codec error: {0}")]
    Codec(String),

    /// Entry already exists in the catalog.
    #[error("catalog entry already exists: {name}")]
    AlreadyExists { name: String },

    /// Entry not found in the catalog.
    #[error("catalog entry not found: {name}")]
    NotFound { name: String },
}

impl CatalogError {
    /// Returns the RS-XXXX error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::IncompatibleSchemaChange { .. } => RS_1002,
            Self::UnknownMergeLaw { .. } => RS_5002,
            Self::Codec(_) | Self::AlreadyExists { .. } | Self::NotFound { .. } => {
                // Use RS-0001 (internal error) for codec/store errors.
                rockstream_types::error_code::RS_0001
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incompatible_schema_change_has_rs_1002() {
        let err = CatalogError::IncompatibleSchemaChange {
            reason: "column 'id' removed".into(),
        };
        assert_eq!(err.error_code(), RS_1002);
        assert!(err.to_string().contains("RS-1002"));
    }

    #[test]
    fn unknown_merge_law_has_rs_5002() {
        let err = CatalogError::UnknownMergeLaw {
            law_id: 42,
            law_version: 3,
            operator_path: "root/agg".into(),
        };
        assert_eq!(err.error_code(), RS_5002);
        assert!(err.to_string().contains("RS-5002"));
    }
}

//! Storage error types.

use thiserror::Error;

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SlateDB error: {0}")]
    Slate(#[from] slatedb::Error),

    #[error("key encoding error: {0}")]
    KeyEncoding(String),

    #[error("merge operator not configured")]
    MergeOperatorNotConfigured,

    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

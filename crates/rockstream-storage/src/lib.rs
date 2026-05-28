//! Storage abstraction layer for RockStream.
//!
//! Wraps SlateDB and provides:
//! - Key encoders/decoders with `namespace_id` in all catalog keys
//! - `ShardDb` for per-shard database access
//! - `WriteBatch` builders for atomic epoch commits
//! - `DbReader` for cross-shard snapshot reads
//! - Merge operator registry for associative aggregates
//! - WAL reader utilities
//!
//! No code path depends on range deletion. Cleanup uses
//! scan-and-delete or compaction-filter patterns.

pub mod error;
pub mod keys;
pub mod merge_registry;
pub mod reader;
pub mod shard_db;
pub mod wal;

pub use error::StorageError;
pub use keys::{CatalogKeyEncoder, ShardKeyEncoder, ShardPrefix};
pub use merge_registry::{MergeOperatorRegistry, SumCountMergeOperator};
pub use reader::ShardReader;
pub use shard_db::ShardDb;

#[cfg(test)]
mod tests;

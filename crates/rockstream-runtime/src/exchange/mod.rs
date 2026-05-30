//! Exchange subsystem for RockStream.
//!
//! Implements the four-path exchange router described in DESIGN.md §7.5:
//!
//! | Path       | Description                                                |
//! |------------|------------------------------------------------------------|
//! | `Elided`   | No movement; source == target shard on same worker.       |
//! | `Loopback` | In-process bounded channel; zero network calls.           |
//! | `Direct`   | Worker-to-worker gRPC shuffle (stub for v0.30).           |
//! | `Durable`  | Object-store fallback; future v0.31 implementation.       |
//!
//! ## Pre-shuffle combiner
//!
//! `PreShuffleCombiner` reduces bytes sent over the wire by merging rows
//! that share the same key using the planner-provided `MergeLawId`.  The
//! combiner is generic: it dispatches into the registered `LawBundle` for
//! the annotated law.  No hard-coded SUM/COUNT/AVG list exists.
//!
//! ## Credit backpressure
//!
//! `CreditTracker` provides a bounded semaphore that limits the number of
//! in-flight batches per sender, preventing unbounded buffering in the
//! exchange layer.

pub mod arrow_ipc;
pub mod combiner;
pub mod credit;
pub mod loopback;
pub mod path;

pub use arrow_ipc::{decode_batch, encode_batch, encoded_size};
pub use combiner::{CombineStats, PreShuffleCombiner};
pub use credit::CreditTracker;
pub use loopback::{ExchangeBatch, LoopbackChannel, LoopbackReceiver};
pub use path::ExchangeClassifier;

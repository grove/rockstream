//! Exchange subsystem for RockStream.
//!
//! Implements the four-path exchange router described in DESIGN.md §7.5:
//!
//! | Path       | Description                                                |
//! |------------|------------------------------------------------------------|
//! | `Elided`   | No movement; source == target shard on same worker.       |
//! | `Loopback` | In-process bounded channel; zero network calls.           |
//! | `Direct`   | Worker-to-worker gRPC shuffle.                            |
//! | `Durable`  | Object-store fallback with law-aware re-merge (v0.31).    |
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
//!
//! ## Durable shuffle fallback (v0.31)
//!
//! `DurableShuffleWriter` writes coalesced shuffle objects to the object
//! store.  `DurableShuffleReader` fetches them using `OutboxEntry` metadata
//! — no LIST call is ever issued on the hot path.  The reader re-merges
//! per-target operands using the registered `LawBundle`, producing
//! bit-identical state to the direct path.

pub mod arrow_ipc;
pub mod combiner;
pub mod credit;
pub mod durable;
pub mod loopback;
pub mod path;

pub use arrow_ipc::{decode_batch, encode_batch, encoded_size};
pub use combiner::{CombineStats, PreShuffleCombiner};
pub use credit::CreditTracker;
pub use durable::{
    decode_object, encode_object, DurableError, DurableShuffleReader, DurableShuffleWriter,
    OutboxEntry, ShuffleFrame,
};
pub use loopback::{ExchangeBatch, LoopbackChannel, LoopbackReceiver};
pub use path::ExchangeClassifier;

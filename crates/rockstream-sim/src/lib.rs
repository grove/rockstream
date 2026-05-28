//! Deterministic simulation harness for RockStream.
//!
//! This crate provides the [`Runtime`] trait that abstracts time, task spawning,
//! sleep, object storage, and network I/O. Two implementations exist:
//!
//! - [`TokioRuntime`]: Production runtime backed by Tokio.
//! - [`SimRuntime`]: Deterministic, seeded-RNG simulation runtime for testing.
//!
//! The [`buggify!`] macro injects faults during simulation builds (feature
//! `simulation`) and compiles to a no-op in production builds.
//!
//! Every operator, scheduler, and storage call site in RockStream is
//! parameterized on the `Runtime` trait so that tests can deterministically
//! reproduce failures.

pub mod buggify;
pub mod clock;
pub mod fault_model;
pub mod network;
pub mod object_store;
pub mod paired_assert;
pub mod runtime;
pub mod sim;
pub mod tokio_rt;

pub use buggify::buggify_enabled;
pub use clock::{Clock, SimClock, TokioClock};
pub use fault_model::{FaultEntry, FaultModel};
pub use network::{SimNetwork, SimNetworkHandle};
pub use object_store::{SimObjectStore, SimObjectStoreHandle};
pub use paired_assert::paired_assert;
pub use runtime::Runtime;
pub use sim::SimRuntime;
pub use tokio_rt::TokioRuntime;

#[cfg(test)]
mod tests;

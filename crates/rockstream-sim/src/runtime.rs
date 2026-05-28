//! The `Runtime` trait: the core abstraction for deterministic simulation.
//!
//! All I/O and time operations in RockStream go through this trait, allowing
//! the simulation runtime to control execution deterministically.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::clock::Clock;
use crate::network::SimNetworkHandle;
use crate::object_store::SimObjectStoreHandle;

/// A boxed future that is Send.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The Runtime trait abstracts time, task spawning, sleep, object storage,
/// and network for deterministic simulation.
pub trait Runtime: Send + Sync + 'static {
    /// The clock type for this runtime.
    type Clock: Clock;

    /// Get a reference to the clock.
    fn clock(&self) -> &Self::Clock;

    /// Sleep for the given duration.
    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()>;

    /// Spawn a task. The task runs concurrently.
    fn spawn<F>(&self, name: &'static str, future: F)
    where
        F: Future<Output = ()> + Send + 'static;

    /// Get the object store handle.
    fn object_store(&self) -> &SimObjectStoreHandle;

    /// Get the network handle.
    fn network(&self) -> &SimNetworkHandle;

    /// Get the seed used by this runtime (for reproducibility logging).
    fn seed(&self) -> u64;

    /// Whether this runtime is in simulation mode.
    fn is_simulation(&self) -> bool;
}

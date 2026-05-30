//! Control-plane service for RockStream.
//!
//! Manages cluster topology, pipeline lifecycle, shard scheduling, and
//! distributed coordination.
//!
//! ## Modules
//!
//! - [`audit`] — File-backed audit log (JSONL)
//! - [`topology`] — In-memory worker registry / topology catalog
//! - [`placement`] — Capacity-aware shard and operator placement
//! - [`service`] — TCP control service for worker registration
//! - [`tls`] — mTLS configuration scaffolding

pub mod audit;
pub mod placement;
pub mod service;
pub mod tls;
pub mod topology;

// Re-export commonly used top-level types.
pub use placement::PlacementAlgorithm;
pub use service::{ControlService, ControlServiceHandle};
pub use tls::TlsConfig;
pub use topology::TopologyCatalog;

#[cfg(test)]
mod tests {
    #[test]
    fn control_crate_compiles() {}
}

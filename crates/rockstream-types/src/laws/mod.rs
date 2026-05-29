//! Built-in merge law implementations.
//!
//! Each law implements `LawBundle` and is registered in the global law registry.
//! v0.5 ships `WeightAdd/v1` — the fundamental Z-set weight addition law.

pub mod registry;
pub mod weight_add;

pub use registry::LawRegistry;
pub use weight_add::WeightAddV1;

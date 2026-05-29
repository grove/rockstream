//! Built-in merge law implementations.
//!
//! Each law implements `LawBundle` and is registered in the global law registry.
//! v0.5 ships `WeightAdd/v1` — the fundamental Z-set weight addition law.
//! v0.7 adds `SumCount/v1` — the abelian-group aggregate law (SUM, COUNT, AVG).

pub mod registry;
pub mod sum_count;
pub mod weight_add;

pub use registry::LawRegistry;
pub use sum_count::SumCountV1;
pub use weight_add::WeightAddV1;

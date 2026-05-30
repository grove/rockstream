//! Built-in merge law implementations.
//!
//! Each law implements `LawBundle` and is registered in the global law registry.
//! v0.5 ships `WeightAdd/v1` — the fundamental Z-set weight addition law.
//! v0.7 adds `SumCount/v1` — the abelian-group aggregate law (SUM, COUNT, AVG).
//! v0.8 adds `MaxRegister/v1` and `MinRegister/v1` — semilattice cached-slot
//!      laws backing the retraction-aware `MinMaxOp` operator.
//! v0.21 adds `HyperLogLog/v1` — semilattice sketch law for planner NDV
//!       estimation.
//! v0.25 adds `BloomUnion/v1` — semilattice sketch law for `APPROX_MEMBERSHIP`.

pub mod bloom_union;
pub mod hyper_log_log;
pub mod max_register;
pub mod min_register;
pub mod registry;
pub mod sum_count;
pub mod weight_add;

pub use bloom_union::BloomUnionV1;
pub use hyper_log_log::HyperLogLogV1;
pub use max_register::MaxRegisterV1;
pub use min_register::MinRegisterV1;
pub use registry::LawRegistry;
pub use sum_count::SumCountV1;
pub use weight_add::WeightAddV1;

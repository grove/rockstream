//! Operator trait and per-operator implementations for RockStream.
//!
//! Each IVM operator (filter, project, aggregate, join, etc.) implements
//! the `Operator` trait defined here.

pub mod aggregate;
pub mod distinct;
pub mod epoch_output;
pub mod filter;
pub mod join;
pub mod map;
pub mod min_max;
pub mod noop;
pub mod operator;
pub mod outer_join;
pub mod project;
pub mod recursive;
pub mod row_codec;
pub mod scheduler;
pub mod set_ops;
pub mod task;
pub mod top_k;
pub mod tumble;
pub mod window;

#[cfg(test)]
mod tests {
    #[test]
    fn ops_crate_compiles() {}
}

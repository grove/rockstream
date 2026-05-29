//! Operator trait and per-operator implementations for RockStream.
//!
//! Each IVM operator (filter, project, aggregate, join, etc.) implements
//! the `Operator` trait defined here.

pub mod epoch_output;
pub mod filter;
pub mod map;
pub mod noop;
pub mod operator;
pub mod project;
pub mod row_codec;
pub mod task;

#[cfg(test)]
mod tests {
    #[test]
    fn ops_crate_compiles() {}
}

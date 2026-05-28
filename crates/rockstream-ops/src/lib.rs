//! Operator trait and per-operator implementations for RockStream.
//!
//! Each IVM operator (filter, project, aggregate, join, etc.) implements
//! the `Operator` trait defined here.

pub mod noop;
pub mod operator;

#[cfg(test)]
mod tests {
    #[test]
    fn ops_crate_compiles() {}
}

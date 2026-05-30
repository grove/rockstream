//! Batch reference engine and property-test harness for RockStream.
//!
//! Implements the DBSP soundness theorem assertion:
//! `incremental(query, deltas) == batch(query, accumulated)`
//!
//! Also provides the **law property-test harness**: a generic test suite that
//! every `LawBundle` implementation must pass to be considered correct. The
//! harness verifies associativity, commutativity, identity, idempotence
//! (where declared), serialization round-trip, and fail-closed malformed
//! operand handling.

pub mod aggregate_oracle;
pub mod batch_oracle;
pub mod bootstrap_oracle;
pub mod distributed_recursive_oracle;
pub mod fuzzer_oracle;
pub mod join_oracle;
pub mod lateral_srf_oracle;
pub mod law_equiv_oracle;
pub mod law_harness;
pub mod min_max_oracle;
pub mod nexmark_oracle;
pub mod recursive_oracle;
pub mod set_op_oracle;
pub mod top_k_oracle;
pub mod tpch_oracle;
pub mod tumble_oracle;
pub mod view_dag_oracle;
pub mod window_oracle;

#[cfg(test)]
mod tests {
    #[test]
    fn oracle_crate_compiles() {}
}

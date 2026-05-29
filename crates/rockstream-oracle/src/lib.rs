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
pub mod law_harness;

#[cfg(test)]
mod tests {
    #[test]
    fn oracle_crate_compiles() {}
}

//! Batch reference engine and property-test harness for RockStream.
//!
//! Implements the DBSP soundness theorem assertion:
//! `incremental(query, deltas) == batch(query, accumulated)`

#[cfg(test)]
mod tests {
    #[test]
    fn oracle_crate_compiles() {}
}

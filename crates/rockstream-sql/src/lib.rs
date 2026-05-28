//! SQL frontend for RockStream.
//!
//! Built on Apache DataFusion. Parses SQL, binds schemas, optimizes logical
//! plans, and lowers to the RockStream PlanNode IR.

#[cfg(test)]
mod tests {
    #[test]
    fn sql_crate_compiles() {}
}

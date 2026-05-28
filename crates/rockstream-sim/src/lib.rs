//! Deterministic simulation harness for RockStream.
//!
//! Provides the `Runtime` trait abstracting time, spawn, sleep, object store,
//! and network. `TokioRuntime` is for production; `SimRuntime` is an in-memory,
//! seeded-RNG implementation for deterministic testing.
//!
//! The `buggify!()` macro is a no-op in release builds and injects faults
//! in simulation builds.

#[cfg(test)]
mod tests {
    #[test]
    fn sim_crate_compiles() {}
}

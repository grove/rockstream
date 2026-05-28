//! Worker process, circuit executor, scheduler, and exchange for RockStream.
//!
//! Contains the per-worker runtime: operator scheduling, epoch coordination,
//! and shuffle/exchange paths.

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_crate_compiles() {}
}

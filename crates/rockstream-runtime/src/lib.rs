//! Worker process, circuit executor, scheduler, and exchange for RockStream.
//!
//! Contains the per-worker runtime: operator scheduling, epoch coordination,
//! and shuffle/exchange paths.

pub mod bench;
pub mod epoch_coordinator;
pub mod explain;
pub mod pipeline;
pub mod support_bundle;

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_crate_compiles() {}
}

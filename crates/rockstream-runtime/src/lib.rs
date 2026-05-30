//! Worker process, circuit executor, scheduler, and exchange for RockStream.
//!
//! Contains the per-worker runtime: operator scheduling, epoch coordination,
//! and shuffle/exchange paths.
//!
//! v0.32 adds the frontier protocol: per-shard reporters, worker-level
//! aggregators, cluster frontier publication, shuffle GC, and monotone
//! partial-progress tokens.

pub mod bench;
pub mod epoch_coordinator;
pub mod exchange;
pub mod explain;
pub mod frontier;
pub mod pipeline;
pub mod support_bundle;

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_crate_compiles() {}
}

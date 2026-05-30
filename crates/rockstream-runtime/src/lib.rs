//! Worker process, circuit executor, scheduler, and exchange for RockStream.
//!
//! Contains the per-worker runtime: operator scheduling, epoch coordination,
//! and shuffle/exchange paths.
//!
//! v0.32 adds the frontier protocol: per-shard reporters, worker-level
//! aggregators, cluster frontier publication, shuffle GC, and monotone
//! partial-progress tokens.
//!
//! v0.34 adds the cluster checkpoint protocol: barrier injection, bounded
//! alignment buffers, per-shard checkpoint creation, atomic cluster checkpoint
//! commit, and old checkpoint GC.

pub mod bench;
pub mod checkpoint;
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

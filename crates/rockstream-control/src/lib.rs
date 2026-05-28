//! Control-plane service for RockStream.
//!
//! Manages cluster topology, pipeline lifecycle, shard scheduling, and
//! distributed coordination.

pub mod audit;

#[cfg(test)]
mod tests {
    #[test]
    fn control_crate_compiles() {}
}

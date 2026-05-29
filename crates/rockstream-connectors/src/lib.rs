//! Source and sink connector implementations for RockStream.
//!
//! Each connector implements the Tier 1 or Tier 2 contract defined in
//! DESIGN.md §13.3.

pub mod fixed_source;
pub mod noop_sink;
pub mod noop_source;
pub mod sink;
pub mod source;

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}

//! Source and sink connector implementations for RockStream.
//!
//! Each connector implements the Tier 1 or Tier 2 contract defined in
//! DESIGN.md §13.3.

pub mod fixed_source;
pub mod generate_rows;
pub mod noop_sink;
pub mod noop_source;
pub mod sink;
pub mod source;

pub use generate_rows::{GenerateRowsConfig, GenerateRowsSource};

#[cfg(test)]
mod tests {
    #[test]
    fn connectors_crate_compiles() {}
}

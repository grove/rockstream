//! Pre-shuffle combiner driven by planner-attached `MergeLawId`.
//!
//! The combiner reduces bytes sent over the exchange wire by merging rows
//! that share the same key using the registered `LawBundle`.  It is driven
//! **entirely** by the `MergeLawId` supplied at plan time; there is no
//! hard-coded list of aggregate functions.
//!
//! ## Correctness guarantee
//!
//! For any associative, commutative law L and any multiset of rows with key
//! K:
//!
//! ```text
//! combine_then_merge(rows) == merge_all(rows)
//! ```
//!
//! i.e. pre-combining is semantically equivalent to forwarding all rows
//! uncombined and merging at the receiver.  This is proven by the CI
//! property tests in `rockstream_runtime::exchange::tests`.
//!
//! ## Bytes-avoided metric
//!
//! `CombineStats` records `input_bytes`, `output_bytes`, and
//! `bytes_avoided = input_bytes - output_bytes` so benchmarks can measure
//! the savings per registered law.

use rockstream_types::laws::LawRegistry;
use rockstream_types::merge_law::MergeLawId;
use std::collections::HashMap;
use std::sync::Arc;

/// A batch of `(key_bytes, value_bytes)` pairs used by the combiner.
type KeyValueBatch = Vec<(Vec<u8>, Vec<u8>)>;

/// Statistics produced after a combine pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombineStats {
    /// Number of input rows.
    pub input_count: usize,
    /// Number of output rows after combining.
    pub output_count: usize,
    /// Total bytes across all input `(key, value)` pairs.
    pub input_bytes: usize,
    /// Total bytes across all output `(key, value)` pairs.
    pub output_bytes: usize,
    /// Bytes eliminated by combining: `input_bytes - output_bytes`.
    pub bytes_avoided: usize,
}

/// Pre-shuffle combiner.
///
/// Groups a batch of `(key, value)` pairs by key and merges values using the
/// law identified by `law_id`.  Rows whose key appears only once pass through
/// unchanged (zero-overhead fast path).
pub struct PreShuffleCombiner {
    registry: Arc<LawRegistry>,
}

impl PreShuffleCombiner {
    /// Create a new combiner backed by the given law registry.
    pub fn new(registry: Arc<LawRegistry>) -> Self {
        PreShuffleCombiner { registry }
    }

    /// Combine a batch using the law identified by `law_id`.
    ///
    /// Returns the reduced batch and per-pass statistics.
    ///
    /// # Errors
    ///
    /// Returns an error string if `law_id` is not registered or if the
    /// law's `merge` function rejects a value pair.
    pub fn combine(
        &self,
        law_id: MergeLawId,
        batch: KeyValueBatch,
    ) -> Result<(KeyValueBatch, CombineStats), String> {
        let law = self
            .registry
            .get(law_id)
            .ok_or_else(|| format!("unknown law {law_id}"))?;

        let input_count = batch.len();
        let input_bytes: usize = batch.iter().map(|(k, v)| k.len() + v.len()).sum();

        // Group by key, merging values as we go.
        let mut combined: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (key, value) in batch {
            match combined.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let merged = law.merge(e.get(), &value)?;
                    *e.get_mut() = merged;
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(value);
                }
            }
        }

        let output: Vec<(Vec<u8>, Vec<u8>)> = combined.into_iter().collect();
        let output_bytes: usize = output.iter().map(|(k, v)| k.len() + v.len()).sum();

        let stats = CombineStats {
            input_count,
            output_count: output.len(),
            input_bytes,
            output_bytes,
            bytes_avoided: input_bytes.saturating_sub(output_bytes),
        };

        Ok((output, stats))
    }

    /// Merge all values in `batch` into a single value for the given law.
    ///
    /// This is the "uncombined receiver" operation: it merges every value
    /// in the batch regardless of key (used in equivalence proofs to verify
    /// that combine-then-merge == merge-all).
    pub fn merge_all(
        &self,
        law_id: MergeLawId,
        batch: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, String> {
        let law = self
            .registry
            .get(law_id)
            .ok_or_else(|| format!("unknown law {law_id}"))?;

        let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for (key, value) in batch {
            match state.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let merged = law.merge(e.get(), value)?;
                    *e.get_mut() = merged;
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert(value.clone());
                }
            }
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::laws::WEIGHT_ADD_ID;
    use rockstream_types::laws::{
        BloomUnionV1, HyperLogLogV1, MaxRegisterV1, MinRegisterV1, SumCountV1, WeightAddV1,
    };

    fn make_combiner() -> PreShuffleCombiner {
        let mut registry = LawRegistry::new();
        registry.register(Arc::new(WeightAddV1));
        registry.register(Arc::new(SumCountV1));
        registry.register(Arc::new(MaxRegisterV1));
        registry.register(Arc::new(MinRegisterV1));
        registry.register(Arc::new(HyperLogLogV1));
        registry.register(Arc::new(BloomUnionV1));
        PreShuffleCombiner::new(Arc::new(registry))
    }

    #[test]
    fn unknown_law_returns_error() {
        let combiner = make_combiner();
        let bad_law = MergeLawId(0xFFFF);
        let result = combiner.combine(bad_law, vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown law"));
    }

    #[test]
    fn combine_empty_batch() {
        let combiner = make_combiner();
        let (out, stats) = combiner.combine(WEIGHT_ADD_ID, vec![]).unwrap();
        assert!(out.is_empty());
        assert_eq!(stats.input_count, 0);
        assert_eq!(stats.output_count, 0);
        assert_eq!(stats.bytes_avoided, 0);
    }
}

//! Law-equivalence corpus oracle for RockStream IVM.
//!
//! Proves that for every registered merge law, executing a query via the
//! **law-merge path** (incremental accumulation using the law's `merge`
//! function) produces the **identical result** as executing the same query
//! via the **batch-recompute path** (full recomputation from accumulated
//! state without using the law).
//!
//! # Registered laws (v0.26)
//!
//! | ID     | Name           | Class          |
//! |--------|----------------|----------------|
//! | 0x0001 | WeightAdd/v1   | AbelianGroup   |
//! | 0x0002 | SumCount/v1    | AbelianGroup   |
//! | 0x0003 | MaxRegister/v1 | Semilattice    |
//! | 0x0004 | MinRegister/v1 | Semilattice    |
//! | 0x0005 | HyperLogLog/v1 | Semilattice    |
//! | 0x0006 | BloomUnion/v1  | Semilattice    |
//!
//! # Equivalence criterion
//!
//! For all laws `L` and all input delta streams `{d_1, …, d_n}`:
//!
//! ```text
//! L.law_path(d_1, …, d_n) == L.batch_path(Σ d_i)
//! ```
//!
//! where `Σ d_i` is the accumulated (full) state.

use std::collections::HashMap;

use rockstream_types::laws::bloom_union::BloomUnionV1;
use rockstream_types::laws::hyper_log_log::{HyperLogLogV1, HLL_WIRE_SIZE};
use rockstream_types::laws::max_register::{encode_max_register, MaxRegisterV1};
use rockstream_types::laws::min_register::{encode_min_register, MinRegisterV1};
use rockstream_types::laws::sum_count::{decode_sum_count, encode_sum_count, SumCountV1};
use rockstream_types::laws::weight_add::{decode_weight, encode_weight, WeightAddV1};
use rockstream_types::merge_law::LawBundle;

// ─── LawEquivResult ──────────────────────────────────────────────────────────

/// The result of a single law-equivalence proof run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LawEquivResult {
    /// Law-merge path and batch-recompute path produced identical output.
    Equivalent,
    /// The two paths diverged; includes a diagnostic message.
    Diverged(String),
}

impl LawEquivResult {
    /// Returns true if the result is `Equivalent`.
    pub fn is_equivalent(&self) -> bool {
        matches!(self, LawEquivResult::Equivalent)
    }
}

// ─── WeightAdd law equivalence ────────────────────────────────────────────────

/// Prove WeightAdd/v1 law equivalence.
///
/// Query: `SUM(value) GROUP BY key` over a stream of `(key, value, weight)` rows.
///
/// - **Law path**: accumulate per-group sum by repeatedly calling
///   `WeightAddV1::merge` on the per-group state.
/// - **Batch path**: iterate the full accumulated state and compute the sum
///   by plain i64 arithmetic (no law involved).
///
/// Both paths must produce identical `HashMap<group_key, sum>`.
pub fn check_weight_add_equivalence(rows: &[(i64, i64, i64)]) -> LawEquivResult {
    let law = WeightAddV1;

    // Law path: incrementally merge per-group accumulators.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value, weight) in rows {
        let contribution = value * weight;
        let contrib_bytes = encode_weight(contribution);
        let state = law_state
            .entry(*key)
            .or_insert_with(|| law.identity().unwrap_or_else(|| encode_weight(0)));
        *state = law.merge(state, &contrib_bytes).expect("WeightAdd merge");
    }
    let law_result: HashMap<i64, i64> = law_state
        .iter()
        .filter_map(|(k, v)| {
            let w = decode_weight(v).ok()?;
            if w == 0 {
                None
            } else {
                Some((*k, w))
            }
        })
        .collect();

    // Batch path: iterate accumulated state with plain arithmetic.
    let mut batch_state: HashMap<i64, i64> = HashMap::new();
    for (key, value, weight) in rows {
        *batch_state.entry(*key).or_insert(0) += value * weight;
    }
    batch_state.retain(|_, v| *v != 0);

    if law_result == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(format!(
            "WeightAdd law path: {law_result:?} != batch path: {batch_state:?}"
        ))
    }
}

// ─── SumCount law equivalence ─────────────────────────────────────────────────

/// Prove SumCount/v1 law equivalence.
///
/// Query: `SUM(value), COUNT(*) GROUP BY key`.
///
/// - **Law path**: accumulate per-group (sum, count) by calling
///   `SumCountV1::merge` on the per-group state.
/// - **Batch path**: plain i64 arithmetic over the full state.
pub fn check_sum_count_equivalence(rows: &[(i64, i64, i64)]) -> LawEquivResult {
    let law = SumCountV1;
    let identity = encode_sum_count(0, 0);

    // Law path.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value, weight) in rows {
        let contribution = encode_sum_count(value * weight, *weight);
        let state = law_state.entry(*key).or_insert_with(|| identity.clone());
        *state = law.merge(state, &contribution).expect("SumCount merge");
    }
    let law_result: HashMap<i64, (i64, i64)> = law_state
        .iter()
        .filter_map(|(k, v)| {
            let (sum, count) = decode_sum_count(v).ok()?;
            if count == 0 && sum == 0 {
                None
            } else {
                Some((*k, (sum, count)))
            }
        })
        .collect();

    // Batch path.
    let mut batch_state: HashMap<i64, (i64, i64)> = HashMap::new();
    for (key, value, weight) in rows {
        let e = batch_state.entry(*key).or_insert((0, 0));
        e.0 += value * weight;
        e.1 += weight;
    }
    batch_state.retain(|_, (s, c)| *s != 0 || *c != 0);

    if law_result == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(format!(
            "SumCount law path: {law_result:?} != batch path: {batch_state:?}"
        ))
    }
}

// ─── MaxRegister law equivalence ─────────────────────────────────────────────

/// Prove MaxRegister/v1 law equivalence.
///
/// Query: `MAX(value) GROUP BY key`.
///
/// - **Law path**: accumulate per-group max by calling
///   `MaxRegisterV1::merge` on the per-group state.
/// - **Batch path**: `i64::max` over the full accumulated state.
///
/// Note: MaxRegister is a semilattice (no inverse). For positive-weight-only
/// inputs, the law path and batch path are equivalent. Negative weights
/// (deletions) are excluded from the input for this test since MaxRegister
/// is not invertible.
pub fn check_max_register_equivalence(rows: &[(i64, i64)]) -> LawEquivResult {
    let law = MaxRegisterV1;
    let identity = encode_max_register(i64::MIN);

    // Law path.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let val_bytes = encode_max_register(*value);
        let state = law_state.entry(*key).or_insert_with(|| identity.clone());
        *state = law.merge(state, &val_bytes).expect("MaxRegister merge");
    }
    let law_result: HashMap<i64, i64> = law_state
        .iter()
        .map(|(k, v)| {
            let val = if v.len() >= 8 {
                i64::from_be_bytes(v[..8].try_into().unwrap_or([0u8; 8]))
            } else {
                i64::MIN
            };
            (*k, val)
        })
        .filter(|(_, v)| *v != i64::MIN)
        .collect();

    // Batch path.
    let mut batch_state: HashMap<i64, i64> = HashMap::new();
    for (key, value) in rows {
        let entry = batch_state.entry(*key).or_insert(i64::MIN);
        if *value > *entry {
            *entry = *value;
        }
    }
    batch_state.retain(|_, v| *v != i64::MIN);

    if law_result == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(format!(
            "MaxRegister law path: {law_result:?} != batch path: {batch_state:?}"
        ))
    }
}

// ─── MinRegister law equivalence ─────────────────────────────────────────────

/// Prove MinRegister/v1 law equivalence.
///
/// Query: `MIN(value) GROUP BY key`.
///
/// Same structure as MaxRegister but using `MinRegisterV1` and `min()`.
pub fn check_min_register_equivalence(rows: &[(i64, i64)]) -> LawEquivResult {
    let law = MinRegisterV1;
    let identity = encode_min_register(i64::MAX);

    // Law path.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let val_bytes = encode_min_register(*value);
        let state = law_state.entry(*key).or_insert_with(|| identity.clone());
        *state = law.merge(state, &val_bytes).expect("MinRegister merge");
    }
    let law_result: HashMap<i64, i64> = law_state
        .iter()
        .map(|(k, v)| {
            let val = if v.len() >= 8 {
                i64::from_be_bytes(v[..8].try_into().unwrap_or([0u8; 8]))
            } else {
                i64::MAX
            };
            (*k, val)
        })
        .filter(|(_, v)| *v != i64::MAX)
        .collect();

    // Batch path.
    let mut batch_state: HashMap<i64, i64> = HashMap::new();
    for (key, value) in rows {
        let entry = batch_state.entry(*key).or_insert(i64::MAX);
        if *value < *entry {
            *entry = *value;
        }
    }
    batch_state.retain(|_, v| *v != i64::MAX);

    if law_result == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(format!(
            "MinRegister law path: {law_result:?} != batch path: {batch_state:?}"
        ))
    }
}

// ─── HyperLogLog law equivalence ─────────────────────────────────────────────

/// Prove HyperLogLog/v1 law equivalence.
///
/// Query: for each group key, compute the union sketch of all element hashes.
///
/// - **Law path**: call `HyperLogLogV1::merge` per element.
/// - **Batch path**: compute per-register max directly from element hashes.
///
/// The HLL sketch is constructed by hashing each element into a bucket
/// (element mod 64) and recording the "leading zeros + 1" of the element.
/// This is a simplified (deterministic) sketch for the proof test.
pub fn check_hll_equivalence(rows: &[(i64, i64)]) -> LawEquivResult {
    let law = HyperLogLogV1;
    let identity = vec![0u8; HLL_WIRE_SIZE];

    // Hash element to HLL sketch: set register[element % 64] to (leading_zeros + 1).
    let element_to_sketch = |element: i64| -> Vec<u8> {
        let mut sketch = vec![0u8; HLL_WIRE_SIZE];
        let bucket = (element.unsigned_abs() as usize) % HLL_WIRE_SIZE;
        let leading = element.unsigned_abs().leading_zeros() as u8 + 1;
        sketch[bucket] = leading;
        sketch
    };

    // Law path.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let sketch = element_to_sketch(*value);
        let state = law_state.entry(*key).or_insert_with(|| identity.clone());
        *state = law.merge(state, &sketch).expect("HyperLogLog merge");
    }

    // Batch path: per-register max.
    let mut batch_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let sketch = element_to_sketch(*value);
        let state = batch_state
            .entry(*key)
            .or_insert_with(|| vec![0u8; HLL_WIRE_SIZE]);
        for i in 0..HLL_WIRE_SIZE {
            if sketch[i] > state[i] {
                state[i] = sketch[i];
            }
        }
    }

    // Remove all-zero entries from both (those are empty groups).
    law_state.retain(|_, v| v.iter().any(|b| *b != 0));
    batch_state.retain(|_, v| v.iter().any(|b| *b != 0));

    if law_state == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(
            "HyperLogLog law path differs from batch path (register-level divergence)".to_string(),
        )
    }
}

// ─── BloomUnion law equivalence ──────────────────────────────────────────────

/// Prove BloomUnion/v1 law equivalence.
///
/// Query: for each group key, compute the union Bloom filter of all elements.
///
/// - **Law path**: call `BloomUnionV1::merge` per element.
/// - **Batch path**: bitwise OR of element hashes directly.
///
/// The Bloom filter is constructed by setting bit `hash(element) % 256`.
pub fn check_bloom_union_equivalence(rows: &[(i64, i64)]) -> LawEquivResult {
    use rockstream_types::laws::bloom_union::BLOOM_UNION_WIRE_SIZE;

    let law = BloomUnionV1;
    let identity = vec![0u8; BLOOM_UNION_WIRE_SIZE];

    // Hash element to Bloom filter: set bit `element % 256` in a 32-byte filter.
    let element_to_bloom = |element: i64| -> Vec<u8> {
        let mut bloom = vec![0u8; BLOOM_UNION_WIRE_SIZE];
        let bit = (element.unsigned_abs() as usize) % (BLOOM_UNION_WIRE_SIZE * 8);
        bloom[bit / 8] |= 1u8 << (bit % 8);
        bloom
    };

    // Law path.
    let mut law_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let bloom = element_to_bloom(*value);
        let state = law_state.entry(*key).or_insert_with(|| identity.clone());
        *state = law.merge(state, &bloom).expect("BloomUnion merge");
    }

    // Batch path: bitwise OR.
    let mut batch_state: HashMap<i64, Vec<u8>> = HashMap::new();
    for (key, value) in rows {
        let bloom = element_to_bloom(*value);
        let state = batch_state
            .entry(*key)
            .or_insert_with(|| vec![0u8; BLOOM_UNION_WIRE_SIZE]);
        for i in 0..BLOOM_UNION_WIRE_SIZE {
            state[i] |= bloom[i];
        }
    }

    // Remove all-zero entries.
    law_state.retain(|_, v| v.iter().any(|b| *b != 0));
    batch_state.retain(|_, v| v.iter().any(|b| *b != 0));

    if law_state == batch_state {
        LawEquivResult::Equivalent
    } else {
        LawEquivResult::Diverged(
            "BloomUnion law path differs from batch path (bit-level divergence)".to_string(),
        )
    }
}

// ─── EquivalenceCorpus ────────────────────────────────────────────────────────

/// The full law-equivalence corpus: runs all 6 registered laws and collects
/// any divergences.
pub struct EquivalenceCorpus {
    /// Number of laws checked.
    pub laws_checked: usize,
    /// Law names that diverged.
    pub diverged: Vec<String>,
}

impl EquivalenceCorpus {
    /// Run the full equivalence corpus on the provided test data.
    ///
    /// `signed_rows`: `(key, value, weight)` tuples for abelian-group laws.
    /// `positive_rows`: `(key, value)` tuples (weight=+1) for semilattice laws.
    pub fn run(signed_rows: &[(i64, i64, i64)], positive_rows: &[(i64, i64)]) -> EquivalenceCorpus {
        let mut diverged = Vec::new();

        // WeightAdd/v1 (abelian group)
        match check_weight_add_equivalence(signed_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("WeightAdd: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        // SumCount/v1 (abelian group)
        match check_sum_count_equivalence(signed_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("SumCount: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        // MaxRegister/v1 (semilattice)
        match check_max_register_equivalence(positive_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("MaxRegister: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        // MinRegister/v1 (semilattice)
        match check_min_register_equivalence(positive_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("MinRegister: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        // HyperLogLog/v1 (semilattice)
        match check_hll_equivalence(positive_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("HyperLogLog: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        // BloomUnion/v1 (semilattice)
        match check_bloom_union_equivalence(positive_rows) {
            LawEquivResult::Diverged(msg) => diverged.push(format!("BloomUnion: {msg}")),
            LawEquivResult::Equivalent => {}
        }

        EquivalenceCorpus {
            laws_checked: 6,
            diverged,
        }
    }

    /// Assert that no laws diverged.
    pub fn assert_all_equivalent(&self) {
        assert!(
            self.diverged.is_empty(),
            "Law-equivalence corpus: {} of {} laws diverged:\n{}",
            self.diverged.len(),
            self.laws_checked,
            self.diverged.join("\n")
        );
    }
}

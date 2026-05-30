//! Lateral/SRF and approximate-aggregate reference oracle (v0.25).
//!
//! Provides batch reference implementations for:
//!
//! 1. **Lateral/SRF evaluation**: `UNNEST`, `GENERATE_SERIES`, and
//!    `JSON_EXTRACT_ARRAY`-style functions.  The oracle expands each input
//!    row independently into zero or more output rows, matching the delta
//!    maintenance semantics of `PlanNode::Lateral`.
//!
//! 2. **Approximate aggregate evaluation**: `APPROX_COUNT_DISTINCT` via
//!    `HyperLogLog/v1` and `APPROX_MEMBERSHIP` via `BloomUnion/v1`.  The
//!    oracle runs the same sketch merge operations as the law bundles to
//!    verify sketch-union law properties.
//!
//! 3. **UDAF requirements documentation**: `UdafRequirements` captures the
//!    algebraic requirements that any user-defined aggregate function must
//!    satisfy, per the v0.25 scope.

use rockstream_plan::LateralFunc;
use rockstream_types::batch::{ZSet, ZSetRow};
use rockstream_types::laws::bloom_union::{
    bloom_check, bloom_insert, BloomUnionV1, BLOOM_UNION_WIRE_SIZE,
};
use rockstream_types::laws::hyper_log_log::{HyperLogLogV1, HLL_WIRE_SIZE};
use rockstream_types::merge_law::LawBundle;

// ─── Lateral / SRF evaluation ────────────────────────────────────────────────

/// Batch reference oracle for `PlanNode::Lateral`.
///
/// Evaluates a set-returning function over a `ZSet`, expanding each row
/// into zero or more output rows.  The output row's weight is inherited from
/// the input row (positive weight = insertion, negative weight = retraction).
///
/// This matches the IVM delta maintenance property: a retracted input row
/// retracts exactly the rows it produced.
pub struct LateralOracle;

impl LateralOracle {
    /// Expand all rows in `input` using the given `LateralFunc`.
    ///
    /// Returns the expanded output `ZSet`.  For each input row:
    /// - The SRF is applied to the row's `value` bytes.
    /// - Each output element is emitted as a new row with the same weight.
    pub fn eval(func: &LateralFunc, input: &ZSet) -> ZSet {
        let mut output = ZSet::default();
        for row in input.iter() {
            let expanded = Self::apply_srf(func, &row.key, &row.value, row.weight);
            for out_row in expanded {
                output.insert(out_row.key, out_row.value, out_row.weight);
            }
        }
        output
    }

    /// Apply a single SRF to one input row.
    ///
    /// Returns the list of `ZSetRow` instances produced by the SRF for this
    /// input row.  Each output row has the same key as the input row (the SRF
    /// produces new value columns) and the same weight.
    fn apply_srf<'a>(
        func: &LateralFunc,
        key: &'a [u8],
        value: &'a [u8],
        weight: i64,
    ) -> Vec<OwnedRow> {
        match func {
            LateralFunc::Unnest { col: _ } => {
                // Decode the value as a length-prefixed list of elements:
                // [u8: count] followed by count × [u8: len, bytes...] entries.
                Self::unnest_bytes(key, value, weight)
            }
            LateralFunc::GenerateSeries { start, stop, step } => {
                Self::generate_series(key, *start, *stop, *step, weight)
            }
            LateralFunc::JsonExtractArray { col: _ } => {
                // Decode the value as a simple ASCII JSON array of integers,
                // e.g., b"[1,2,3]".  Splits on commas, strips brackets.
                Self::json_extract_array(key, value, weight)
            }
        }
    }

    /// Unnest a length-prefixed byte sequence.
    ///
    /// Wire format: `[u8: element_count] [u8: elem_len] [elem_bytes] ...`
    fn unnest_bytes(key: &[u8], value: &[u8], weight: i64) -> Vec<OwnedRow> {
        if value.is_empty() {
            return vec![];
        }
        let count = value[0] as usize;
        let mut pos = 1;
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            if pos >= value.len() {
                break;
            }
            let elem_len = value[pos] as usize;
            pos += 1;
            let end = pos + elem_len;
            if end > value.len() {
                break;
            }
            let elem = &value[pos..end];
            pos = end;
            rows.push(OwnedRow {
                key: key.to_vec(),
                value: elem.to_vec(),
                weight,
            });
        }
        rows
    }

    /// Generate an arithmetic series of i64 values.
    ///
    /// Each value in `start..=stop` (step `step`) is emitted as a row whose
    /// value is the i64 in big-endian 8-byte encoding.
    fn generate_series(key: &[u8], start: i64, stop: i64, step: i64, weight: i64) -> Vec<OwnedRow> {
        if step == 0 {
            return vec![];
        }
        let mut rows = Vec::new();
        let mut v = start;
        loop {
            if step > 0 && v > stop {
                break;
            }
            if step < 0 && v < stop {
                break;
            }
            rows.push(OwnedRow {
                key: key.to_vec(),
                value: v.to_be_bytes().to_vec(),
                weight,
            });
            v = match v.checked_add(step) {
                Some(next) => next,
                None => break,
            };
        }
        rows
    }

    /// Parse a simple ASCII JSON integer array like `[1,2,3]`.
    ///
    /// Each element is emitted as a row whose value is the element's decimal
    /// ASCII bytes, e.g., `b"1"`, `b"2"`, `b"3"`.
    fn json_extract_array(key: &[u8], value: &[u8], weight: i64) -> Vec<OwnedRow> {
        let s = match std::str::from_utf8(value) {
            Ok(s) => s.trim(),
            Err(_) => return vec![],
        };
        // Strip outer brackets.
        let inner = if s.starts_with('[') && s.ends_with(']') {
            &s[1..s.len() - 1]
        } else {
            return vec![];
        };
        if inner.trim().is_empty() {
            return vec![];
        }
        inner
            .split(',')
            .map(|elem| OwnedRow {
                key: key.to_vec(),
                value: elem.trim().as_bytes().to_vec(),
                weight,
            })
            .collect()
    }
}

/// Owned row produced by SRF expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub weight: i64,
}

// ─── Approximate aggregate oracle ────────────────────────────────────────────

/// Reference oracle for `APPROX_COUNT_DISTINCT` using `HyperLogLog/v1`.
///
/// Accumulates input values into an HLL sketch by treating each `value`
/// byte sequence as a hash sample: the first byte (or 0 if empty) is used
/// as the register index and the second byte (or 0) as the register value.
pub struct ApproxCountDistinctOracle {
    sketch: [u8; HLL_WIRE_SIZE],
}

impl ApproxCountDistinctOracle {
    /// Create a new empty sketch (all registers zero).
    pub fn new() -> Self {
        Self {
            sketch: [0u8; HLL_WIRE_SIZE],
        }
    }

    /// Insert a value into the sketch.
    ///
    /// Uses a simple deterministic hash: `register_idx = value[0] % 64`,
    /// `register_val = (value[1] % 32) + 1` (at least 1 to mark presence).
    /// This is a toy hash for proof tests; production would use a proper hash.
    pub fn insert(&mut self, value: &[u8]) {
        let b0 = value.first().copied().unwrap_or(0);
        let b1 = value.get(1).copied().unwrap_or(0);
        let reg_idx = (b0 as usize) % HLL_WIRE_SIZE;
        let reg_val = (b1 % 32) + 1;
        // Register holds the maximum observed value (simulating leading-zero count).
        if reg_val > self.sketch[reg_idx] {
            self.sketch[reg_idx] = reg_val;
        }
    }

    /// Merge another sketch into this one (register-wise max).
    pub fn merge_sketch(&mut self, other: &[u8; HLL_WIRE_SIZE]) {
        for (a, &b) in self.sketch.iter_mut().zip(other.iter()) {
            *a = (*a).max(b);
        }
    }

    /// Return the current sketch bytes.
    pub fn sketch_bytes(&self) -> &[u8; HLL_WIRE_SIZE] {
        &self.sketch
    }

    /// Verify that the law `merge` operation produces the same result as
    /// the oracle's register-wise max.
    ///
    /// Returns `true` if the law merge matches the oracle merge (correctness
    /// property for the sketch-union law test).
    pub fn verify_law_merge(left: &[u8], right: &[u8]) -> bool {
        let law = HyperLogLogV1;
        let merged = match law.merge(left, right) {
            Ok(v) => v,
            Err(_) => return false,
        };
        // Oracle: register-wise max.
        let expected: Vec<u8> = left
            .iter()
            .zip(right.iter())
            .map(|(&l, &r)| l.max(r))
            .collect();
        merged == expected
    }
}

impl Default for ApproxCountDistinctOracle {
    fn default() -> Self {
        Self::new()
    }
}

/// Reference oracle for `APPROX_MEMBERSHIP` using `BloomUnion/v1`.
///
/// Accumulates input values into a Bloom filter sketch by hashing each
/// value's first byte into the 256-bit filter.
pub struct ApproxMembershipOracle {
    filter: [u8; BLOOM_UNION_WIRE_SIZE],
}

impl ApproxMembershipOracle {
    /// Create a new empty Bloom filter (all bits zero).
    pub fn new() -> Self {
        Self {
            filter: [0u8; BLOOM_UNION_WIRE_SIZE],
        }
    }

    /// Insert a value into the Bloom filter.
    ///
    /// Uses the first byte of the value as the hash key.
    pub fn insert(&mut self, value: &[u8]) {
        let hash_byte = value.first().copied().unwrap_or(0);
        bloom_insert(&mut self.filter, hash_byte);
    }

    /// Check whether a value may be present in the filter.
    ///
    /// Returns `true` if the value may be present (with bounded false-positive
    /// rate), `false` if definitely absent.
    pub fn check(&self, value: &[u8]) -> bool {
        let hash_byte = value.first().copied().unwrap_or(0);
        bloom_check(&self.filter, hash_byte)
    }

    /// Merge another filter into this one (bitwise OR).
    pub fn merge_filter(&mut self, other: &[u8; BLOOM_UNION_WIRE_SIZE]) {
        for (a, &b) in self.filter.iter_mut().zip(other.iter()) {
            *a |= b;
        }
    }

    /// Return the current filter bytes.
    pub fn filter_bytes(&self) -> &[u8; BLOOM_UNION_WIRE_SIZE] {
        &self.filter
    }

    /// Verify that the law `merge` operation produces the same result as
    /// the oracle's bitwise OR.
    pub fn verify_law_merge(left: &[u8], right: &[u8]) -> bool {
        let law = BloomUnionV1;
        let merged = match law.merge(left, right) {
            Ok(v) => v,
            Err(_) => return false,
        };
        // Oracle: bitwise OR.
        let expected: Vec<u8> = left
            .iter()
            .zip(right.iter())
            .map(|(&l, &r)| l | r)
            .collect();
        merged == expected
    }

    /// Verify no false negatives: every inserted element must be found.
    pub fn verify_no_false_negatives(inserted: &[u8]) -> bool {
        let mut oracle = Self::new();
        for &b in inserted {
            oracle.insert(&[b]);
        }
        for &b in inserted {
            if !oracle.check(&[b]) {
                return false;
            }
        }
        true
    }
}

impl Default for ApproxMembershipOracle {
    fn default() -> Self {
        Self::new()
    }
}

// ─── UDAF requirements documentation ─────────────────────────────────────────

/// Algebraic requirements for a user-defined aggregate function.
///
/// This is the v0.25 "UDAF requirements documented before implementation"
/// deliverable.  It is not yet wired to runtime dispatch; see `UdafSpec` in
/// `rockstream_plan` for the IR-level counterpart.
///
/// A UDAF is eligible for a `MergeLaw` annotation iff all of the following
/// hold:
///
/// 1. **Associativity**: `merge(merge(a, b), c) == merge(a, merge(b, c))`.
/// 2. **Commutativity**: `merge(a, b) == merge(b, a)`.
/// 3. **Identity**: There exists an identity element `e` s.t.
///    `merge(e, a) == a` for all `a`.
///
/// If additionally:
/// 4. **Invertibility**: There exists `inv(a)` s.t. `merge(a, inv(a)) == e`,
///    the UDAF maps to an `AbelianGroup` law (like `WeightAdd/v1`).
///
/// Without invertibility, the UDAF maps to a `Semilattice` or
/// `CommutativeMonoid` law and must carry `ExtremumRequiresRmw` if
/// retraction-aware correctness requires a state rescan.
///
/// Without any of the above, the UDAF is annotated with
/// `UnknownUdafProperties` in `EXPLAIN INCREMENTAL`.
///
/// # Merge law annotation slot
///
/// When a UDAF satisfies the above requirements and is registered with a
/// `MergeLawId`, the planner attaches that law ID to every `Aggregate` node
/// using this UDAF.  This is the "annotation slot" referenced in the v0.25
/// roadmap scope.
///
/// Full registration DDL (`CREATE MERGE LAW`) is planned for v0.51+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdafRequirements {
    /// Name of the UDAF.
    pub name: String,
    /// True if `merge(merge(a,b),c) == merge(a,merge(b,c))`.
    pub associative: bool,
    /// True if `merge(a,b) == merge(b,a)`.
    pub commutative: bool,
    /// True if there exists an identity element `e` s.t. `merge(e,a) == a`.
    pub has_identity: bool,
    /// True if there exists an inverse `inv(a)` s.t. `merge(a,inv(a)) == e`.
    pub has_inverse: bool,
    /// True if `merge(a,a) == a`.
    pub idempotent: bool,
    /// Notes on retraction correctness (e.g., "requires full state rescan").
    pub retraction_note: String,
}

impl UdafRequirements {
    /// Return `true` if this UDAF qualifies for a `CommutativeMonoid` law.
    pub fn is_commutative_monoid(&self) -> bool {
        self.associative && self.commutative && self.has_identity
    }

    /// Return `true` if this UDAF qualifies for an `AbelianGroup` law.
    pub fn is_abelian_group(&self) -> bool {
        self.is_commutative_monoid() && self.has_inverse
    }

    /// Return `true` if this UDAF qualifies for a `Semilattice` law.
    pub fn is_semilattice(&self) -> bool {
        self.is_commutative_monoid() && self.idempotent
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build a `ZSet` from a list of `(key, value, weight)` tuples.
pub fn zset_from_rows(rows: &[(&[u8], &[u8], i64)]) -> ZSet {
    let mut z = ZSet::default();
    for &(k, v, w) in rows {
        z.insert(k.to_vec(), v.to_vec(), w);
    }
    z
}

/// Count the total number of (weight-1) rows in a `ZSet`.
pub fn count_rows(z: &ZSet) -> i64 {
    z.iter().map(|r| r.weight).sum()
}

/// Get all values in a `ZSet` (weight-1 rows only, in iteration order).
pub fn collect_values(z: &ZSet) -> Vec<Vec<u8>> {
    let mut values: Vec<Vec<u8>> = z
        .iter()
        .filter(|r| r.weight > 0)
        .map(|r: ZSetRow| r.value.to_vec())
        .collect();
    values.sort();
    values
}

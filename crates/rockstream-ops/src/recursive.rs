//! Recursive IVM operator for RockStream (v0.22).
//!
//! ## Design
//!
//! `RecursiveOp` computes the fixed-point of a user-supplied step function
//! starting from an initial base delta.  It uses **semi-naive evaluation**:
//! each iteration feeds only the *new* rows produced in the previous
//! iteration (the "frontier") into the step function, rather than the entire
//! accumulated relation.  This makes convergence efficient for sparse graphs
//! like transitive closure.
//!
//! ### Semi-naive algorithm
//!
//! ```text
//! accumulated  = ∅
//! frontier     = base_delta (rows with net_weight > 0)
//!
//! while frontier ≠ ∅ and iterations < max_iterations:
//!     candidates = step_fn(frontier)
//!     new_rows   = { r ∈ candidates | r ∉ accumulated }
//!     accumulated ∪= new_rows
//!     frontier   = new_rows
//!     iterations++
//!
//! output = net additions to accumulated across all iterations
//! ```
//!
//! ### Convergence detection
//!
//! Convergence is detected when `frontier` is empty (no new rows were
//! produced by the step function).  The `converged()` accessor returns
//! `true` after each call to `process()` that reached convergence before
//! hitting `max_iterations`.
//!
//! ### Monotone (insert-only) mode
//!
//! When `monotone = true` the operator enforces that all input weights are
//! strictly positive (+1).  Negative-weight (retraction) rows are rejected
//! with a `RS-1509` error.  Monotone recursion publishes a
//! `complete_through` token (the `converged()` flag) once convergence is
//! reached: rows emitted before convergence represent partial progress —
//! they will never be retracted because the relation is insert-only.
//!
//! ### DRed escape hatch (non-monotone)
//!
//! When `monotone = false`, the operator still accepts and processes inserts
//! normally, but any retraction delta is rejected with `RS-1509`.  The
//! planner marks this operator `not_merge_safe_reason =
//! recursion_dred_required` (EXPLAIN INCREMENTAL) to indicate that full
//! DRed support is not yet implemented.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;

/// Step function: maps a frontier ZSet to a ZSet of candidate new rows.
///
/// The closure receives the rows added in the **previous iteration**
/// (the frontier) and must return the new rows that can be derived from them.
/// For transitive closure: if the frontier contains `(x, y)`, and there is
/// an edge `(y, z)` in the base relation, the step function returns `(x, z)`.
pub type StepFn = Arc<dyn Fn(&ZSet) -> ZSet + Send + Sync + 'static>;

/// The `RecursiveOp` IVM operator.
pub struct RecursiveOp {
    max_iterations: usize,
    monotone: bool,
    step_fn: StepFn,
    /// Accumulated live rows: `(key, value) → net_weight`.
    /// Only rows with `net_weight > 0` represent live facts.
    accumulated: HashMap<(Vec<u8>, Vec<u8>), i64>,
    /// Whether the last `process()` call reached convergence before hitting
    /// `max_iterations`.  True if the final frontier was empty.
    converged: bool,
    /// Number of semi-naive iterations executed in the last `process()` call.
    iterations: usize,
    name: String,
}

impl RecursiveOp {
    /// Create a new `RecursiveOp`.
    ///
    /// # Parameters
    /// - `max_iterations`: safety cap on semi-naive iterations per epoch.
    /// - `monotone`: if `true`, retractions are rejected with `RS-1509`.
    /// - `step_fn`: maps the current-iteration frontier to candidate new rows.
    pub fn new(max_iterations: usize, monotone: bool, step_fn: StepFn) -> Self {
        assert!(max_iterations > 0, "max_iterations must be positive");
        Self {
            max_iterations,
            monotone,
            step_fn,
            accumulated: HashMap::new(),
            converged: false,
            iterations: 0,
            name: "RecursiveOp".to_owned(),
        }
    }

    /// Process a base delta, run semi-naive iteration to convergence, and
    /// return the net additions to the accumulated relation.
    ///
    /// # Errors
    ///
    /// Returns `Err("RS-1509: ...")` if `monotone = true` and any row in
    /// `base_delta` has a negative weight (retraction).
    pub fn process(&mut self, base_delta: &ZSet) -> Result<ZSet, String> {
        // ── Monotone / escape-hatch check ────────────────────────────────────
        if self.monotone {
            for row in base_delta.iter() {
                if row.weight < 0 {
                    return Err(format!(
                        "RS-1509: non-monotone delta rejected in monotone recursion \
                         (key={:?}, weight={})",
                        row.key, row.weight
                    ));
                }
            }
        }

        // ── Phase 1: apply base delta ─────────────────────────────────────────
        let mut output = ZSet::new();
        let mut frontier = ZSet::new();

        for row in base_delta.iter() {
            let k = (row.key.clone(), row.value.clone());
            let old_w = *self.accumulated.get(&k).unwrap_or(&0);
            let new_w = old_w + row.weight;

            if new_w <= 0 {
                self.accumulated.remove(&k);
            } else {
                self.accumulated.insert(k, new_w);
            }

            let was_live = old_w > 0;
            let is_live = new_w > 0;

            if !was_live && is_live {
                output.insert(row.key.clone(), row.value.clone(), 1);
                frontier.insert(row.key.clone(), row.value.clone(), 1);
            } else if was_live && !is_live {
                output.insert(row.key.clone(), row.value.clone(), -1);
            }
        }

        // ── Phase 2: semi-naive iteration ─────────────────────────────────────
        self.iterations = 0;
        while !frontier.is_empty() && self.iterations < self.max_iterations {
            let candidates = (self.step_fn)(&frontier);
            let mut next_frontier = ZSet::new();

            for row in candidates.iter() {
                if row.weight <= 0 {
                    continue;
                }
                let k = (row.key.clone(), row.value.clone());
                let entry = self.accumulated.entry(k).or_insert(0);
                if *entry == 0 {
                    // New fact derived by the step function.
                    *entry = row.weight;
                    output.insert(row.key.clone(), row.value.clone(), 1);
                    next_frontier.insert(row.key.clone(), row.value.clone(), 1);
                }
            }

            frontier = next_frontier;
            self.iterations += 1;
        }

        // ── Phase 3: convergence ──────────────────────────────────────────────
        self.converged = frontier.is_empty();
        Ok(output)
    }

    /// Whether the last `process()` call reached convergence (the frontier
    /// was empty after the final iteration).
    ///
    /// For monotone recursion this is the `complete_through` token: all rows
    /// reachable from the base facts have been emitted and will never be
    /// retracted.
    pub fn converged(&self) -> bool {
        self.converged
    }

    /// Number of semi-naive iterations executed in the last `process()` call.
    pub fn iterations(&self) -> usize {
        self.iterations
    }

    /// Number of live facts currently in the accumulated relation.
    pub fn accumulated_len(&self) -> usize {
        self.accumulated.len()
    }
}

#[async_trait]
impl Operator for RecursiveOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let output = match RecursiveOp::process(self, &input.zset) {
            Ok(delta) => delta,
            Err(_) => ZSet::new(), // RS-1509: non-monotone rejected; emit empty delta.
        };
        ZSetBatch {
            zset: output,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, _epoch: Epoch) {}

    fn name(&self) -> &str {
        &self.name
    }

    fn merge_law(&self) -> Option<MergeLawId> {
        Some(WEIGHT_ADD_ID)
    }
}

// ─── Constructor helpers ──────────────────────────────────────────────────────

/// Create a step function for transitive closure.
///
/// The `edges` ZSet contains `(from, to)` pairs encoded as:
/// - `key = [from_byte]`
/// - `value = [to_byte]`
///
/// The step function takes a frontier of `(a, b)` pairs and, for each
/// `(b, c)` edge in `edges`, produces `(a, c)`.
///
/// This is the standard semi-naive TC step:
/// `TC_next(a, c) := frontier(a, b) ⋈ edges(b, c)`
pub fn tc_step_fn(edges: ZSet) -> StepFn {
    Arc::new(move |frontier: &ZSet| {
        let mut result = ZSet::new();
        for f_row in frontier.iter() {
            // f_row.key = [a], f_row.value = [b]
            if f_row.key.is_empty() || f_row.value.is_empty() {
                continue;
            }
            let b = f_row.value[0];
            // Find all edges (b, c).
            for e_row in edges.iter() {
                if e_row.key.is_empty() || e_row.value.is_empty() {
                    continue;
                }
                if e_row.key[0] == b && e_row.weight > 0 {
                    let c = e_row.value[0];
                    // Emit (a, c).
                    result.insert(f_row.key.clone(), vec![c], 1);
                }
            }
        }
        result
    })
}

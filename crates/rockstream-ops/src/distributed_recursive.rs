//! Distributed (sharded) recursive IVM operator for RockStream (v0.33).
//!
//! ## Design
//!
//! `DistributedRecursiveOp` extends `RecursiveOp` (v0.22) to support multi-shard
//! execution with an `Exchange` step inside the recursive scope. Rows are
//! partitioned across virtual shards by key hash; after each semi-naive iteration
//! the step-function output is exchanged back to the owning shards before the
//! next iteration begins. This models the distributed execution described in
//! IVM.md §11.1.
//!
//! ### Distributed semi-naive algorithm
//!
//! ```text
//! // Initialise
//! for shard s in 0..num_shards:
//!     accumulated[s] = ∅
//!     local_frontier[s] = partition(base_delta, s)
//!
//! // Phase 1: absorb base delta
//! for s in 0..num_shards:
//!     for r in local_frontier[s] where r.weight > 0:
//!         if r ∉ accumulated[s]:
//!             accumulated[s] += r
//!             output += r
//!
//! // Phase 2: iterative exchange
//! while any frontier non-empty and iterations < max_iterations:
//!     // Local step on each shard's frontier
//!     exchange_inbox[0..num_shards] = ∅
//!     for s in 0..num_shards:
//!         candidates = step_fn(local_frontier[s])
//!         // Exchange: route each candidate to its owning shard
//!         for r in candidates where r.weight > 0:
//!             target = fnv(r.key) % num_shards
//!             exchange_inbox[target] += r
//!
//!     // Deliver inbox and build next frontier
//!     for s in 0..num_shards:
//!         next_frontier[s] = ∅
//!         for r in exchange_inbox[s]:
//!             if r ∉ accumulated[s]:
//!                 accumulated[s] += r
//!                 output += r
//!                 next_frontier[s] += r
//!
//!     // Stall detection (per-shard)
//!     for s in 0..num_shards:
//!         if local_frontier[s] non-empty AND next_frontier[s] empty:
//!             stall_count[s]++
//!             if stall_count[s] >= stall_timeout_iterations:
//!                 // Per-shard recompute fallback
//!                 recomputed = step_fn(zset_from(accumulated[s]))
//!                 for r in recomputed where r ∉ accumulated[s]:
//!                     accumulated[s] += r
//!                     output += r
//!                     next_frontier[s] += r
//!                 recompute_count++
//!                 if next_frontier[s] still empty:
//!                     return Err("RS-1512: inner-frontier stall …")
//!                 stall_count[s] = 0
//!         else:
//!             stall_count[s] = 0
//!
//!     local_frontier = next_frontier
//!     iterations++
//!
//! converged = all frontiers empty
//! ```
//!
//! ### Inner-frontier convergence
//!
//! The inner frontier is the union of all per-shard frontiers. It advances to
//! empty (convergence) when no shard produces new rows in an iteration. This
//! participates in the cluster-level antichain aggregation described in
//! IVM.md §11.1 via the `inner_frontier()` accessor.
//!
//! ### Per-shard recompute fallback
//!
//! When a shard's frontier is non-empty but its step function produces no new
//! rows after `stall_timeout_iterations` consecutive iterations, the operator
//! switches that shard to full-recompute mode for one iteration: the step
//! function is applied to the entire accumulated state (not just the frontier).
//! If the recompute also produces no new rows, `RS-1512` is returned.
//!
//! ### Skew handling
//!
//! Uneven input distributions (skewed keys) cause some shards to receive many
//! more rows than others. The exchange loop naturally handles this: shards with
//! heavy input still converge at their own pace; lighter shards finish early and
//! have empty frontiers while heavy shards keep iterating. The global convergence
//! condition requires ALL shards to report empty frontiers.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::frontier::Antichain;
use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;

use crate::operator::Operator;
use crate::recursive::StepFn;

/// Per-shard accumulated state: maps (key, value) pairs to their net weight.
type ShardAccumulated = HashMap<(Vec<u8>, Vec<u8>), i64>;

// ─── Partition helper ─────────────────────────────────────────────────────────

/// Compute the target shard for a row key using FNV-1a hashing.
///
/// FNV-1a is fast, deterministic, and produces good distribution for
/// short byte-slice keys like node IDs.
fn shard_for_key(key: &[u8], num_shards: usize) -> usize {
    const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;
    let hash = key.iter().fold(FNV_OFFSET, |acc, &b| {
        acc.wrapping_mul(FNV_PRIME) ^ (b as u64)
    });
    (hash % num_shards as u64) as usize
}

/// Build a `ZSet` from the live rows in an accumulated HashMap.
fn zset_from_accumulated(acc: &ShardAccumulated) -> ZSet {
    let mut z = ZSet::new();
    for ((key, value), &weight) in acc {
        if weight > 0 {
            z.insert(key.clone(), value.clone(), weight);
        }
    }
    z
}

// ─── DistributedRecursiveOp ───────────────────────────────────────────────────

/// Distributed (sharded) recursive IVM operator (v0.33).
///
/// Models multi-shard execution with an Exchange step inside the recursive
/// scope. The `num_shards` parameter controls the number of virtual shards;
/// rows are assigned to shards by `fnv(key) % num_shards`.
pub struct DistributedRecursiveOp {
    num_shards: usize,
    max_iterations: usize,
    /// Number of consecutive stall iterations before triggering per-shard
    /// recompute fallback.
    stall_timeout_iterations: usize,
    monotone: bool,
    step_fn: StepFn,
    /// Per-shard accumulated state: (key, value) → net_weight.
    shard_accumulated: Vec<ShardAccumulated>,
    /// Whether the last `process()` call converged.
    converged: bool,
    /// Semi-naive iterations executed in the last `process()` call.
    iterations: usize,
    /// Number of per-shard recomputes triggered in the last `process()` call.
    recompute_count: usize,
    /// Name for the `Operator` trait.
    name: String,
}

impl DistributedRecursiveOp {
    /// Create a new `DistributedRecursiveOp`.
    ///
    /// # Parameters
    /// - `num_shards`: number of virtual shards (≥ 1).
    /// - `max_iterations`: per-epoch semi-naive iteration cap.
    /// - `stall_timeout_iterations`: consecutive stall iterations before
    ///   triggering per-shard recompute.  Use `usize::MAX` to disable.
    /// - `monotone`: if `true`, retractions are rejected with `RS-1509`.
    /// - `step_fn`: maps the current-iteration frontier to candidate new rows.
    pub fn new(
        num_shards: usize,
        max_iterations: usize,
        stall_timeout_iterations: usize,
        monotone: bool,
        step_fn: StepFn,
    ) -> Self {
        assert!(num_shards >= 1, "num_shards must be ≥ 1");
        assert!(max_iterations > 0, "max_iterations must be positive");
        let shard_accumulated = (0..num_shards).map(|_| HashMap::new()).collect();
        Self {
            num_shards,
            max_iterations,
            stall_timeout_iterations,
            monotone,
            step_fn,
            shard_accumulated,
            converged: false,
            iterations: 0,
            recompute_count: 0,
            name: "DistributedRecursiveOp".to_owned(),
        }
    }

    /// Process a base delta, run distributed semi-naive iteration to
    /// convergence, and return the net additions to the accumulated relation.
    ///
    /// # Errors
    ///
    /// - `"RS-1509: …"` if `monotone = true` and any row has negative weight.
    /// - `"RS-1512: …"` if a shard's inner frontier stalls beyond
    ///   `stall_timeout_iterations` and recompute fails to make progress.
    /// - `"RS-1513: …"` if `max_iterations` is reached without convergence.
    pub fn process(&mut self, base_delta: &ZSet) -> Result<ZSet, String> {
        // ── Monotone check ────────────────────────────────────────────────────
        if self.monotone {
            for row in base_delta.iter() {
                if row.weight < 0 {
                    return Err(format!(
                        "RS-1509: non-monotone delta rejected in monotone distributed \
                         recursion (key={:?}, weight={})",
                        row.key, row.weight
                    ));
                }
            }
        }

        let mut output = ZSet::new();

        // ── Phase 1: partition base delta and absorb into shards ──────────────
        let mut per_shard_frontier: Vec<ZSet> = (0..self.num_shards).map(|_| ZSet::new()).collect();

        for row in base_delta.iter() {
            if row.weight <= 0 {
                continue;
            }
            let target = shard_for_key(&row.key, self.num_shards);
            let k = (row.key.clone(), row.value.clone());
            let entry = self.shard_accumulated[target].entry(k).or_insert(0);
            if *entry == 0 {
                *entry = row.weight;
                output.insert(row.key.clone(), row.value.clone(), 1);
                per_shard_frontier[target].insert(row.key.clone(), row.value.clone(), 1);
            }
        }

        // ── Phase 2: distributed semi-naive iteration ─────────────────────────
        let mut stall_counts = vec![0usize; self.num_shards];
        self.iterations = 0;
        self.recompute_count = 0;

        while per_shard_frontier.iter().any(|f| !f.is_empty())
            && self.iterations < self.max_iterations
        {
            // Local step on each shard's frontier + exchange routing.
            let mut exchange_inbox: Vec<ZSet> = (0..self.num_shards).map(|_| ZSet::new()).collect();

            for shard_frontier in &per_shard_frontier {
                let candidates = (self.step_fn)(shard_frontier);
                for row in candidates.iter() {
                    if row.weight <= 0 {
                        continue;
                    }
                    let target = shard_for_key(&row.key, self.num_shards);
                    exchange_inbox[target].insert(row.key.clone(), row.value.clone(), 1);
                }
            }

            // Deliver inbox → build next frontiers.
            let mut next_frontiers: Vec<ZSet> = (0..self.num_shards).map(|_| ZSet::new()).collect();
            for shard_idx in 0..self.num_shards {
                for row in exchange_inbox[shard_idx].iter() {
                    if row.weight <= 0 {
                        continue;
                    }
                    let k = (row.key.clone(), row.value.clone());
                    let entry = self.shard_accumulated[shard_idx].entry(k).or_insert(0);
                    if *entry == 0 {
                        *entry = 1;
                        output.insert(row.key.clone(), row.value.clone(), 1);
                        next_frontiers[shard_idx].insert(row.key.clone(), row.value.clone(), 1);
                    }
                }
            }

            // Stall detection + per-shard recompute fallback.
            //
            // A shard's inner frontier is considered stalled when it received
            // rows via the exchange inbox but none of them were new (all already
            // accumulated). This means rows are circulating without making
            // progress — the classic distributed fixed-point stall.
            //
            // Shards that simply ran out of local work (empty inbox, empty next
            // frontier) are NOT stalled — they have legitimately converged their
            // local contribution. Stall is only declared when the INBOX was
            // non-empty but delivered no new rows.
            for shard_idx in 0..self.num_shards {
                if !exchange_inbox[shard_idx].is_empty() && next_frontiers[shard_idx].is_empty() {
                    stall_counts[shard_idx] += 1;
                    if stall_counts[shard_idx] >= self.stall_timeout_iterations {
                        // Per-shard recompute fallback: run step on full accumulated state.
                        let full_state = zset_from_accumulated(&self.shard_accumulated[shard_idx]);
                        let recomputed = (self.step_fn)(&full_state);
                        let mut recompute_added = false;
                        for row in recomputed.iter() {
                            if row.weight <= 0 {
                                continue;
                            }
                            let target = shard_for_key(&row.key, self.num_shards);
                            let k = (row.key.clone(), row.value.clone());
                            let entry = self.shard_accumulated[target].entry(k).or_insert(0);
                            if *entry == 0 {
                                *entry = 1;
                                output.insert(row.key.clone(), row.value.clone(), 1);
                                next_frontiers[target].insert(
                                    row.key.clone(),
                                    row.value.clone(),
                                    1,
                                );
                                recompute_added = true;
                            }
                        }
                        self.recompute_count += 1;
                        if !recompute_added {
                            // Recompute made no progress — surface RS-1512.
                            return Err(format!(
                                "RS-1512: inner-frontier stall in distributed recursion \
                                 (shard={shard_idx}, stall_iterations={}, recompute_count={}). \
                                 The step function makes no progress on this shard's accumulated \
                                 state. Check for cycles, skewed partitioning, or a buggy step \
                                 function.",
                                stall_counts[shard_idx], self.recompute_count
                            ));
                        }
                        stall_counts[shard_idx] = 0;
                    }
                } else {
                    stall_counts[shard_idx] = 0;
                }
            }

            per_shard_frontier = next_frontiers;
            self.iterations += 1;
        }

        // ── Phase 3: convergence / cap check ─────────────────────────────────
        self.converged = per_shard_frontier.iter().all(|f| f.is_empty());
        if !self.converged && self.iterations >= self.max_iterations {
            return Err(format!(
                "RS-1513: distributed recursion max-iteration cap of {} exceeded without \
                 convergence (num_shards={}). The recursive view may not converge for this \
                 input; increase max_iterations or constrain the query.",
                self.max_iterations, self.num_shards
            ));
        }

        Ok(output)
    }

    /// Whether the last `process()` call reached convergence.
    pub fn converged(&self) -> bool {
        self.converged
    }

    /// Number of semi-naive iterations executed in the last `process()` call.
    pub fn iterations(&self) -> usize {
        self.iterations
    }

    /// Number of per-shard recomputes triggered in the last `process()` call.
    pub fn recompute_count(&self) -> usize {
        self.recompute_count
    }

    /// Total live facts accumulated across all shards.
    pub fn total_accumulated_len(&self) -> usize {
        self.shard_accumulated
            .iter()
            .map(|a| a.values().filter(|&&w| w > 0).count())
            .sum()
    }

    /// Return the inner frontier as an antichain of iteration timestamps.
    ///
    /// An empty antichain means all shards have converged (inner frontier
    /// has advanced past the last iteration). A non-empty antichain reports
    /// the current iteration number(s) that are still in progress.
    pub fn inner_frontier(&self, current_iteration: u64) -> Antichain<u64> {
        if self.converged {
            Antichain::empty()
        } else {
            Antichain::from_elem(current_iteration)
        }
    }

    /// Number of virtual shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
}

#[async_trait]
impl Operator for DistributedRecursiveOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }

    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        let output = match DistributedRecursiveOp::process(self, &input.zset) {
            Ok(delta) => delta,
            Err(_) => ZSet::new(),
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

/// Create a sharded TC step function that operates on a distributed edge set.
///
/// Each shard holds the edges whose source node hashes to that shard. The
/// step function takes a frontier of `(a, b)` pairs and, for each `(b, c)`
/// edge in `all_edges`, produces `(a, c)`. The exchange then routes each
/// `(a, c)` to the shard that owns `a`.
///
/// This is the distributed version of `tc_step_fn` from `recursive.rs`.
pub fn distributed_tc_step_fn(all_edges: ZSet) -> StepFn {
    Arc::new(move |frontier: &ZSet| {
        let mut result = ZSet::new();
        for f_row in frontier.iter() {
            if f_row.key.is_empty() || f_row.value.is_empty() {
                continue;
            }
            for e_row in all_edges.iter() {
                if e_row.key.is_empty() || e_row.value.is_empty() {
                    continue;
                }
                if e_row.key == f_row.value && e_row.weight > 0 {
                    result.insert(f_row.key.clone(), e_row.value.clone(), 1);
                }
            }
        }
        result
    })
}

/// Create a distributed TC step function for 4-byte big-endian node IDs.
///
/// Encodes node IDs as 4-byte big-endian `u32` values. This is used for
/// large-scale tests where 1-byte node IDs (max 256 nodes) are insufficient.
pub fn distributed_tc_step_fn_u32(all_edges: ZSet) -> StepFn {
    distributed_tc_step_fn(all_edges)
}

// ─── Encoding helpers for u32 node IDs ────────────────────────────────────────

/// Encode a u32 node ID as a 4-byte big-endian key.
pub fn encode_node(node: u32) -> Vec<u8> {
    node.to_be_bytes().to_vec()
}

/// Decode a 4-byte big-endian key to a u32 node ID.
pub fn decode_node(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

// ─── Edge set builders ─────────────────────────────────────────────────────────

/// Build a ZSet of edges from (from_node, to_node) u32 pairs.
pub fn edges_u32(pairs: &[(u32, u32)]) -> ZSet {
    let mut z = ZSet::new();
    for (from, to) in pairs {
        z.insert(encode_node(*from), encode_node(*to), 1);
    }
    z
}

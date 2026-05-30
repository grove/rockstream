//! DiffCtx differentiation pass for RockStream IVM.
//!
//! Implements the differentiation rules that transform logical `PlanNode`s
//! into physical `OpNode` execution graphs. Each logical node is mapped to
//! one or more physical operators with merge-law annotations attached by
//! the differentiator.

use rockstream_plan::{
    AggregateFunc, NotMergeSafeReason, OpKind, OpNode, PlanNode, WindowFunc, WindowStrategy,
};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::bloom_union::BLOOM_UNION_ID;
use rockstream_types::laws::hyper_log_log::HLL_ID;
use rockstream_types::laws::max_register::MAX_REGISTER_ID;
use rockstream_types::laws::min_register::MIN_REGISTER_ID;
use rockstream_types::laws::sum_count::SUM_COUNT_ID;
use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
use rockstream_types::merge_law::MergeLawId;

/// The differentiation context: transforms a logical plan into a physical
/// operator graph with merge-law annotations.
pub struct DiffCtx {
    next_id: u64,
}

impl DiffCtx {
    /// Create a new differentiation context.
    pub fn new() -> Self {
        Self { next_id: 0 }
    }

    /// Differentiate a logical plan into a physical operator graph.
    ///
    /// Returns a topologically-sorted list of `OpNode`s (sources first,
    /// sinks last).
    pub fn differentiate(&mut self, plan: &PlanNode) -> Vec<OpNode> {
        let mut nodes = Vec::new();
        self.diff_node(plan, &mut nodes);
        nodes
    }

    fn alloc_id(&mut self) -> OperatorId {
        let id = OperatorId(self.next_id);
        self.next_id += 1;
        id
    }

    fn diff_node(&mut self, plan: &PlanNode, nodes: &mut Vec<OpNode>) -> OperatorId {
        match plan {
            PlanNode::Source { name } => {
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Source { name: name.clone() },
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![],
                });
                id
            }
            PlanNode::Filter { input, .. } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Filter,
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Project { input, .. } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Project,
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Map { input, .. } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Map,
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Aggregate {
                input, aggregates, ..
            } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                let (law, reason) = self.law_for_aggregate(aggregates);
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Aggregate,
                    merge_law: law,
                    not_merge_safe_reason: reason,
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Join { left, right, .. } => {
                let left_id = self.diff_node(left, nodes);
                let right_id = self.diff_node(right, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Join,
                    merge_law: Some(WEIGHT_ADD_ID),
                    not_merge_safe_reason: None,
                    inputs: vec![left_id, right_id],
                });
                id
            }
            PlanNode::Union { left, right } => {
                let left_id = self.diff_node(left, nodes);
                let right_id = self.diff_node(right, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Union,
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![left_id, right_id],
                });
                id
            }
            PlanNode::Window {
                input,
                window_exprs,
            } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                let has_sliding = window_exprs.iter().any(|we| {
                    matches!(
                        we.func,
                        WindowFunc::SlidingSum { .. } | WindowFunc::SlidingAvg { .. }
                    )
                });
                let (strategy, law, reason) = if has_sliding {
                    (WindowStrategy::SlidingAggregate, Some(SUM_COUNT_ID), None)
                } else {
                    (
                        WindowStrategy::PartitionRecompute,
                        None,
                        Some(NotMergeSafeReason::PartitionRecomputation),
                    )
                };
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Window { strategy },
                    merge_law: law,
                    not_merge_safe_reason: reason,
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::TumbleWindow {
                input,
                time_col: _,
                window_size_ms,
                late_data_policy,
            } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::TumbleWindow {
                        window_size_ms: *window_size_ms,
                        late_data_policy: late_data_policy.clone(),
                    },
                    // Watermark state uses MaxRegister/v1 (semilattice,
                    // idempotent).
                    merge_law: Some(MAX_REGISTER_ID),
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::TopK {
                input,
                k,
                rank_col,
                partition_by,
            } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::TopK {
                        k: *k,
                        rank_col: *rank_col,
                        partition_by: partition_by.clone(),
                    },
                    // Row weight state uses WeightAdd/v1 (abelian group).
                    merge_law: Some(WEIGHT_ADD_ID),
                    not_merge_safe_reason: None,
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Snapshot {
                source_name,
                batch_size,
            } => {
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Snapshot {
                        source_name: source_name.clone(),
                        batch_size: *batch_size,
                    },
                    // Snapshot is a stateless insert-only source: rows are
                    // emitted as positive-weight Z-set entries with no
                    // arrangement.  WeightAdd/v1 governs the downstream
                    // accumulation of snapshot rows.
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![],
                });
                id
            }
            PlanNode::ViewRef { view_name } => {
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::ViewRef {
                        view_name: view_name.clone(),
                    },
                    // ViewRef is structurally a source at the physical level:
                    // it reads CDC deltas from an upstream materialized view.
                    // No local arrangement; cadence inheritance and frontier
                    // meet are handled by the scheduler.
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![],
                });
                id
            }
            PlanNode::Lateral { input, func } => {
                let input_id = self.diff_node(input, nodes);
                let id = self.alloc_id();
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Lateral { func: func.clone() },
                    // Lateral/SRF is stateless: it maps each input row to
                    // zero or more output rows with no arrangement.  A
                    // retracted input row retracts exactly its output rows.
                    merge_law: None,
                    not_merge_safe_reason: Some(NotMergeSafeReason::Stateless),
                    inputs: vec![input_id],
                });
                id
            }
            PlanNode::Recursion {
                base,
                step,
                max_iterations,
                monotone,
            } => {
                let base_id = self.diff_node(base, nodes);
                let step_id = self.diff_node(step, nodes);
                let id = self.alloc_id();
                // Monotone recursion: WeightAdd/v1 (abelian group, insert-only
                // terms).  complete_through is published once converged.
                // Non-monotone: DRed escape hatch — still WeightAdd/v1 for the
                // arrangement, but flagged with RecursionDredRequired because
                // retractions require read-modify-write and are rejected at
                // runtime with RS-1509.
                let reason = if *monotone {
                    None
                } else {
                    Some(NotMergeSafeReason::RecursionDredRequired)
                };
                nodes.push(OpNode {
                    id,
                    kind: OpKind::Recursion {
                        max_iterations: *max_iterations,
                        monotone: *monotone,
                    },
                    merge_law: Some(WEIGHT_ADD_ID),
                    not_merge_safe_reason: reason,
                    inputs: vec![base_id, step_id],
                });
                id
            }
        }
    }

    /// Determine the merge law for an aggregate node based on its functions.
    ///
    /// For SUM/COUNT/AVG, the `WeightAdd/v1` abelian-group law applies.
    ///
    /// For MIN/MAX, the operator uses an indexed multiset (BTreeMap) for
    /// retraction-aware correctness, but reports the cached-slot law for
    /// `EXPLAIN INCREMENTAL`:
    /// - MAX → `MaxRegister/v1` (semilattice: `merge = max`)
    /// - MIN → `MinRegister/v1` (semilattice: `merge = min`)
    ///
    /// Both extremum variants also carry `ExtremumRequiresRmw` because
    /// `get_merged()` on the storage arrangement alone is insufficient after
    /// retractions — the operator's prefix-scan rescan is required.
    ///
    /// For APPROX_COUNT_DISTINCT, `HyperLogLog/v1` is used (semilattice,
    /// non-invertible).  Retraction-aware correctness requires a full sketch
    /// rescan; `ExtremumRequiresRmw` is reported.
    ///
    /// For APPROX_MEMBERSHIP, `BloomUnion/v1` is used (semilattice,
    /// non-invertible).  Same `ExtremumRequiresRmw` requirement.
    fn law_for_aggregate(
        &self,
        aggregates: &[rockstream_plan::AggregateExpr],
    ) -> (Option<MergeLawId>, Option<NotMergeSafeReason>) {
        let has_max = aggregates
            .iter()
            .any(|a| matches!(a.func, AggregateFunc::Max));
        let has_min = aggregates
            .iter()
            .any(|a| matches!(a.func, AggregateFunc::Min));
        let has_approx_distinct = aggregates
            .iter()
            .any(|a| matches!(a.func, AggregateFunc::ApproxCountDistinct));
        let has_approx_membership = aggregates
            .iter()
            .any(|a| matches!(a.func, AggregateFunc::ApproxMembership));

        if has_max {
            // MAX aggregate: cached slot uses MaxRegister/v1.
            (
                Some(MAX_REGISTER_ID),
                Some(NotMergeSafeReason::ExtremumRequiresRmw),
            )
        } else if has_min {
            // MIN aggregate: cached slot uses MinRegister/v1.
            (
                Some(MIN_REGISTER_ID),
                Some(NotMergeSafeReason::ExtremumRequiresRmw),
            )
        } else if has_approx_distinct {
            // APPROX_COUNT_DISTINCT: HyperLogLog/v1 (semilattice).
            // Non-invertible; retraction requires sketch rescan.
            (Some(HLL_ID), Some(NotMergeSafeReason::ExtremumRequiresRmw))
        } else if has_approx_membership {
            // APPROX_MEMBERSHIP: BloomUnion/v1 (semilattice).
            // Non-invertible; retraction requires full filter rescan.
            (
                Some(BLOOM_UNION_ID),
                Some(NotMergeSafeReason::ExtremumRequiresRmw),
            )
        } else {
            // SUM / COUNT / AVG: fully invertible via WeightAdd/v1.
            (Some(WEIGHT_ADD_ID), None)
        }
    }
}

impl Default for DiffCtx {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr};

    #[test]
    fn diff_source_filter_project() {
        let plan = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "orders".into(),
                }),
                predicate: Expr::Column(0),
            }),
            columns: vec![Expr::Column(0)],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        assert_eq!(nodes.len(), 3);
        assert!(matches!(nodes[0].kind, OpKind::Source { .. }));
        assert!(matches!(nodes[1].kind, OpKind::Filter));
        assert!(matches!(nodes[2].kind, OpKind::Project));
        // All stateless — no merge law
        assert!(nodes.iter().all(|n| n.merge_law.is_none()));
    }

    #[test]
    fn diff_sum_aggregate_gets_weight_add() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "sales".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let agg = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .unwrap();
        assert_eq!(agg.merge_law, Some(WEIGHT_ADD_ID));
        assert!(agg.not_merge_safe_reason.is_none());
    }

    #[test]
    fn diff_min_aggregate_not_merge_safe() {
        use rockstream_types::laws::min_register::MIN_REGISTER_ID;

        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "temps".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Min,
                input: Expr::Column(1),
                distinct: false,
            }],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let agg = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .unwrap();
        // v0.8: MIN uses MinRegister/v1 as the cached-slot law (EXPLAIN INCREMENTAL).
        assert_eq!(agg.merge_law, Some(MIN_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw)
        );
    }

    #[test]
    fn diff_max_aggregate_uses_max_register() {
        use rockstream_types::laws::max_register::MAX_REGISTER_ID;

        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "prices".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Max,
                input: Expr::Column(1),
                distinct: false,
            }],
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let agg = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .unwrap();
        // v0.8: MAX uses MaxRegister/v1 as the cached-slot law (EXPLAIN INCREMENTAL).
        assert_eq!(agg.merge_law, Some(MAX_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw)
        );
    }

    #[test]
    fn diff_join_uses_weight_add() {
        let plan = PlanNode::Join {
            left: Box::new(PlanNode::Source { name: "a".into() }),
            right: Box::new(PlanNode::Source { name: "b".into() }),
            condition: Expr::Column(0),
        };

        let mut ctx = DiffCtx::new();
        let nodes = ctx.differentiate(&plan);
        let join = nodes
            .iter()
            .find(|n| matches!(n.kind, OpKind::Join))
            .unwrap();
        assert_eq!(join.merge_law, Some(WEIGHT_ADD_ID));
    }
}

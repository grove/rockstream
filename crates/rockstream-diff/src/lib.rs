//! DiffCtx differentiation pass for RockStream IVM.
//!
//! Implements the differentiation rules that transform logical `PlanNode`s
//! into physical `OpNode` execution graphs. Each logical node is mapped to
//! one or more physical operators with merge-law annotations attached by
//! the differentiator.

use rockstream_plan::{
    AggregateFunc, NotMergeSafeReason, OpKind, OpNode, PlanNode,
};
use rockstream_types::ids::OperatorId;
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
            PlanNode::Join {
                left, right, ..
            } => {
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
        }
    }

    /// Determine the merge law for an aggregate node based on its functions.
    fn law_for_aggregate(
        &self,
        aggregates: &[rockstream_plan::AggregateExpr],
    ) -> (Option<MergeLawId>, Option<NotMergeSafeReason>) {
        // If all aggregates are SUM/COUNT/AVG, WeightAdd applies.
        // If any is MIN/MAX, we need read-modify-write (not merge-safe).
        let has_extremum = aggregates
            .iter()
            .any(|a| matches!(a.func, AggregateFunc::Min | AggregateFunc::Max));

        if has_extremum {
            (None, Some(NotMergeSafeReason::ExtremumRequiresRmw))
        } else {
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
        let agg = nodes.iter().find(|n| matches!(n.kind, OpKind::Aggregate)).unwrap();
        assert_eq!(agg.merge_law, Some(WEIGHT_ADD_ID));
        assert!(agg.not_merge_safe_reason.is_none());
    }

    #[test]
    fn diff_min_aggregate_not_merge_safe() {
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
        let agg = nodes.iter().find(|n| matches!(n.kind, OpKind::Aggregate)).unwrap();
        assert!(agg.merge_law.is_none());
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
        let join = nodes.iter().find(|n| matches!(n.kind, OpKind::Join)).unwrap();
        assert_eq!(join.merge_law, Some(WEIGHT_ADD_ID));
    }
}


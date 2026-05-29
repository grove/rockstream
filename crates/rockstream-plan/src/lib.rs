//! PlanNode IR and physical OpNode graph for RockStream.
//!
//! Defines the `PlanNode` enum (the logical PlanIR from IVM.md §5) and the
//! physical `OpNode` graph used for execution. Each `PlanNode` carries enough
//! metadata for the planner to attach merge-law annotations.

use rockstream_types::ids::OperatorId;
use rockstream_types::merge_law::MergeLawId;

/// A logical plan node in the IVM plan IR.
///
/// The plan IR is a tree of declarative operations that describe *what* to
/// compute. The `DiffCtx` pass transforms this into a physical `OpNode` graph
/// that describes *how* to compute it incrementally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanNode {
    /// Read from a named source.
    Source { name: String },
    /// Filter rows by a predicate expression.
    Filter {
        input: Box<PlanNode>,
        predicate: Expr,
    },
    /// Project (select) columns.
    Project {
        input: Box<PlanNode>,
        columns: Vec<Expr>,
    },
    /// Apply a scalar function to each row (map).
    Map { input: Box<PlanNode>, func: Expr },
    /// Aggregate with group-by keys.
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Inner join on a condition.
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expr,
    },
    /// Union of two inputs.
    Union {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
}

/// A scalar expression in the plan IR.
///
/// This is intentionally minimal for v0.5. DataFusion expression integration
/// comes in Phase 2 (v0.11+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// A column reference by index.
    Column(usize),
    /// A literal value (stored as bytes for generality).
    Literal(Vec<u8>),
    /// A binary operation.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

/// An aggregate function expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateExpr {
    /// The aggregate function.
    pub func: AggregateFunc,
    /// The input expression to aggregate.
    pub input: Expr,
    /// Whether this is a DISTINCT aggregate.
    pub distinct: bool,
}

/// Built-in aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// A physical operator node in the execution graph.
///
/// Each `OpNode` corresponds to a running operator instance with a unique
/// `OperatorId`, an optional merge-law annotation, and references to its
/// input(s).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpNode {
    /// Unique operator ID within the pipeline.
    pub id: OperatorId,
    /// The kind of physical operator.
    pub kind: OpKind,
    /// Merge law used by this operator's arrangement (if any).
    pub merge_law: Option<MergeLawId>,
    /// Why this operator is NOT merge-safe (if applicable).
    /// From the closed `NotMergeSafeReason` enum.
    pub not_merge_safe_reason: Option<NotMergeSafeReason>,
    /// Input operator IDs.
    pub inputs: Vec<OperatorId>,
}

/// Physical operator kinds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpKind {
    /// Read from source.
    Source { name: String },
    /// Stateless filter.
    Filter,
    /// Stateless projection.
    Project,
    /// Stateless map.
    Map,
    /// Stateful aggregate with an arrangement.
    Aggregate,
    /// Stateful join with dual arrangements.
    Join,
    /// Stateless union.
    Union,
    /// Emit to output sink.
    Sink { name: String },
}

/// Closed enum of reasons an operator does not support merge-safe reads.
///
/// Used in `EXPLAIN INCREMENTAL` output. The set is fixed at compile time
/// per the v0.5 contract — new reasons require a version bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotMergeSafeReason {
    /// MIN/MAX require read-modify-write for correctness.
    ExtremumRequiresRmw,
    /// INTERSECT/EXCEPT use weight clamping, not a law.
    ClampNotALaw,
    /// User-defined aggregate with unknown algebraic properties.
    UnknownUdafProperties,
    /// Operator has no arrangement (stateless).
    Stateless,
}

impl NotMergeSafeReason {
    /// Convert to the canonical string used in `EXPLAIN` output.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ExtremumRequiresRmw => "extremum_requires_rmw",
            Self::ClampNotALaw => "clamp_not_a_law",
            Self::UnknownUdafProperties => "unknown_udaf_properties",
            Self::Stateless => "stateless",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_node_source() {
        let plan = PlanNode::Source {
            name: "orders".into(),
        };
        assert!(matches!(plan, PlanNode::Source { .. }));
    }

    #[test]
    fn plan_node_filter_project() {
        let src = PlanNode::Source {
            name: "events".into(),
        };
        let filtered = PlanNode::Filter {
            input: Box::new(src),
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(0)),
                right: Box::new(Expr::Literal(vec![0, 0, 0, 42])),
            },
        };
        let projected = PlanNode::Project {
            input: Box::new(filtered),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        };
        assert!(matches!(projected, PlanNode::Project { .. }));
    }

    #[test]
    fn op_node_with_merge_law() {
        use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;

        let node = OpNode {
            id: OperatorId(1),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(0)],
        };
        assert_eq!(node.merge_law, Some(WEIGHT_ADD_ID));
    }

    #[test]
    fn not_merge_safe_reason_strings() {
        assert_eq!(
            NotMergeSafeReason::ExtremumRequiresRmw.as_str(),
            "extremum_requires_rmw"
        );
        assert_eq!(NotMergeSafeReason::ClampNotALaw.as_str(), "clamp_not_a_law");
    }
}

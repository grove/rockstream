//! PlanNode IR and physical OpNode graph for RockStream.
//!
//! Defines the `PlanNode` enum (the logical PlanIR from IVM.md §5) and the
//! physical `OpNode` graph used for execution. Each `PlanNode` carries enough
//! metadata for the planner to attach merge-law annotations.

pub mod explain;

use rockstream_types::ids::OperatorId;
use rockstream_types::merge_law::MergeLawId;

/// Re-exported for backward compatibility — `NotMergeSafeReason` is now
/// defined in `rockstream_types::explain`.
pub use rockstream_types::explain::NotMergeSafeReason;

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
    /// Window functions (v0.19).
    Window {
        input: Box<PlanNode>,
        window_exprs: Vec<WindowExpr>,
    },
    /// Tumbling time-window operator (v0.20).
    ///
    /// Groups rows into fixed-size, non-overlapping time windows of
    /// `window_size_ms` milliseconds. Windows close when the event-time
    /// watermark advances past the window end. `time_col` is the index of
    /// the column containing the event timestamp (i64 milliseconds, big-endian
    /// 8 bytes). Late rows (arriving after window close) are handled per
    /// `late_data_policy`. Watermark advances are tracked via `MaxRegister/v1`
    /// (semilattice, idempotent).
    TumbleWindow {
        input: Box<PlanNode>,
        /// Column index holding the event timestamp (i64 ms, BE 8 bytes).
        time_col: usize,
        /// Duration of each tumbling window in milliseconds.
        window_size_ms: i64,
        /// Policy for rows that arrive after the window has closed.
        late_data_policy: LateDataPolicy,
    },
    /// Top-K operator (v0.21).
    ///
    /// Maintains the top-`k` rows ranked by the column at `rank_col` (i64
    /// big-endian; descending: highest value = rank 1).  Optional
    /// `partition_by` columns create independent Top-K groups.
    ///
    /// Internally tracks `k + epsilon` rows as a buffer (epsilon = k) to
    /// handle the delete-refill path without a full state rescan.  When a
    /// ranked row is deleted the next-best row from the buffer fills its
    /// slot.  When a new row outranks the current k-th row a delta swap
    /// is emitted (retract the displaced row, insert the new row).
    TopK {
        input: Box<PlanNode>,
        /// Number of top rows to maintain per partition.
        k: usize,
        /// Column index to rank by (i64 BE; descending: highest = rank 1).
        rank_col: usize,
        /// Column indices to partition by.  Empty = single global partition.
        partition_by: Vec<usize>,
    },
    /// Recursive operator (v0.22).
    ///
    /// Computes the fixed-point of iterating `step` starting from `base`.
    /// Semi-naive evaluation: each iteration feeds only the new delta rows
    /// into `step`, not the entire accumulated relation.
    ///
    /// Convergence is detected when the iteration produces no new rows.
    /// `max_iterations` is a safety cap (typically 1024) that prevents
    /// infinite loops on non-convergent non-monotone queries.
    ///
    /// When `monotone = true`, the relation is insert-only: retractions are
    /// rejected at runtime with `RS-1009`.  Monotone recursion publishes a
    /// `complete_through` token once convergence is reached, enabling
    /// downstream operators to consume partial-progress results safely.
    ///
    /// When `monotone = false`, the DRed (Delete and Re-derive) strategy is
    /// required.  If DRed proves unsound under concurrent deletes the escape
    /// hatch is active: non-monotone deltas are rejected with `RS-1009` and
    /// the operator reports `not_merge_safe_reason=recursion_dred_required`.
    Recursion {
        /// The base relation providing the initial facts.
        base: Box<PlanNode>,
        /// The step relation: maps the current accumulated output back as
        /// input to derive new rows.  Typically a self-join or filter.
        step: Box<PlanNode>,
        /// Safety cap on the number of semi-naive iterations per epoch.
        max_iterations: usize,
        /// Whether this recursion is restricted to monotone (insert-only) terms.
        monotone: bool,
    },
}

/// Policy for late-arriving rows in time-window operators.
///
/// A row is "late" if its event timestamp falls within a window that has
/// already been closed by the watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LateDataPolicy {
    /// Silently discard the late row. The closed window output is unchanged.
    Drop,
    /// Retract the previous window output and re-emit including the late row.
    Update,
    /// Route the late row to a named side-channel sink.
    RouteToSink { sink_name: String },
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

/// A window function expression (v0.19).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowExpr {
    pub func: WindowFunc,
    pub partition_by: Vec<usize>,
    pub order_by: Vec<usize>,
}

/// Window functions supported in v0.19.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(u64),
    Lag { offset: usize },
    Lead { offset: usize },
    SlidingSum { frame_rows: usize },
    SlidingAvg { frame_rows: usize },
}

/// Window operator IVM strategy (v0.19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowStrategy {
    PartitionRecompute,
    SlidingAggregate,
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
    /// Window function operator (v0.19).
    Window { strategy: WindowStrategy },
    /// Tumbling time-window operator (v0.20).
    ///
    /// Watermark state tracked via `MaxRegister/v1`; output emitted once per
    /// window when the watermark advances past the window end.
    TumbleWindow {
        window_size_ms: i64,
        late_data_policy: LateDataPolicy,
    },
    /// Top-K operator (v0.21).
    ///
    /// Emits the top-`k` rows per partition ranked by `rank_col` (i64 BE;
    /// descending).  Delta swaps are emitted when the ranked set changes.
    /// The `k + epsilon` buffer (epsilon = k) backs the delete-refill path.
    TopK {
        k: usize,
        rank_col: usize,
        partition_by: Vec<usize>,
    },
    /// Recursive operator (v0.22).
    ///
    /// Runs semi-naive fixed-point iteration until convergence or
    /// `max_iterations` is reached.  `monotone = true` enables
    /// `complete_through` partial-progress publication; `monotone = false`
    /// activates the DRed escape hatch (`not_merge_safe_reason =
    /// recursion_dred_required`) and rejects retractions at runtime.
    Recursion {
        /// Safety cap on semi-naive iterations per epoch.
        max_iterations: usize,
        /// Whether this operator is restricted to monotone (insert-only) terms.
        monotone: bool,
    },
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

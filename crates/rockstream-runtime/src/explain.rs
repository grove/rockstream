//! `rockstream explain` — operator graph annotated with merge-law info.
//!
//! Implements the Level 1 (default) `EXPLAIN INCREMENTAL` output described in
//! DESIGN.md §14.8: a human-readable table of every operator in a plan
//! annotated with its merge law (or the reason it is not merge-safe).
//!
//! # Usage
//! ```rust
//! use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
//! use rockstream_runtime::explain::explain_plan;
//!
//! let plan = PlanNode::Aggregate {
//!     input: Box::new(PlanNode::Source { name: "orders".into() }),
//!     group_by: vec![Expr::Column(0)],
//!     aggregates: vec![AggregateExpr {
//!         func: AggregateFunc::Sum,
//!         input: Expr::Column(1),
//!         distinct: false,
//!     }],
//! };
//! let rows = explain_plan(&plan);
//! assert!(rows.iter().any(|r| r.merge_law.as_deref() == Some("WeightAdd/v1")));
//! ```

use rockstream_diff::DiffCtx;
use rockstream_plan::{OpKind, PlanNode};
use rockstream_types::laws::LawRegistry;

/// A single row in the `EXPLAIN INCREMENTAL` output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainRow {
    /// Operator ID (sequential within this plan).
    pub op_id: u64,
    /// Human-readable operator kind (e.g., "Source", "Aggregate", "Filter").
    pub kind: String,
    /// The merge law name (e.g., `"WeightAdd/v1"`, `"MaxRegister/v1"`) if the
    /// operator's arrangement is merge-safe.
    pub merge_law: Option<String>,
    /// Reason this operator is NOT merge-safe (e.g.,
    /// `"extremum_requires_rmw"`) when `merge_law` is a cached-slot law.
    pub not_merge_safe_reason: Option<String>,
}

impl ExplainRow {
    /// Format as a single human-readable line for the CLI output.
    ///
    /// Examples:
    /// ```text
    ///  0  Source(orders)            stateless
    ///  1  Aggregate                 merge_law=WeightAdd/v1
    ///  2  Aggregate                 merge_law=MaxRegister/v1  not_merge_safe=extremum_requires_rmw
    /// ```
    pub fn format_line(&self) -> String {
        let law_part = match (&self.merge_law, &self.not_merge_safe_reason) {
            (Some(law), Some(reason)) => {
                format!("merge_law={law}  not_merge_safe={reason}")
            }
            (Some(law), None) => format!("merge_law={law}"),
            (None, Some(reason)) => format!("not_merge_safe={reason}"),
            (None, None) => "stateless".to_string(),
        };
        format!("{:>3}  {:<28}  {}", self.op_id, self.kind, law_part)
    }
}

/// Format a `kind` label for the operator-kind string shown in explain output.
fn kind_label(kind: &OpKind) -> String {
    match kind {
        OpKind::Source { name } => format!("Source({name})"),
        OpKind::Filter => "Filter".to_string(),
        OpKind::Project => "Project".to_string(),
        OpKind::Map => "Map".to_string(),
        OpKind::Aggregate => "Aggregate".to_string(),
        OpKind::Join => "Join".to_string(),
        OpKind::Union => "Union".to_string(),
        OpKind::Sink { name } => format!("Sink({name})"),
        OpKind::Window { strategy } => format!("Window[{strategy:?}]"),
        OpKind::TumbleWindow {
            window_size_ms,
            late_data_policy,
        } => format!("TumbleWindow[{window_size_ms}ms,{late_data_policy:?}]"),
        OpKind::TopK {
            k,
            rank_col,
            partition_by,
        } => format!("TopK[k={k},rank={rank_col},partitions={partition_by:?}]"),
        OpKind::Recursion {
            max_iterations,
            monotone,
        } => format!("Recursion[max_iter={max_iterations},monotone={monotone}]"),
    }
}

/// Differentiate `plan` and return a list of `ExplainRow`s, one per operator,
/// in topological order (sources first, sinks last).
///
/// This is the backend for `rockstream explain <view>` (DESIGN.md §14.8,
/// Level 1 default output).
pub fn explain_plan(plan: &PlanNode) -> Vec<ExplainRow> {
    let mut ctx = DiffCtx::new();
    let nodes = ctx.differentiate(plan);
    let registry = LawRegistry::with_builtins();

    nodes
        .iter()
        .map(|node| {
            let law_name = node.merge_law.and_then(|id| {
                registry
                    .get(id)
                    .map(|law| format!("{}/{}", law.name(), law.version()))
            });
            let reason_str = node.not_merge_safe_reason.map(|r| r.as_str().to_string());

            ExplainRow {
                op_id: node.id.0,
                kind: kind_label(&node.kind),
                merge_law: law_name,
                not_merge_safe_reason: reason_str,
            }
        })
        .collect()
}

/// Render the full explain output as a multi-line string (header + rows).
///
/// The format matches DESIGN.md §14.8 Level 1 (default human-readable).
pub fn render_explain(plan_name: &str, plan: &PlanNode) -> String {
    let rows = explain_plan(plan);
    let mut out = format!("EXPLAIN INCREMENTAL  {plan_name}\n");
    out.push_str(&"-".repeat(60));
    out.push('\n');
    for row in &rows {
        out.push_str(&row.format_line());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};

    fn sum_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    fn max_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "prices".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Max,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    fn min_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "temps".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Min,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    #[test]
    fn sum_shows_weight_add_law() {
        let rows = explain_plan(&sum_plan());
        let agg = rows
            .iter()
            .find(|r| r.kind == "Aggregate")
            .expect("must have Aggregate row");
        assert_eq!(
            agg.merge_law.as_deref(),
            Some("WeightAdd/v1"),
            "SUM must report WeightAdd/v1"
        );
        assert!(
            agg.not_merge_safe_reason.is_none(),
            "SUM must not have a not_merge_safe_reason"
        );
    }

    #[test]
    fn count_shows_weight_add_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "events".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                input: Expr::Column(0),
                distinct: false,
            }],
        };
        let rows = explain_plan(&plan);
        let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
        assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
        assert!(agg.not_merge_safe_reason.is_none());
    }

    #[test]
    fn avg_shows_weight_add_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "scores".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Avg,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let rows = explain_plan(&plan);
        let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
        assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
        assert!(agg.not_merge_safe_reason.is_none());
    }

    #[test]
    fn max_shows_max_register_and_not_merge_safe() {
        let rows = explain_plan(&max_plan());
        let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
        assert_eq!(
            agg.merge_law.as_deref(),
            Some("MaxRegister/v1"),
            "MAX must report MaxRegister/v1 as cached-slot law"
        );
        assert_eq!(
            agg.not_merge_safe_reason.as_deref(),
            Some("extremum_requires_rmw"),
            "MAX must report extremum_requires_rmw"
        );
    }

    #[test]
    fn min_shows_min_register_and_not_merge_safe() {
        let rows = explain_plan(&min_plan());
        let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
        assert_eq!(
            agg.merge_law.as_deref(),
            Some("MinRegister/v1"),
            "MIN must report MinRegister/v1 as cached-slot law"
        );
        assert_eq!(
            agg.not_merge_safe_reason.as_deref(),
            Some("extremum_requires_rmw"),
            "MIN must report extremum_requires_rmw"
        );
    }

    #[test]
    fn distinct_aggregate_shows_weight_add_law() {
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "users".into(),
            }),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                input: Expr::Column(0),
                distinct: true,
            }],
        };
        let rows = explain_plan(&plan);
        let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
        // DISTINCT COUNT also uses WeightAdd/v1 (distinct tracking via Z-set weights).
        assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
    }

    #[test]
    fn filter_project_are_stateless() {
        let plan = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "orders".into(),
                }),
                predicate: Expr::Column(0),
            }),
            columns: vec![Expr::Column(0)],
        };
        let rows = explain_plan(&plan);
        for row in &rows {
            assert!(
                row.merge_law.is_none() || row.kind.starts_with("Source"),
                "filter/project must be stateless"
            );
        }
    }

    #[test]
    fn render_explain_contains_header_and_rows() {
        let out = render_explain("my_view", &sum_plan());
        assert!(out.contains("EXPLAIN INCREMENTAL  my_view"));
        assert!(out.contains("WeightAdd/v1"));
        assert!(out.contains("Source(orders)"));
    }

    #[test]
    fn explain_row_format_line_sum() {
        let row = ExplainRow {
            op_id: 1,
            kind: "Aggregate".to_string(),
            merge_law: Some("WeightAdd/v1".to_string()),
            not_merge_safe_reason: None,
        };
        let line = row.format_line();
        assert!(line.contains("WeightAdd/v1"));
        assert!(!line.contains("not_merge_safe"));
    }

    #[test]
    fn explain_row_format_line_max() {
        let row = ExplainRow {
            op_id: 2,
            kind: "Aggregate".to_string(),
            merge_law: Some("MaxRegister/v1".to_string()),
            not_merge_safe_reason: Some("extremum_requires_rmw".to_string()),
        };
        let line = row.format_line();
        assert!(line.contains("MaxRegister/v1"));
        assert!(line.contains("extremum_requires_rmw"));
    }
}

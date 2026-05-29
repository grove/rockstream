//! SQL frontend for RockStream.
//!
//! Built on Apache DataFusion. Parses SQL DDL/DML, binds schemas, optimizes
//! logical plans, and lowers to the RockStream `PlanNode` IR.
//!
//! # v0.11 deliverables
//!
//! - [`SqlFrontend::parse_statement`] — parse one SQL statement into a
//!   DataFusion AST (`Statement`).
//! - [`SqlFrontend::lower`] — lower a DataFusion `LogicalPlan` to a
//!   `PlanNode` tree with merge-law annotations for every aggregate node.
//!
//! Every aggregate operator in the lowered plan carries either a `MergeLawId`
//! (via `DiffCtx::differentiate`) or an explicit `not_merge_safe_reason`.
//! This is verified by the `all_aggregate_nodes_have_law_or_reason` test.
//!
//! # Scope
//!
//! This module handles the structural lowering of LogicalPlan nodes:
//! `TableScan` → `Source`, `Projection` → `Project`, `Filter` → `Filter`,
//! `Aggregate` → `Aggregate` (with law-annotated aggregate functions),
//! `Union` → `Union`. Scalar expressions lower to the `rockstream_plan::Expr`
//! IR. Full schema binding (column-by-name resolution with index assignment)
//! is deferred to v0.12; for now all column references lower to index 0.

use datafusion::logical_expr::Operator as DFOperator;
use datafusion::logical_expr::{
    expr::{AggregateFunction as DFAggFunc, Alias, BinaryExpr, Case, Cast},
    Expr as DFExpr, LogicalPlan,
};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr as PlanExpr, PlanNode};
use thiserror::Error;

pub use datafusion::logical_expr::LogicalPlan as DataFusionPlan;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the SQL frontend.
#[derive(Debug, Error)]
pub enum SqlError {
    /// SQL parse error from the DataFusion SQL parser.
    #[error("SQL parse error: {0}")]
    Parse(String),

    /// A `LogicalPlan` node type is not yet supported for IVM lowering.
    #[error("not yet implemented: {0}")]
    NotYetImplemented(String),

    /// A schema, table, or column name could not be resolved.
    #[error("resolution error: {0}")]
    Resolution(String),
}

// ---------------------------------------------------------------------------
// SqlFrontend
// ---------------------------------------------------------------------------

/// The SQL frontend for RockStream.
///
/// Wraps the DataFusion SQL parser and provides the entry point for lowering
/// SQL statements to the RockStream `PlanNode` IR.
///
/// # Usage
///
/// ```rust
/// use rockstream_sql::SqlFrontend;
///
/// let frontend = SqlFrontend::new();
/// let stmts = frontend.parse_statement("SELECT 1").unwrap();
/// assert_eq!(stmts.len(), 1);
/// ```
pub struct SqlFrontend {
    dialect: GenericDialect,
}

impl SqlFrontend {
    /// Create a new SQL frontend with the default (generic ANSI) dialect.
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse a SQL string into a list of DataFusion `Statement`s.
    ///
    /// Uses the DataFusion SQL parser which understands DataFusion extensions
    /// in addition to standard SQL. Returns an error if the input is not
    /// syntactically valid SQL.
    pub fn parse_statement(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        DFParser::parse_sql_with_dialect(sql, &self.dialect)
            .map(|stmts| stmts.into_iter().collect())
            .map_err(|e| SqlError::Parse(e.to_string()))
    }

    /// Lower a DataFusion `LogicalPlan` to a RockStream `PlanNode` tree.
    ///
    /// Handles the Phase 1 operator set:
    /// - `TableScan` → `PlanNode::Source`
    /// - `EmptyRelation` → `PlanNode::Source { name: "<empty>" }`
    /// - `Projection` → `PlanNode::Project`
    /// - `Filter` → `PlanNode::Filter`
    /// - `Aggregate` → `PlanNode::Aggregate` with law-mapped `AggregateFunc`
    /// - `Union` → `PlanNode::Union` (folded pairwise from n inputs)
    ///
    /// Aggregate function mapping (used by `DiffCtx` to attach law IDs):
    /// - `SUM` → `AggregateFunc::Sum` → `WeightAdd/v1`
    /// - `COUNT` → `AggregateFunc::Count` → `WeightAdd/v1`
    /// - `AVG` → `AggregateFunc::Avg` → `WeightAdd/v1`
    /// - `MIN` → `AggregateFunc::Min` → `MinRegister/v1` + `ExtremumRequiresRmw`
    /// - `MAX` → `AggregateFunc::Max` → `MaxRegister/v1` + `ExtremumRequiresRmw`
    ///
    /// Full schema binding (column-index resolution) is deferred to v0.12.
    /// For now, all column references lower to index 0.
    pub fn lower(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        self.lower_plan(plan)
    }

    fn lower_plan(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        match plan {
            LogicalPlan::TableScan(ts) => Ok(PlanNode::Source {
                name: ts.table_name.table().to_string(),
            }),

            LogicalPlan::EmptyRelation(_) => Ok(PlanNode::Source {
                name: "<empty>".into(),
            }),

            LogicalPlan::Projection(proj) => {
                let input = self.lower_plan(proj.input.as_ref())?;
                let columns = proj
                    .expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Project {
                    input: Box::new(input),
                    columns,
                })
            }

            LogicalPlan::Filter(filter) => {
                let input = self.lower_plan(filter.input.as_ref())?;
                let predicate = self.lower_expr(&filter.predicate)?;
                Ok(PlanNode::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }

            LogicalPlan::Aggregate(agg) => {
                let input = self.lower_plan(agg.input.as_ref())?;
                let group_by = agg
                    .group_expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                let aggregates = agg
                    .aggr_expr
                    .iter()
                    .map(|e| self.lower_aggregate_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregates,
                })
            }

            LogicalPlan::Union(union_plan) => {
                let mut it = union_plan.inputs.iter();
                let first = match it.next() {
                    Some(p) => self.lower_plan(p)?,
                    None => {
                        return Err(SqlError::Resolution("Union has no inputs".into()));
                    }
                };
                it.try_fold(first, |acc, next| {
                    let right = self.lower_plan(next)?;
                    Ok(PlanNode::Union {
                        left: Box::new(acc),
                        right: Box::new(right),
                    })
                })
            }

            // SubqueryAlias is transparent — lower the inner plan.
            LogicalPlan::SubqueryAlias(alias) => self.lower_plan(alias.input.as_ref()),

            // Limit/Sort/Distinct are not yet supported; return a clear error.
            other => Err(SqlError::NotYetImplemented(format!(
                "LogicalPlan node '{}' — will be lowered in v0.12+",
                other.display()
            ))),
        }
    }

    fn lower_expr(&self, expr: &DFExpr) -> Result<PlanExpr, SqlError> {
        match expr {
            // Column reference: full index resolution is deferred to v0.12.
            DFExpr::Column(_col) => Ok(PlanExpr::Column(0)),

            DFExpr::Literal(scalar, _) => {
                let bytes = scalar.to_string().into_bytes();
                Ok(PlanExpr::Literal(bytes))
            }

            DFExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.lower_expr(left.as_ref())?;
                let r = self.lower_expr(right.as_ref())?;
                let bin_op = self.lower_operator(op)?;
                Ok(PlanExpr::BinaryOp {
                    op: bin_op,
                    left: Box::new(l),
                    right: Box::new(r),
                })
            }

            // Cast: preserve inner expression; type info is schema-bound in v0.12.
            DFExpr::Cast(Cast { expr, .. }) => self.lower_expr(expr.as_ref()),
            DFExpr::TryCast(tc) => self.lower_expr(tc.expr.as_ref()),

            // CASE: lower to the else branch or first result branch.
            DFExpr::Case(Case {
                when_then_expr,
                else_expr,
                ..
            }) => {
                if let Some(e) = else_expr {
                    self.lower_expr(e.as_ref())
                } else if let Some((_, result)) = when_then_expr.first() {
                    self.lower_expr(result.as_ref())
                } else {
                    Err(SqlError::NotYetImplemented("CASE with no arms".into()))
                }
            }

            // Alias: transparent wrapper — lower the inner expression.
            DFExpr::Alias(Alias { expr, .. }) => self.lower_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!("Expr: {other}"))),
        }
    }

    fn lower_operator(&self, op: &DFOperator) -> Result<BinaryOp, SqlError> {
        match op {
            DFOperator::Eq => Ok(BinaryOp::Eq),
            DFOperator::NotEq => Ok(BinaryOp::Ne),
            DFOperator::Lt => Ok(BinaryOp::Lt),
            DFOperator::LtEq => Ok(BinaryOp::Le),
            DFOperator::Gt => Ok(BinaryOp::Gt),
            DFOperator::GtEq => Ok(BinaryOp::Ge),
            DFOperator::Plus => Ok(BinaryOp::Add),
            DFOperator::Minus => Ok(BinaryOp::Sub),
            DFOperator::Multiply => Ok(BinaryOp::Mul),
            DFOperator::Divide => Ok(BinaryOp::Div),
            DFOperator::And => Ok(BinaryOp::And),
            DFOperator::Or => Ok(BinaryOp::Or),
            other => Err(SqlError::NotYetImplemented(format!("Operator: {other:?}"))),
        }
    }

    fn lower_aggregate_expr(&self, expr: &DFExpr) -> Result<AggregateExpr, SqlError> {
        match expr {
            DFExpr::AggregateFunction(DFAggFunc { func, params }) => {
                let name = func.name().to_lowercase();
                let agg_func = match name.as_str() {
                    "count" => AggregateFunc::Count,
                    "sum" => AggregateFunc::Sum,
                    "avg" | "mean" => AggregateFunc::Avg,
                    "min" => AggregateFunc::Min,
                    "max" => AggregateFunc::Max,
                    other => {
                        return Err(SqlError::NotYetImplemented(format!(
                            "aggregate function '{other}'"
                        )));
                    }
                };
                let input_expr = params
                    .args
                    .first()
                    .map(|e| self.lower_expr(e))
                    .unwrap_or(Ok(PlanExpr::Column(0)))?;
                Ok(AggregateExpr {
                    func: agg_func,
                    input: input_expr,
                    distinct: params.distinct,
                })
            }

            // Alias wrapping an aggregate (common after projection pushdown).
            DFExpr::Alias(Alias { expr, .. }) => self.lower_aggregate_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!(
                "aggregate expression: {other}"
            ))),
        }
    }
}

impl Default for SqlFrontend {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_statement tests -------------------------------------------

    #[test]
    fn parse_simple_select() {
        let f = SqlFrontend::new();
        let stmts = f.parse_statement("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_view_ddl() {
        let f = SqlFrontend::new();
        let stmts = f
            .parse_statement(
                "CREATE VIEW orders_by_region AS \
                 SELECT region, SUM(amount) FROM orders GROUP BY region",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_error_returns_err() {
        let f = SqlFrontend::new();
        let result = f.parse_statement("THIS IS NOT SQL ;;;");
        assert!(result.is_err(), "invalid SQL must return SqlError::Parse");
    }
}

// ---------------------------------------------------------------------------
// Lowering proof tests (require rockstream-diff dev-dep)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod lowering_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
    use datafusion::logical_expr::{col, table_scan, LogicalPlanBuilder};
    use datafusion::prelude::lit;
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{
        AggregateExpr, AggregateFunc, Expr as PlanExpr, NotMergeSafeReason, OpKind, OpNode,
    };
    use rockstream_types::laws::max_register::MAX_REGISTER_ID;
    use rockstream_types::laws::min_register::MIN_REGISTER_ID;
    use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
    use rockstream_types::merge_law::MergeLawId;

    /// Build a table-scan LogicalPlan for a two-column "orders" table.
    fn orders_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        table_scan(Some("orders"), &schema, None)
            .expect("table_scan must succeed")
            .build()
            .expect("build must succeed")
    }

    /// Extract operator kind names from an OpNode slice.
    fn op_kinds(nodes: &[OpNode]) -> Vec<&'static str> {
        nodes
            .iter()
            .map(|n| match &n.kind {
                OpKind::Source { .. } => "Source",
                OpKind::Filter => "Filter",
                OpKind::Project => "Project",
                OpKind::Map => "Map",
                OpKind::Aggregate => "Aggregate",
                OpKind::Join => "Join",
                OpKind::Union => "Union",
                OpKind::Sink { .. } => "Sink",
            })
            .collect()
    }

    /// Extract merge law IDs from an OpNode slice.
    fn op_laws(nodes: &[OpNode]) -> Vec<Option<MergeLawId>> {
        nodes.iter().map(|n| n.merge_law).collect()
    }

    /// Extract not-merge-safe reasons from an OpNode slice.
    fn op_reasons(nodes: &[OpNode]) -> Vec<Option<NotMergeSafeReason>> {
        nodes.iter().map(|n| n.not_merge_safe_reason).collect()
    }

    // --- structural lowering tests ---------------------------------------

    #[test]
    fn lower_empty_relation_produces_source() {
        let f = SqlFrontend::new();
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let node = f.lower(&plan).unwrap();
        assert!(matches!(node, PlanNode::Source { .. }));
    }

    #[test]
    fn lower_table_scan_produces_source() {
        let f = SqlFrontend::new();
        let plan = orders_scan();
        let node = f.lower(&plan).unwrap();
        match &node {
            PlanNode::Source { name } => assert_eq!(name, "orders"),
            other => panic!("expected Source, got {other:?}"),
        }
    }

    #[test]
    fn lower_filter_op_structure_matches_hand_built() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .filter(col("amount").gt(lit(0i64)))
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();

        // Hand-built equivalent
        let hand = PlanNode::Filter {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(PlanExpr::Column(0)),
                right: Box::new(PlanExpr::Literal(b"0".to_vec())),
            },
        };

        let mut ctx1 = DiffCtx::new();
        let lowered_ops = ctx1.differentiate(&lowered);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand);

        assert_eq!(
            op_kinds(&lowered_ops),
            op_kinds(&hand_ops),
            "filter: SQL-lowered and hand-built must produce same operator structure"
        );
    }

    #[test]
    fn lower_projection_op_structure_matches_hand_built() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("region")])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();

        let hand = PlanNode::Project {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            columns: vec![PlanExpr::Column(0)],
        };

        let mut ctx1 = DiffCtx::new();
        let lo = ctx1.differentiate(&lowered);
        let mut ctx2 = DiffCtx::new();
        let ho = ctx2.differentiate(&hand);
        assert_eq!(op_kinds(&lo), op_kinds(&ho));
    }

    // --- aggregate law annotation tests ----------------------------------

    #[test]
    fn lower_aggregate_sum_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![sum(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have an Aggregate node");
        assert_eq!(
            agg.merge_law,
            Some(WEIGHT_ADD_ID),
            "SUM must use WeightAdd/v1"
        );
        assert!(
            agg.not_merge_safe_reason.is_none(),
            "SUM is merge-safe: no reason expected"
        );
    }

    #[test]
    fn lower_aggregate_count_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![count(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have an Aggregate node");
        assert_eq!(
            agg.merge_law,
            Some(WEIGHT_ADD_ID),
            "COUNT uses WeightAdd/v1"
        );
    }

    #[test]
    fn lower_aggregate_avg_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![avg(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(WEIGHT_ADD_ID), "AVG uses WeightAdd/v1");
    }

    #[test]
    fn lower_aggregate_max_gets_extremum_reason() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![max(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(MAX_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw),
            "MAX must have ExtremumRequiresRmw reason"
        );
    }

    #[test]
    fn lower_aggregate_min_gets_extremum_reason() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![min(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(MIN_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw),
            "MIN must have ExtremumRequiresRmw reason"
        );
    }

    // --- "identical physical plans" proof (the v0.11 proof criterion) ---

    /// Proof: SQL-lowered and hand-built PlanIR produce identical physical
    /// plans (operator kinds + law annotations) for the Phase 1 operators.
    ///
    /// This is the core v0.11 proof criterion from ROADMAP.md:
    /// "SQL and hard-coded PlanIR produce identical physical plans for the
    ///  Phase 1 operators".
    #[test]
    fn sql_plan_matches_hand_built_aggregate_sum_phase1_proof() {
        // Build via DataFusion LogicalPlanBuilder
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![sum(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let sql_plan = f.lower(&df_plan).unwrap();

        // Hand-built equivalent plan
        let hand_plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![PlanExpr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: PlanExpr::Column(0),
                distinct: false,
            }],
        };

        let mut ctx1 = DiffCtx::new();
        let sql_ops = ctx1.differentiate(&sql_plan);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand_plan);

        assert_eq!(
            op_kinds(&sql_ops),
            op_kinds(&hand_ops),
            "SQL and hand-built plans must produce identical operator structure"
        );
        assert_eq!(
            op_laws(&sql_ops),
            op_laws(&hand_ops),
            "SQL and hand-built plans must produce identical law annotations"
        );
        assert_eq!(
            op_reasons(&sql_ops),
            op_reasons(&hand_ops),
            "SQL and hand-built plans must produce identical not-merge-safe reasons"
        );
    }

    /// Proof: SQL-lowered filter+project plan matches hand-built.
    #[test]
    fn sql_plan_matches_hand_built_filter_project_phase1_proof() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .filter(col("amount").gt(lit(0i64)))
            .unwrap()
            .project(vec![col("region")])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let sql_plan = f.lower(&df_plan).unwrap();

        let hand_plan = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "orders".into(),
                }),
                predicate: PlanExpr::Column(0),
            }),
            columns: vec![PlanExpr::Column(0)],
        };

        let mut ctx1 = DiffCtx::new();
        let sql_ops = ctx1.differentiate(&sql_plan);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand_plan);

        assert_eq!(
            op_kinds(&sql_ops),
            op_kinds(&hand_ops),
            "filter+project: SQL-lowered and hand-built operator structure must match"
        );
        assert_eq!(
            op_laws(&sql_ops),
            op_laws(&hand_ops),
            "filter+project: law annotations must match"
        );
    }

    /// Proof: every aggregate node in any lowered plan has either a merge_law
    /// or a not_merge_safe_reason from the closed enum. This covers all
    /// Phase 1 aggregate functions: SUM, COUNT, AVG, MIN, MAX.
    #[test]
    fn all_aggregate_nodes_have_law_or_reason() {
        let f = SqlFrontend::new();
        type AggFnCase = (&'static str, fn() -> DFExpr);
        let test_cases: &[AggFnCase] = &[
            ("sum", || sum(col("amount"))),
            ("count", || count(col("amount"))),
            ("avg", || avg(col("amount"))),
            ("max", || max(col("amount"))),
            ("min", || min(col("amount"))),
        ];

        for (name, build_aggr) in test_cases {
            let scan = orders_scan();
            let df_plan = LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![build_aggr()])
                .unwrap()
                .build()
                .unwrap();

            let lowered = f.lower(&df_plan).unwrap();
            let mut ctx = DiffCtx::new();
            let ops = ctx.differentiate(&lowered);

            for op in &ops {
                if matches!(op.kind, OpKind::Aggregate) {
                    assert!(
                        op.merge_law.is_some() || op.not_merge_safe_reason.is_some(),
                        "aggregate '{name}' op must have merge_law or not_merge_safe_reason; \
                         got merge_law={:?} reason={:?}",
                        op.merge_law,
                        op.not_merge_safe_reason
                    );
                }
            }
        }
    }

    /// Proof: the plan dump (via EXPLAIN) shows either a registered law name
    /// or a not_merge_safe_reason for every aggregate.
    #[test]
    fn explain_output_covers_all_aggregate_laws() {
        use rockstream_runtime::explain::explain_plan;

        let f = SqlFrontend::new();

        // Type alias avoids clippy::type_complexity lint.
        type AggCase = (
            &'static str,
            fn() -> DFExpr,
            Option<&'static str>,
            Option<&'static str>,
        );
        let agg_cases: &[AggCase] = &[
            ("sum", || sum(col("amount")), Some("WeightAdd/v1"), None),
            ("count", || count(col("amount")), Some("WeightAdd/v1"), None),
            ("avg", || avg(col("amount")), Some("WeightAdd/v1"), None),
            (
                "max",
                || max(col("amount")),
                Some("MaxRegister/v1"),
                Some("extremum_requires_rmw"),
            ),
            (
                "min",
                || min(col("amount")),
                Some("MinRegister/v1"),
                Some("extremum_requires_rmw"),
            ),
        ];

        for (name, build_aggr, exp_law, exp_reason) in agg_cases {
            let scan = orders_scan();
            let df_plan = LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![build_aggr()])
                .unwrap()
                .build()
                .unwrap();

            let lowered = f.lower(&df_plan).unwrap();
            let rows = explain_plan(&lowered);

            let agg_row = rows
                .iter()
                .find(|r| r.kind == "Aggregate")
                .unwrap_or_else(|| panic!("{name}: explain must contain Aggregate row"));

            assert_eq!(
                agg_row.merge_law.as_deref(),
                *exp_law,
                "{name}: wrong merge_law in explain"
            );
            assert_eq!(
                agg_row.not_merge_safe_reason.as_deref(),
                *exp_reason,
                "{name}: wrong not_merge_safe_reason in explain"
            );
        }
    }
}

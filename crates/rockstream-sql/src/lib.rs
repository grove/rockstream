//! SQL frontend for RockStream.
//!
//! Built on Apache DataFusion. Parses SQL DDL/DML, binds schemas, optimizes
//! logical plans, and lowers to the RockStream `PlanNode` IR.
//!
//! # Phase 2 entry points
//!
//! - [`SqlFrontend::parse_statement`] — parse one SQL statement into a
//!   DataFusion AST (`Statement`).
//! - [`SqlFrontend::lower`] — lower a `LogicalPlan` to a `PlanNode` tree.
//!   (Not yet implemented; returns `SqlError::NotYetImplemented`.)
//!
//! These are the two surfaces that Phase 2 milestones (IVM-4 through IVM-6)
//! will build on. All other DataFusion integration (schema catalog, physical
//! planning, extension nodes) layers on top of this skeleton.

use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use thiserror::Error;

pub use datafusion::logical_expr::LogicalPlan;
pub use rockstream_plan::PlanNode;

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
    ///
    /// Phase 2 milestones call this to parse `CREATE MATERIALIZED VIEW`,
    /// `SELECT`, and `EXPLAIN INCREMENTAL` statements.
    pub fn parse_statement(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        DFParser::parse_sql_with_dialect(sql, &self.dialect)
            .map(|stmts| stmts.into_iter().collect())
            .map_err(|e| SqlError::Parse(e.to_string()))
    }

    /// Lower a DataFusion `LogicalPlan` to a RockStream `PlanNode` tree.
    ///
    /// **Not yet implemented.** This is the primary Phase 2 deliverable:
    /// the lowering pass transforms DataFusion's logical plan (which knows
    /// about relational algebra) into the RockStream operator graph (which
    /// knows about incremental maintenance, merge laws, and Z-sets).
    ///
    /// Returns `SqlError::NotYetImplemented` until Phase 2 milestones
    /// IVM-4 through IVM-6 implement the individual node types.
    pub fn lower(&self, _plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        Err(SqlError::NotYetImplemented(
            "LogicalPlan lowering is implemented in Phase 2 (IVM-4..IVM-6)".into(),
        ))
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

    #[test]
    fn lower_returns_not_yet_implemented() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::*;
        // Create a trivial empty-values plan.
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let f = SqlFrontend::new();
        let result = f.lower(&plan);
        assert!(
            matches!(result, Err(SqlError::NotYetImplemented(_))),
            "lower must return NotYetImplemented until Phase 2"
        );
    }
}

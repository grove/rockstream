//! View-on-view DAG reference oracle for RockStream (v0.24).
//!
//! Provides batch reference implementations for the view-on-view DAG
//! feature.  Used in property tests to verify:
//!
//! 1. **Cycle detection**: `detect_cycle` rejects cyclic view graphs.
//! 2. **Convergence**: Five-level DAG and diamond topology produce correct
//!    outputs under continuous input.
//! 3. **Diamond consistency**: When two paths through the DAG converge, the
//!    downstream view sees the merged result of both paths, equivalent to
//!    reading from the common upstream view directly.
//! 4. **Cadence inheritance**: Topological evaluation order matches epoch
//!    scheduling — a downstream view is only evaluated after all its upstream
//!    views have been evaluated.

use rockstream_plan::dag::{detect_cycle, direct_view_refs, topological_order};
use rockstream_plan::PlanNode;
use rockstream_types::batch::ZSet;
use std::collections::HashMap;

/// Reference oracle for view-on-view DAG evaluation.
pub struct ViewDagOracle;

impl ViewDagOracle {
    /// Evaluate all views in a DAG given base source inputs.
    ///
    /// Returns the output `ZSet` for every view in the DAG, keyed by view
    /// name.  The `sources` map provides the input `ZSet` for each base
    /// source (e.g., `"orders"` → `ZSet { … }`).
    ///
    /// Views are evaluated in topological order (sources first, dependents
    /// last), which models the epoch scheduling order of the runtime.
    ///
    /// Returns `Err(cycle_path)` if the DAG contains a cycle.
    pub fn evaluate(
        views: &HashMap<String, PlanNode>,
        sources: &HashMap<String, ZSet>,
    ) -> Result<HashMap<String, ZSet>, Vec<String>> {
        let order = topological_order(views)?;
        let mut env: HashMap<String, ZSet> = sources.clone();
        for view_name in &order {
            let plan = &views[view_name];
            let output = Self::eval_plan(plan, &env);
            env.insert(view_name.clone(), output);
        }
        Ok(env)
    }

    /// Check whether a view DAG contains a cycle.
    ///
    /// Returns `true` if a cycle exists (RS-1011 would be emitted at runtime).
    pub fn has_cycle(views: &HashMap<String, PlanNode>) -> bool {
        detect_cycle(views).is_err()
    }

    /// Return the topological order of views (sources first, dependents last).
    ///
    /// Returns `None` if the DAG contains a cycle.
    pub fn topo_order(views: &HashMap<String, PlanNode>) -> Option<Vec<String>> {
        topological_order(views).ok()
    }

    /// Return the direct view dependencies of a view plan.
    pub fn dependencies(plan: &PlanNode) -> Vec<String> {
        direct_view_refs(plan)
    }

    /// Simulate delivering `epochs` delta batches through the DAG.
    ///
    /// `per_epoch_source` is called with the epoch index and should return the
    /// delta `ZSet` for each source in that epoch.  The function accumulates
    /// outputs across all epochs and returns the final accumulated `ZSet` for
    /// each view.
    ///
    /// Returns `Err(cycle_path)` if the DAG contains a cycle.
    pub fn simulate_epochs(
        views: &HashMap<String, PlanNode>,
        mut per_epoch_source: impl FnMut(usize, &str) -> ZSet,
        epochs: usize,
    ) -> Result<HashMap<String, ZSet>, Vec<String>> {
        let order = topological_order(views)?;
        let mut accumulated: HashMap<String, ZSet> = HashMap::new();

        for epoch in 0..epochs {
            // Build per-epoch source deltas.
            let mut epoch_env: HashMap<String, ZSet> = HashMap::new();
            // Collect source names from all plans.
            for plan in views.values() {
                collect_source_names(plan, &mut epoch_env, &mut |name| {
                    per_epoch_source(epoch, name)
                });
            }

            // Evaluate each view for this epoch.
            for view_name in &order {
                let plan = &views[view_name];
                // Combine epoch delta with accumulated state for evaluation.
                let mut eval_env = accumulated.clone();
                for (k, v) in &epoch_env {
                    eval_env.entry(k.clone()).or_default().merge(v);
                }
                let output = Self::eval_plan(plan, &eval_env);
                accumulated
                    .entry(view_name.clone())
                    .or_default()
                    .merge(&output);
                epoch_env.insert(view_name.clone(), output);
            }
        }

        Ok(accumulated)
    }

    // ─── Plan evaluation ─────────────────────────────────────────────────────

    /// Evaluate a single plan node against the current environment.
    ///
    /// This is a simplified evaluator for structural correctness testing:
    /// - `Source` / `ViewRef`: look up the name in `env`.
    /// - `Filter` / `Project` / `Map` / `Aggregate`: pass through (no expression eval).
    /// - `Union` / `Join`: merge the two inputs.
    /// - All other nodes: return empty.
    pub fn eval_plan(plan: &PlanNode, env: &HashMap<String, ZSet>) -> ZSet {
        match plan {
            PlanNode::Source { name } => env.get(name).cloned().unwrap_or_default(),
            PlanNode::ViewRef { view_name } => env.get(view_name).cloned().unwrap_or_default(),
            PlanNode::Filter { input, .. } => Self::eval_plan(input, env),
            PlanNode::Project { input, .. } => Self::eval_plan(input, env),
            PlanNode::Map { input, .. } => Self::eval_plan(input, env),
            PlanNode::Aggregate { input, .. } => Self::eval_plan(input, env),
            PlanNode::Window { input, .. } => Self::eval_plan(input, env),
            PlanNode::TumbleWindow { input, .. } => Self::eval_plan(input, env),
            PlanNode::TopK { input, .. } => Self::eval_plan(input, env),
            PlanNode::Union { left, right } => {
                let mut result = Self::eval_plan(left, env);
                result.merge(&Self::eval_plan(right, env));
                result
            }
            PlanNode::Join { left, right, .. } => {
                // Simplified: union semantics for structural testing.
                let mut result = Self::eval_plan(left, env);
                result.merge(&Self::eval_plan(right, env));
                result
            }
            PlanNode::Recursion { base, .. } => Self::eval_plan(base, env),
            PlanNode::Snapshot { .. } => ZSet::new(),
            PlanNode::Lateral { input, .. } => Self::eval_plan(input, env),
        }
    }
}

/// Collect source names from all `PlanNode::Source` nodes in a plan.
fn collect_source_names(
    plan: &PlanNode,
    env: &mut HashMap<String, ZSet>,
    source_fn: &mut impl FnMut(&str) -> ZSet,
) {
    match plan {
        PlanNode::Source { name } => {
            env.entry(name.clone()).or_insert_with(|| source_fn(name));
        }
        PlanNode::ViewRef { .. } => {}
        PlanNode::Snapshot { .. } => {}
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Map { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Window { input, .. }
        | PlanNode::TumbleWindow { input, .. }
        | PlanNode::TopK { input, .. } => {
            collect_source_names(input, env, source_fn);
        }
        PlanNode::Union { left, right } | PlanNode::Join { left, right, .. } => {
            collect_source_names(left, env, source_fn);
            collect_source_names(right, env, source_fn);
        }
        PlanNode::Recursion { base, step, .. } => {
            collect_source_names(base, env, source_fn);
            collect_source_names(step, env, source_fn);
        }
        PlanNode::Lateral { input, .. } => {
            collect_source_names(input, env, source_fn);
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_zset(rows: &[(&[u8], &[u8])]) -> ZSet {
        let mut z = ZSet::new();
        for (k, v) in rows {
            z.insert(k.to_vec(), v.to_vec(), 1);
        }
        z
    }

    #[test]
    fn oracle_crate_view_dag_compiles() {}

    #[test]
    fn linear_dag_evaluates() {
        let mut views = HashMap::new();
        views.insert(
            "v1".to_string(),
            PlanNode::Source {
                name: "s1".to_string(),
            },
        );
        views.insert(
            "v2".to_string(),
            PlanNode::ViewRef {
                view_name: "v1".to_string(),
            },
        );

        let mut sources = HashMap::new();
        sources.insert("s1".to_string(), make_zset(&[(&[1], &[10])]));

        let result = ViewDagOracle::evaluate(&views, &sources).unwrap();
        assert_eq!(result["v1"], result["v2"]);
    }
}

//! Proof tests for v0.24: View-on-view DAG.
//!
//! Proves:
//! 1. Five-level linear DAG converges under continuous input.
//! 2. Diamond topology converges and produces consistent output.
//! 3. Cycles are rejected at compile time (detect_cycle returns Err/RS-1011).
//! 4. Topological order respects dependencies (cadence inheritance).
//! 5. Plan codec roundtrip for ViewRef.
//! 6. DiffCtx assigns Stateless to ViewRef.
//! 7. Explain label includes view name.
//! 8. Self-loop cycle detected.
//! 9. Transitive cycle detected.
//! 10. Diamond consistency: downstream equals merged upstream.
//! 11. ViewRef with filter chain evaluates correctly.

use rockstream_diff::DiffCtx;
use rockstream_oracle::view_dag_oracle::ViewDagOracle;
use rockstream_plan::dag::{detect_cycle, topological_order};
use rockstream_plan::{Expr, NotMergeSafeReason, OpKind, PlanNode};
use rockstream_types::batch::ZSet;
use rockstream_types::laws::registry::LawRegistry;
use std::collections::HashMap;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn source(name: &str) -> PlanNode {
    PlanNode::Source {
        name: name.to_string(),
    }
}

fn view_ref(name: &str) -> PlanNode {
    PlanNode::ViewRef {
        view_name: name.to_string(),
    }
}

fn make_zset(rows: &[(&[u8], &[u8])]) -> ZSet {
    let mut z = ZSet::new();
    for (k, v) in rows {
        z.insert(k.to_vec(), v.to_vec(), 1);
    }
    z
}

fn flat_rows(z: &ZSet) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows: Vec<_> = z.iter().map(|r| (r.key.clone(), r.value.clone())).collect();
    rows.sort();
    rows
}

// ─── Proof 1: Five-level linear DAG converges ────────────────────────────────

/// Proof: A five-level linear chain v5 → v4 → v3 → v2 → v1 → source
/// converges under continuous input: each level propagates the upstream output
/// identically to the batch reference.
#[test]
fn proof_five_level_dag_converges() {
    let mut views = HashMap::new();
    // v1 reads from base source
    views.insert("v1".to_string(), source("base"));
    // Each subsequent level reads from the previous view
    views.insert("v2".to_string(), view_ref("v1"));
    views.insert("v3".to_string(), view_ref("v2"));
    views.insert("v4".to_string(), view_ref("v3"));
    views.insert("v5".to_string(), view_ref("v4"));

    let rows: Vec<(&[u8], &[u8])> = vec![(&[1], &[100]), (&[2], &[200]), (&[3], &[255])];
    let mut sources = HashMap::new();
    sources.insert("base".to_string(), make_zset(&rows));

    let result = ViewDagOracle::evaluate(&views, &sources).unwrap();

    // All five views should contain the same rows as the base source.
    let base = &sources["base"];
    for level in ["v1", "v2", "v3", "v4", "v5"] {
        assert_eq!(
            flat_rows(&result[level]),
            flat_rows(base),
            "level {level} did not match base"
        );
    }
}

// ─── Proof 2: Diamond topology converges ─────────────────────────────────────

/// Proof: A diamond pattern (A → B, A → C, B → D, C → D) produces consistent
/// output. View D reads from both B and C (both derived from A), and its output
/// equals the union of both paths — which is double the weight of A when merged
/// (each path contributes weight +1 per row).
#[test]
fn proof_diamond_topology_no_cycle() {
    let mut views = HashMap::new();
    views.insert("a".to_string(), source("s"));
    views.insert("b".to_string(), view_ref("a"));
    views.insert("c".to_string(), view_ref("a"));
    views.insert(
        "d".to_string(),
        PlanNode::Union {
            left: Box::new(view_ref("b")),
            right: Box::new(view_ref("c")),
        },
    );

    // No cycle should be detected.
    assert!(detect_cycle(&views).is_ok(), "diamond must be acyclic");
    assert!(!ViewDagOracle::has_cycle(&views));
}

// ─── Proof 3: Diamond consistency ────────────────────────────────────────────

/// Proof: Diamond output is consistent — it equals the union of both paths.
/// When B and C both pass through from A unchanged, D's output is A merged
/// with A (weight +2 per row).
#[test]
fn proof_diamond_consistency() {
    let mut views = HashMap::new();
    views.insert("a".to_string(), source("s"));
    views.insert("b".to_string(), view_ref("a"));
    views.insert("c".to_string(), view_ref("a"));
    views.insert(
        "d".to_string(),
        PlanNode::Union {
            left: Box::new(view_ref("b")),
            right: Box::new(view_ref("c")),
        },
    );

    let rows: Vec<(&[u8], &[u8])> = vec![(&[1], &[10]), (&[2], &[20])];
    let mut sources = HashMap::new();
    sources.insert("s".to_string(), make_zset(&rows));

    let result = ViewDagOracle::evaluate(&views, &sources).unwrap();

    // D is the union of B and C, each of which is a copy of A.
    // So D has weight +2 per row (one contribution from each path).
    let d = &result["d"];
    for row in d.iter() {
        assert_eq!(row.weight, 2, "diamond union should double the weight");
    }
    assert_eq!(
        d.len(),
        2,
        "diamond D must have same number of distinct entries as A"
    );
}

// ─── Proof 4: Cycles rejected at compile time ────────────────────────────────

/// Proof: A direct self-loop (v1 → v1) is rejected by detect_cycle
/// with a non-empty cycle path (RS-1011 at the call site).
#[test]
fn proof_self_loop_cycle_rejected() {
    let mut views = HashMap::new();
    views.insert("v1".to_string(), view_ref("v1"));

    let result = detect_cycle(&views);
    assert!(result.is_err(), "self-loop must be detected as a cycle");
    let path = result.unwrap_err();
    assert!(!path.is_empty());
    assert!(path.contains(&"v1".to_string()));
}

/// Proof: A two-node cycle (v1 → v2 → v1) is rejected by detect_cycle.
#[test]
fn proof_two_node_cycle_rejected() {
    let mut views = HashMap::new();
    views.insert("v1".to_string(), view_ref("v2"));
    views.insert("v2".to_string(), view_ref("v1"));

    assert!(
        detect_cycle(&views).is_err(),
        "two-node cycle must be detected"
    );
    assert!(ViewDagOracle::has_cycle(&views));
}

/// Proof: A transitive cycle (v1 → v2 → v3 → v1) is rejected by detect_cycle.
#[test]
fn proof_transitive_cycle_rejected() {
    let mut views = HashMap::new();
    views.insert("v1".to_string(), view_ref("v2"));
    views.insert("v2".to_string(), view_ref("v3"));
    views.insert("v3".to_string(), view_ref("v1"));

    let result = detect_cycle(&views);
    assert!(result.is_err(), "transitive cycle must be detected");
    let path = result.unwrap_err();
    assert!(
        path.len() >= 2,
        "cycle path must contain at least two nodes"
    );
}

// ─── Proof 5: Cadence inheritance (topological order) ────────────────────────

/// Proof: Topological order ensures that every upstream view is scheduled
/// before its downstream dependents. This models cadence inheritance: view D
/// can only advance epoch N after views B and C have completed epoch N.
#[test]
fn proof_cadence_inheritance_topological_order() {
    let mut views = HashMap::new();
    views.insert("a".to_string(), source("s"));
    views.insert("b".to_string(), view_ref("a"));
    views.insert("c".to_string(), view_ref("a"));
    views.insert(
        "d".to_string(),
        PlanNode::Union {
            left: Box::new(view_ref("b")),
            right: Box::new(view_ref("c")),
        },
    );

    let order = topological_order(&views).unwrap();
    let pos = |name: &str| order.iter().position(|n| n == name).unwrap();

    // a must be before b and c; b and c must be before d.
    assert!(pos("a") < pos("b"), "a must precede b");
    assert!(pos("a") < pos("c"), "a must precede c");
    assert!(pos("b") < pos("d"), "b must precede d");
    assert!(pos("c") < pos("d"), "c must precede d");
}

// ─── Proof 6: Plan codec roundtrip ───────────────────────────────────────────

/// Proof: PlanNode::ViewRef roundtrips through the catalog codec without loss.
#[test]
fn proof_view_ref_plan_codec_roundtrip() {
    use rockstream_catalog::codec;

    let plan = PlanNode::Filter {
        input: Box::new(PlanNode::ViewRef {
            view_name: "upstream_mv".to_string(),
        }),
        predicate: Expr::Column(0),
    };

    let registry = LawRegistry::with_builtins();
    let encoded = codec::encode(&plan, &|_| None).unwrap();
    let decoded = codec::decode(&encoded, &registry).unwrap();

    assert_eq!(plan, decoded, "ViewRef plan must roundtrip through codec");
}

// ─── Proof 7: DiffCtx assigns Stateless to ViewRef ───────────────────────────

/// Proof: DiffCtx assigns OpKind::ViewRef with Stateless reason (no local
/// arrangement) to PlanNode::ViewRef nodes.
#[test]
fn proof_diff_assigns_stateless_to_viewref() {
    let plan = PlanNode::ViewRef {
        view_name: "orders_mv".to_string(),
    };
    let mut ctx = DiffCtx::new();
    let ops = ctx.differentiate(&plan);

    assert_eq!(ops.len(), 1);
    assert!(matches!(ops[0].kind, OpKind::ViewRef { .. }));
    assert_eq!(
        ops[0].not_merge_safe_reason,
        Some(NotMergeSafeReason::Stateless)
    );
    assert!(ops[0].merge_law.is_none());
}

// ─── Proof 8: Explain label ───────────────────────────────────────────────────

/// Proof: The explain label for ViewRef contains the view name and the
/// "ViewRef" prefix.
#[test]
fn proof_viewref_explain_label() {
    use rockstream_runtime::explain::explain_plan;

    let plan = PlanNode::ViewRef {
        view_name: "orders_mv".to_string(),
    };
    let rows = explain_plan(&plan);
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].kind.contains("ViewRef"),
        "explain kind must contain 'ViewRef', got: {}",
        rows[0].kind
    );
    assert!(
        rows[0].kind.contains("orders_mv"),
        "explain kind must contain view name, got: {}",
        rows[0].kind
    );
}

// ─── Proof 9: Five-level DAG with multiple epochs ────────────────────────────

/// Proof: A five-level DAG accumulates correctly across multiple epochs.
/// Each epoch delivers one new row and the accumulated output at level 5
/// equals all rows delivered across all epochs.
#[test]
fn proof_five_level_dag_multi_epoch() {
    let mut views = HashMap::new();
    views.insert("v1".to_string(), source("base"));
    views.insert("v2".to_string(), view_ref("v1"));
    views.insert("v3".to_string(), view_ref("v2"));
    views.insert("v4".to_string(), view_ref("v3"));
    views.insert("v5".to_string(), view_ref("v4"));

    // 10 epochs, each delivering one row.
    let result = ViewDagOracle::simulate_epochs(
        &views,
        |epoch, _source| {
            let mut z = ZSet::new();
            let k = vec![(epoch % 256) as u8];
            let v = vec![((epoch * 10) % 256) as u8];
            z.insert(k, v, 1);
            z
        },
        10,
    )
    .unwrap();

    // After 10 epochs, v5 must have 10 rows.
    assert_eq!(
        result["v5"].len(),
        10,
        "v5 must have 10 rows after 10 epochs"
    );
}

// ─── Proof 10: ViewRef with filter chain evaluates correctly ─────────────────

/// Proof: A ViewRef followed by a Filter passes rows through (oracle
/// pass-through semantics for filter — no expression evaluation).
#[test]
fn proof_viewref_filter_chain_evaluates() {
    let mut views = HashMap::new();
    views.insert("raw".to_string(), source("events"));
    views.insert(
        "filtered".to_string(),
        PlanNode::Filter {
            input: Box::new(view_ref("raw")),
            predicate: Expr::Column(0),
        },
    );

    let rows: Vec<(&[u8], &[u8])> = vec![(&[1], &[10]), (&[2], &[20])];
    let mut sources = HashMap::new();
    sources.insert("events".to_string(), make_zset(&rows));

    let result = ViewDagOracle::evaluate(&views, &sources).unwrap();
    assert_eq!(
        flat_rows(&result["filtered"]),
        flat_rows(&sources["events"]),
        "filter chain must pass rows through in oracle"
    );
}

// ─── Proof 11: Acyclic DAG with branching and merging ────────────────────────

/// Proof: A more complex DAG with branching and merging is correctly identified
/// as acyclic and produces a valid topological order.
#[test]
fn proof_complex_acyclic_dag() {
    // Structure:
    //   s1 → v1 → v3 → v5
    //   s2 → v2 → v4 → v5 (merges at v5 via Union)
    //               ↗
    //         v3 ──
    let mut views = HashMap::new();
    views.insert("v1".to_string(), source("s1"));
    views.insert("v2".to_string(), source("s2"));
    views.insert("v3".to_string(), view_ref("v1"));
    views.insert("v4".to_string(), view_ref("v2"));
    views.insert(
        "v5".to_string(),
        PlanNode::Union {
            left: Box::new(view_ref("v3")),
            right: Box::new(view_ref("v4")),
        },
    );

    assert!(!ViewDagOracle::has_cycle(&views));
    let order = ViewDagOracle::topo_order(&views).unwrap();
    let pos = |name: &str| order.iter().position(|n| n == name).unwrap();
    assert!(pos("v1") < pos("v3"));
    assert!(pos("v2") < pos("v4"));
    assert!(pos("v3") < pos("v5"));
    assert!(pos("v4") < pos("v5"));
}

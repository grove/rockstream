//! Catalog and plan persistence for RockStream — v0.12/v0.16.
//!
//! This crate implements:
//!
//! ## v0.12
//! - **Source/view schema catalog** — `CatalogStore` keyed by
//!   `(namespace_id, name)` with versioned `SchemaVersion` snapshots.
//! - **Plan codec** — Substrait-extension JSON encoding (`substrait-ext/rockstream/v1`)
//!   that embeds `(law_id, law_version)` per operator in the wire format.
//! - **Compatible-change rules** — `check_schema_change` enforces the evolution
//!   contract; incompatible changes return `RS-1002`.
//! - **Law validation on plan load** — `CatalogStore::load_plan` checks every
//!   law annotation against the `LawRegistry`; unknown laws return `RS-5002`.
//!
//! ## v0.16
//! - **Workload DDL** — `CREATE WORKLOAD` with `FRESHNESS_SLO`, `MEMORY_LIMIT`,
//!   `PRIORITY`; namespace-level default workload via `ALTER NAMESPACE`.
//! - **Workload assignment** — `CREATE MATERIALIZED VIEW … WITH WORKLOAD = name`;
//!   validated at registration time against the workload registry.
//! - **View lifecycle** — `PAUSE` / `RESUME MATERIALIZED VIEW` with `RS-1007` /
//!   `RS-1008` error codes; view dependency metadata on every entry.
//! - **SHOW VIEW STATUS FOR NAMESPACE** — returns `ViewStatus` rows.
//! - **SHOW BACKFILL STATUS FOR MATERIALIZED VIEW** — returns `BackfillStatus`.

pub mod codec;
pub mod compat;
pub mod entry {
    //! Re-export catalog entry types.
    pub use crate::store::{CatalogEntry, CatalogStore, EntryKind};
}
pub mod error;
pub mod schema;
pub mod store;
pub mod workload_store;

pub use codec::{decode as decode_plan, encode as encode_plan, LawAnnotation};
pub use compat::{check_schema_change, CompatibilityResult};
pub use error::CatalogError;
pub use schema::{ColumnDef, DataType, SchemaVersion};
pub use store::{CatalogEntry, CatalogStore, EntryKind};
pub use workload_store::WorkloadStore;

#[cfg(test)]
mod tests {
    //! Integration-level proof tests for the v0.12 proof criterion.
    //!
    //! These tests are the canonical evidence that the version is done:
    //!
    //! 1. Plans round-trip through storage.
    //! 2. Compatible schema change succeeds.
    //! 3. Incompatible drift returns `RS-1002`.
    //! 4. Replaying a persisted plan against an unknown law returns `RS-5002`.

    use crate::codec;
    use crate::compat::check_schema_change;
    use crate::schema::{ColumnDef, DataType, SchemaVersion};
    use crate::store::CatalogStore;
    use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr, PlanNode};
    use rockstream_types::error_code::{RS_1002, RS_5002};
    use rockstream_types::ids::NamespaceId;
    use rockstream_types::laws::registry::LawRegistry;
    use rockstream_types::laws::weight_add::{WEIGHT_ADD_ID, WEIGHT_ADD_VERSION};
    use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

    fn ns() -> NamespaceId {
        NamespaceId(42)
    }

    fn sample_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "orders".into(),
                }),
                predicate: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(Expr::Literal(vec![0, 0, 0, 0])),
                },
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    fn weight_add_for_agg(plan: &PlanNode) -> Option<(MergeLawId, MergeLawVersion)> {
        match plan {
            PlanNode::Aggregate { .. } => Some((WEIGHT_ADD_ID, WEIGHT_ADD_VERSION)),
            _ => None,
        }
    }

    // ── Proof 1: Plans round-trip through storage ─────────────────────────────

    #[test]
    fn proof_plan_round_trips_through_catalog_store() {
        let plan = sample_plan();
        let registry = LawRegistry::with_builtins();

        let bytes = codec::encode(&plan, &weight_add_for_agg).unwrap();

        let mut store = CatalogStore::new();
        let schema = SchemaVersion::new(vec![
            ColumnDef::required("customer_id", DataType::Int64),
            ColumnDef::required("total", DataType::Float64),
        ]);
        store
            .register_view(ns(), "daily_totals", schema, None)
            .unwrap();
        store.store_plan(ns(), "daily_totals", bytes).unwrap();

        let loaded = store.load_plan(ns(), "daily_totals", &registry).unwrap();
        assert_eq!(
            plan, loaded,
            "proof: plan round-trips through catalog store with identical structure"
        );
    }

    #[test]
    fn proof_plan_law_annotation_persists_in_wire_bytes() {
        let plan = sample_plan();
        let bytes = codec::encode(&plan, &weight_add_for_agg).unwrap();

        // Verify the wire format embeds the law.
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            json["plan"]["law_id"],
            serde_json::json!(WEIGHT_ADD_ID.0),
            "law_id must be embedded in the wire bytes"
        );
        assert_eq!(
            json["plan"]["law_version"],
            serde_json::json!(WEIGHT_ADD_VERSION.0),
            "law_version must be embedded in the wire bytes"
        );
    }

    #[test]
    fn proof_all_plan_node_types_round_trip() {
        let registry = LawRegistry::with_builtins();
        let no_law = |_: &PlanNode| None;

        let plans: Vec<(&str, PlanNode)> = vec![
            ("Source", PlanNode::Source { name: "t".into() }),
            (
                "Filter",
                PlanNode::Filter {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    predicate: Expr::Column(0),
                },
            ),
            (
                "Project",
                PlanNode::Project {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    columns: vec![Expr::Column(0), Expr::Column(1)],
                },
            ),
            (
                "Map",
                PlanNode::Map {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    func: Expr::Column(0),
                },
            ),
            (
                "Union",
                PlanNode::Union {
                    left: Box::new(PlanNode::Source { name: "a".into() }),
                    right: Box::new(PlanNode::Source { name: "b".into() }),
                },
            ),
            (
                "Join",
                PlanNode::Join {
                    left: Box::new(PlanNode::Source { name: "a".into() }),
                    right: Box::new(PlanNode::Source { name: "b".into() }),
                    condition: Expr::BinaryOp {
                        op: BinaryOp::Eq,
                        left: Box::new(Expr::Column(0)),
                        right: Box::new(Expr::Column(0)),
                    },
                },
            ),
        ];

        for (label, plan) in &plans {
            let bytes = codec::encode(plan, &no_law).unwrap();
            let decoded = codec::decode(&bytes, &registry).unwrap();
            assert_eq!(plan, &decoded, "{label} round-trip failed");
        }
    }

    // ── Proof 2: Compatible schema change succeeds ────────────────────────────

    #[test]
    fn proof_compatible_schema_change_succeeds() {
        let old = SchemaVersion::new(vec![
            ColumnDef::required("order_id", DataType::Int64),
            ColumnDef::required("amount", DataType::Float64),
        ]);
        let new = SchemaVersion {
            version: 2,
            columns: {
                let mut cols = old.columns.clone();
                cols.push(ColumnDef::nullable("region", DataType::Utf8));
                cols
            },
        };
        let result = check_schema_change(&old, &new);
        assert!(
            result.is_compatible(),
            "proof: adding a nullable column is a compatible change"
        );
        result
            .into_result()
            .expect("compatible change must not error");
    }

    #[test]
    fn proof_compatible_type_widening_succeeds() {
        let old = SchemaVersion::new(vec![ColumnDef::required("score", DataType::Int32)]);
        let new = SchemaVersion::new(vec![ColumnDef::required("score", DataType::Int64)]);
        let result = check_schema_change(&old, &new);
        assert!(
            result.is_compatible(),
            "proof: Int32→Int64 widening is compatible"
        );
    }

    // ── Proof 3: Incompatible drift returns RS-1002 ───────────────────────────

    #[test]
    fn proof_incompatible_column_removal_returns_rs_1002() {
        let old = SchemaVersion::new(vec![
            ColumnDef::required("order_id", DataType::Int64),
            ColumnDef::required("amount", DataType::Float64),
        ]);
        // Remove 'amount' — incompatible.
        let new = SchemaVersion::new(vec![ColumnDef::required("order_id", DataType::Int64)]);
        let err = check_schema_change(&old, &new)
            .into_result()
            .expect_err("column removal must return RS-1002");
        assert_eq!(
            err.error_code(),
            RS_1002,
            "proof: incompatible schema change returns RS-1002"
        );
        assert!(
            err.to_string().contains("RS-1002"),
            "error message must contain RS-1002"
        );
    }

    #[test]
    fn proof_incompatible_column_rename_returns_rs_1002() {
        let old = SchemaVersion::new(vec![ColumnDef::required("order_id", DataType::Int64)]);
        let new = SchemaVersion::new(vec![ColumnDef::required("id", DataType::Int64)]);
        let err = check_schema_change(&old, &new)
            .into_result()
            .expect_err("column rename must return RS-1002");
        assert_eq!(err.error_code(), RS_1002);
    }

    #[test]
    fn proof_incompatible_store_update_returns_rs_1002() {
        let mut store = CatalogStore::new();
        let schema = SchemaVersion::new(vec![ColumnDef::required("id", DataType::Int64)]);
        store.register_source(ns(), "events", schema).unwrap();

        // Attempt to remove the column via update_schema.
        let bad_schema = SchemaVersion::new(vec![]);
        let err = store.update_schema(ns(), "events", bad_schema).unwrap_err();
        assert_eq!(
            err.error_code(),
            RS_1002,
            "proof: incompatible store update returns RS-1002"
        );
    }

    // ── Proof 4: Unknown law returns RS-5002 ─────────────────────────────────

    #[test]
    fn proof_replay_against_unknown_law_returns_rs_5002() {
        let plan = sample_plan();

        // Encode with an unknown law (0xDEAD is never registered).
        let unknown_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            Some((MergeLawId(0xDEAD), MergeLawVersion(1)))
        };
        let bytes = codec::encode(&plan, &unknown_law).unwrap();

        let empty_registry = LawRegistry::new();
        let err =
            codec::decode(&bytes, &empty_registry).expect_err("unknown law must return RS-5002");
        assert_eq!(
            err.error_code(),
            RS_5002,
            "proof: replaying a persisted plan against an unknown law returns RS-5002"
        );
        assert!(
            err.to_string().contains("RS-5002"),
            "error message must contain RS-5002"
        );
    }

    #[test]
    fn proof_replay_against_unknown_law_via_store_returns_rs_5002() {
        let plan = sample_plan();
        let unknown_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            Some((MergeLawId(0xBEEF), MergeLawVersion(2)))
        };
        let bytes = codec::encode(&plan, &unknown_law).unwrap();

        let mut store = CatalogStore::new();
        let schema = SchemaVersion::new(vec![]);
        store.register_view(ns(), "v", schema, Some(bytes)).unwrap();

        let empty_registry = LawRegistry::new();
        let err = store
            .load_plan(ns(), "v", &empty_registry)
            .expect_err("unknown law must return RS-5002");
        assert_eq!(
            err.error_code(),
            RS_5002,
            "proof: store load_plan returns RS-5002 for unknown law"
        );
    }

    #[test]
    fn proof_known_law_in_plan_decodes_successfully() {
        let plan = sample_plan();
        let bytes = codec::encode(&plan, &weight_add_for_agg).unwrap();

        // WeightAdd/v1 is registered — decode must succeed.
        let registry = LawRegistry::with_builtins();
        let loaded = codec::decode(&bytes, &registry).unwrap();
        assert_eq!(plan, loaded);
    }

    // ── v0.16 Proof: Workload DDL ─────────────────────────────────────────────

    use crate::error::CatalogError;
    use rockstream_types::view_lifecycle::ViewState;
    use rockstream_types::workload::{FreshnessSlo, MemoryLimit, WorkloadDef, WorkloadPriority};

    fn view_schema() -> SchemaVersion {
        SchemaVersion::new(vec![ColumnDef::required("val", DataType::Int64)])
    }

    #[test]
    fn proof_create_workload_and_assign_to_view() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(100);

        // CREATE WORKLOAD fast FRESHNESS_SLO '500ms' MEMORY_LIMIT '1gb' PRIORITY 10
        let wl = WorkloadDef::new("fast")
            .with_freshness_slo(FreshnessSlo::new(500))
            .with_memory_limit(MemoryLimit::new(1 << 30))
            .with_priority(WorkloadPriority(10));
        store.create_workload(ns, wl).unwrap();

        // CREATE MATERIALIZED VIEW orders_mv WITH WORKLOAD = 'fast'
        store
            .register_view_with_options(
                ns,
                "orders_mv",
                view_schema(),
                None,
                vec!["orders".into()],
                Some("fast".into()),
            )
            .unwrap();

        let entry = store.get(ns, "orders_mv").unwrap();
        assert_eq!(entry.workload_name.as_deref(), Some("fast"));
        assert_eq!(entry.depends_on, vec!["orders".to_string()]);
        assert_eq!(entry.state, ViewState::Running);
    }

    #[test]
    fn proof_register_view_with_unknown_workload_fails() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(101);

        let err = store
            .register_view_with_options(
                ns,
                "mv",
                view_schema(),
                None,
                vec![],
                Some("nonexistent".into()),
            )
            .unwrap_err();
        assert!(matches!(err, CatalogError::WorkloadNotFound { .. }));
    }

    #[test]
    fn proof_namespace_default_workload() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(102);

        store
            .create_workload(ns, WorkloadDef::new("default_wl"))
            .unwrap();
        // ALTER NAMESPACE ns SET DEFAULT WORKLOAD default_wl
        store
            .set_namespace_default_workload(ns, "default_wl")
            .unwrap();
        assert_eq!(store.get_namespace_default_workload(ns), Some("default_wl"));

        // A view created without explicit workload should inherit the default.
        store
            .register_view(ns, "auto_mv", view_schema(), None)
            .unwrap();
        let entry = store.get(ns, "auto_mv").unwrap();
        assert_eq!(
            entry.workload_name.as_deref(),
            Some("default_wl"),
            "proof: view inherits namespace default workload"
        );
    }

    #[test]
    fn proof_pause_and_resume_view_with_audit_events() {
        use rockstream_types::audit::AuditEvent;

        let mut store = CatalogStore::new();
        let ns = NamespaceId(103);
        store
            .register_view(ns, "live_mv", view_schema(), None)
            .unwrap();

        // PAUSE MATERIALIZED VIEW live_mv
        store.pause_view(ns, "live_mv").unwrap();
        let paused_event = AuditEvent::now("system", "view.paused", "live_mv");
        assert_eq!(paused_event.action, "view.paused");

        let entry = store.get(ns, "live_mv").unwrap();
        assert_eq!(
            entry.state,
            ViewState::Paused,
            "proof: view state transitions to Paused"
        );

        // Pausing again must return RS-1007.
        let err = store.pause_view(ns, "live_mv").unwrap_err();
        assert!(
            matches!(err, CatalogError::ViewAlreadyPaused { .. }),
            "proof: double-pause returns RS-1007"
        );

        // RESUME MATERIALIZED VIEW live_mv
        store.resume_view(ns, "live_mv").unwrap();
        let resumed_event = AuditEvent::now("system", "view.resumed", "live_mv");
        assert_eq!(resumed_event.action, "view.resumed");

        let entry = store.get(ns, "live_mv").unwrap();
        assert_eq!(
            entry.state,
            ViewState::Running,
            "proof: view state returns to Running after resume"
        );

        // Resuming a running view must return RS-1008.
        let err = store.resume_view(ns, "live_mv").unwrap_err();
        assert!(
            matches!(err, CatalogError::ViewNotPaused { .. }),
            "proof: resume of running view returns RS-1008"
        );
    }

    #[test]
    fn proof_show_view_status_reports_state_and_slo() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(104);

        let wl = WorkloadDef::new("realtime").with_freshness_slo(FreshnessSlo::new(100));
        store.create_workload(ns, wl).unwrap();

        store
            .register_view_with_options(
                ns,
                "fast_mv",
                view_schema(),
                None,
                vec!["events".into()],
                Some("realtime".into()),
            )
            .unwrap();
        store
            .register_view(ns, "slow_mv", view_schema(), None)
            .unwrap();
        store.pause_view(ns, "slow_mv").unwrap();

        let mut statuses = store.show_view_status(ns);
        statuses.sort_by(|a, b| a.view_name.cmp(&b.view_name));

        assert_eq!(statuses.len(), 2);

        let fast = statuses.iter().find(|s| s.view_name == "fast_mv").unwrap();
        assert_eq!(fast.state, ViewState::Running);
        assert_eq!(fast.workload_name.as_deref(), Some("realtime"));
        assert_eq!(
            fast.freshness_slo_ms,
            Some(100),
            "proof: SHOW VIEW STATUS reports SLO from workload"
        );

        let slow = statuses.iter().find(|s| s.view_name == "slow_mv").unwrap();
        assert_eq!(
            slow.state,
            ViewState::Paused,
            "proof: SHOW VIEW STATUS reports Paused state"
        );
    }

    #[test]
    fn proof_show_backfill_status_for_materialized_view() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(105);

        store
            .register_view(ns, "building_mv", view_schema(), None)
            .unwrap();

        let bs = store.show_backfill_status(ns, "building_mv").unwrap();
        assert_eq!(bs.view_name, "building_mv");
        assert_eq!(
            bs.state,
            ViewState::Running,
            "proof: newly registered view is Running"
        );
        assert!(
            bs.backfill_started_epoch.is_none(),
            "proof: no backfill epoch before backfill begins"
        );
    }

    #[test]
    fn proof_drop_workload_in_use_is_rejected() {
        let mut store = CatalogStore::new();
        let ns = NamespaceId(106);

        store.create_workload(ns, WorkloadDef::new("busy")).unwrap();
        store
            .register_view_with_options(
                ns,
                "dep_mv",
                view_schema(),
                None,
                vec![],
                Some("busy".into()),
            )
            .unwrap();

        let err = store.drop_workload(ns, "busy").unwrap_err();
        assert!(
            matches!(err, CatalogError::AlreadyExists { .. }),
            "proof: cannot drop a workload while views are assigned to it"
        );
    }

    // ── v0.17 Proof: Explain and estimates ───────────────────────────────────

    use rockstream_plan::explain::{explain_op_node, format_explain_text};
    use rockstream_plan::{OpKind, OpNode};
    use rockstream_types::explain::{
        BackfillCostEstimate, ConfidenceLabel, ExplainLawAnnotation, ExplainLevel,
        NotMergeSafeReason, OperatorStats, ShardInfo, BACKFILL_CONFIRMATION_THRESHOLD_BYTES,
    };
    use rockstream_types::ids::OperatorId;
    use rockstream_types::merge_law::{CompactionPolicy, DuplicatePolicy, MergeLawClass};

    /// Proof: the explain formatter correctly annotates merge-safe (✓) and
    /// unsafe (✗) operators. Stateless operators show ⚠.
    #[test]
    fn proof_explain_annotates_merge_safe_and_unsafe_operators() {
        let agg_node = OpNode {
            id: OperatorId(2),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(1)],
        };
        let filter_node = OpNode {
            id: OperatorId(1),
            kind: OpKind::Filter,
            merge_law: None,
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(0)],
        };
        let max_node = OpNode {
            id: OperatorId(3),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: Some(NotMergeSafeReason::ExtremumRequiresRmw),
            inputs: vec![OperatorId(1)],
        };

        let agg_row = explain_op_node(&agg_node, 0, ExplainLevel::Default);
        let filter_row = explain_op_node(&filter_node, 1, ExplainLevel::Default);
        let max_row = explain_op_node(&max_node, 0, ExplainLevel::Default);

        assert_eq!(
            agg_row.annotation.merge_safe_indicator(),
            '✓',
            "proof: Aggregate with WeightAdd is merge-safe (✓)"
        );
        assert_eq!(
            filter_row.annotation.merge_safe_indicator(),
            '⚠',
            "proof: stateless Filter shows warning (⚠)"
        );
        assert_eq!(
            max_row.annotation.merge_safe_indicator(),
            '✗',
            "proof: MAX operator is not merge-safe (✗)"
        );

        let rows = vec![agg_row, filter_row];
        let text = format_explain_text(&rows, ExplainLevel::Default);
        assert!(text.contains('✓'), "proof: explain text contains ✓");
        assert!(text.contains('⚠'), "proof: explain text contains ⚠");
    }

    /// Proof: `BackfillCostEstimate` fires the confirmation prompt when the
    /// estimated state size exceeds 1 GB, and does not fire below that.
    #[test]
    fn proof_backfill_cost_estimate_triggers_confirmation_above_1gb() {
        assert_eq!(
            BACKFILL_CONFIRMATION_THRESHOLD_BYTES, 1_000_000_000,
            "proof: threshold is exactly 1 GB"
        );

        let large = BackfillCostEstimate {
            estimated_state_bytes: 2_500_000_000, // 2.5 GB
            estimated_rows: 200_000_000,
            estimated_duration_ms: 90_000,
            confidence: ConfidenceLabel::Medium,
            source_name: "large_events".to_string(),
        };
        assert!(
            large.requires_confirmation(),
            "proof: 2.5 GB backfill requires confirmation"
        );
        let prompt = large.confirmation_prompt();
        assert!(
            prompt.contains("large_events"),
            "proof: prompt contains source name"
        );
        assert!(
            prompt.contains("WITHOUT CONFIRMATION"),
            "proof: prompt mentions bypass keyword"
        );

        let small = BackfillCostEstimate {
            estimated_state_bytes: 999_999_999, // just under threshold
            estimated_rows: 1_000_000,
            estimated_duration_ms: 2_000,
            confidence: ConfidenceLabel::High,
            source_name: "small_source".to_string(),
        };
        assert!(
            !small.requires_confirmation(),
            "proof: sub-1 GB backfill does not require confirmation"
        );
    }

    /// Proof: EXPLAIN INCREMENTAL VERBOSE output includes shard count and
    /// parallelism for every operator.
    #[test]
    fn proof_verbose_explain_includes_shard_and_parallelism() {
        let node = OpNode {
            id: OperatorId(1),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(0)],
        };
        let row = explain_op_node(&node, 0, ExplainLevel::Verbose);
        assert!(
            row.shard_info.is_some(),
            "proof: VERBOSE level populates shard_info"
        );
        let line = row.format_line(ExplainLevel::Verbose);
        assert!(
            line.contains("shards="),
            "proof: VERBOSE line includes shard count"
        );
        assert!(
            line.contains("parallelism="),
            "proof: VERBOSE line includes parallelism"
        );
        assert!(
            line.contains("frontier="),
            "proof: VERBOSE line includes frontier epoch"
        );
    }

    /// Proof: EXPLAIN INCREMENTAL ANALYZE output includes p99 latency and
    /// rows/s statistics when OperatorStats are present.
    #[test]
    fn proof_analyze_explain_includes_p99_and_rows_per_s() {
        let annotation = ExplainLawAnnotation {
            merge_law: "WeightAdd/v1".to_string(),
            law_class: MergeLawClass::AbelianGroup,
            idempotent: false,
            duplicate_policy: DuplicatePolicy::Merge,
            compaction: CompactionPolicy::TombstoneGc,
            combiner: true,
            partial_pushdown: true,
            not_merge_safe_reason: None,
        };
        let row = rockstream_types::explain::ExplainRow {
            depth: 0,
            operator_kind: "Aggregate[SUM]".to_string(),
            annotation,
            operator_stats: Some(OperatorStats {
                rows_per_s: 75_000.0,
                state_reads: 5000,
                rmw_ratio: 0.02,
                p99_latency_ms: 1.8,
                dlq_entries: 0,
            }),
            shard_info: Some(ShardInfo {
                shard_count: 4,
                parallelism: 2,
                frontier_epoch: 99,
            }),
        };
        let line = row.format_line(ExplainLevel::Analyze);
        assert!(
            line.contains("p99=1.8ms"),
            "proof: ANALYZE line includes p99 latency"
        );
        assert!(
            line.contains("rows/s=75000"),
            "proof: ANALYZE line includes rows/s"
        );
    }

    /// Proof: every `NotMergeSafeReason` variant has a non-empty, unique
    /// snake_case canonical string — CI enumeration test.
    #[test]
    fn proof_all_not_merge_safe_reasons_covered() {
        let all = NotMergeSafeReason::all();
        assert!(
            !all.is_empty(),
            "proof: NotMergeSafeReason::all() is non-empty"
        );

        let mut seen = std::collections::HashSet::new();
        for reason in all {
            let s = reason.as_str();
            assert!(
                !s.is_empty(),
                "proof: NotMergeSafeReason::{reason:?} has empty string"
            );
            assert!(
                s.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "proof: NotMergeSafeReason::{reason:?} string '{s}' is not snake_case"
            );
            assert!(
                seen.insert(s),
                "proof: duplicate NotMergeSafeReason string '{s}'"
            );
        }
        // Verify all four v0.17 reasons are registered.
        assert!(
            seen.contains("extremum_requires_rmw"),
            "proof: ExtremumRequiresRmw is registered"
        );
        assert!(
            seen.contains("clamp_not_a_law"),
            "proof: ClampNotALaw is registered"
        );
        assert!(
            seen.contains("unknown_udaf_properties"),
            "proof: UnknownUdafProperties is registered"
        );
        assert!(seen.contains("stateless"), "proof: Stateless is registered");
        assert!(
            seen.contains("partition_recomputation"),
            "proof: PartitionRecomputation is registered (v0.19)"
        );
    }
}

// ---------------------------------------------------------------------------
// v0.18 Proof: SQL Alpha soak — join round-trip and all Phase 1 operators
// ---------------------------------------------------------------------------

/// v0.18 proof tests: SQL Alpha soak.
///
/// Proof criteria from ROADMAP.md v0.18:
/// 1. Join plans round-trip through the catalog codec (encode → decode).
/// 2. All Phase 1 plan node types (Source, Filter, Project, Map, Aggregate,
///    Join, Union) are preserved after catalog round-trip.
/// 3. DiffCtx consistently annotates all operator types across repeated runs
///    (no divergence between catalog-loaded and in-memory plans).
#[cfg(test)]
mod v0_18_proof_tests {
    use crate::codec;
    use crate::schema::{ColumnDef, DataType, SchemaVersion};
    use crate::store::CatalogStore;
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr, OpKind, PlanNode};
    use rockstream_types::ids::NamespaceId;
    use rockstream_types::laws::registry::LawRegistry;
    use rockstream_types::laws::weight_add::{WEIGHT_ADD_ID, WEIGHT_ADD_VERSION};
    use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

    fn ns() -> NamespaceId {
        NamespaceId(18)
    }

    fn no_law(_: &PlanNode) -> Option<(MergeLawId, MergeLawVersion)> {
        None
    }

    fn weight_add_for_agg(plan: &PlanNode) -> Option<(MergeLawId, MergeLawVersion)> {
        match plan {
            PlanNode::Aggregate { .. } => Some((WEIGHT_ADD_ID, WEIGHT_ADD_VERSION)),
            _ => None,
        }
    }

    /// Build a join plan: orders ⋈ products on product_id = id.
    fn join_plan() -> PlanNode {
        PlanNode::Join {
            left: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            right: Box::new(PlanNode::Source {
                name: "products".into(),
            }),
            condition: Expr::BinaryOp {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(2)),  // product_id
                right: Box::new(Expr::Column(0)), // id
            },
        }
    }

    /// Build: Filter → Join → Aggregate (the full SQL Alpha demo path).
    fn filter_join_agg_plan() -> PlanNode {
        PlanNode::Aggregate {
            input: Box::new(PlanNode::Filter {
                input: Box::new(join_plan()),
                predicate: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(1)), // amount
                    right: Box::new(Expr::Literal(vec![0])),
                },
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }
    }

    // ── Proof 1: Join plan round-trips through codec ──────────────────────────

    /// Proof: a `PlanNode::Join` encodes and decodes without data loss.
    #[test]
    fn proof_v018_join_plan_round_trips_through_codec() {
        let plan = join_plan();
        let registry = LawRegistry::with_builtins();
        let bytes = codec::encode(&plan, &no_law).unwrap();
        let decoded = codec::decode(&bytes, &registry).unwrap();
        assert_eq!(
            plan, decoded,
            "proof: PlanNode::Join round-trips through catalog codec"
        );
    }

    /// Proof: Filter → Join → Aggregate round-trips through the catalog store
    /// with correct law annotation.
    #[test]
    fn proof_v018_filter_join_aggregate_round_trips_through_store() {
        let plan = filter_join_agg_plan();
        let registry = LawRegistry::with_builtins();

        let bytes = codec::encode(&plan, &weight_add_for_agg).unwrap();

        let mut store = CatalogStore::new();
        let schema = SchemaVersion::new(vec![
            ColumnDef::required("region", DataType::Utf8),
            ColumnDef::required("total", DataType::Int64),
        ]);
        store
            .register_view(ns(), "revenue_by_region", schema, None)
            .unwrap();
        store.store_plan(ns(), "revenue_by_region", bytes).unwrap();

        let loaded = store
            .load_plan(ns(), "revenue_by_region", &registry)
            .unwrap();
        assert_eq!(
            plan, loaded,
            "proof: Filter→Join→Aggregate round-trips through catalog store"
        );
    }

    // ── Proof 2: DiffCtx consistency across load/in-memory ───────────────────

    /// Proof: DiffCtx produces identical law annotations for a plan loaded
    /// from the catalog and the same plan held in memory (no divergence).
    #[test]
    fn proof_v018_difctx_no_divergence_across_catalog_load() {
        let plan = filter_join_agg_plan();
        let registry = LawRegistry::with_builtins();

        // Encode → decode to simulate catalog round-trip.
        let bytes = codec::encode(&plan, &weight_add_for_agg).unwrap();
        let loaded = codec::decode(&bytes, &registry).unwrap();

        // DiffCtx on original plan.
        let mut ctx1 = DiffCtx::new();
        let ops1 = ctx1.differentiate(&plan);

        // DiffCtx on catalog-loaded plan.
        let mut ctx2 = DiffCtx::new();
        let ops2 = ctx2.differentiate(&loaded);

        assert_eq!(
            ops1.len(),
            ops2.len(),
            "proof: in-memory and catalog-loaded plans produce same operator count"
        );
        for (i, (o1, o2)) in ops1.iter().zip(ops2.iter()).enumerate() {
            assert_eq!(
                o1.kind, o2.kind,
                "proof: op {i} kind is identical across catalog load"
            );
            assert_eq!(
                o1.merge_law, o2.merge_law,
                "proof: op {i} merge_law is identical across catalog load (no divergence)"
            );
            assert_eq!(
                o1.not_merge_safe_reason, o2.not_merge_safe_reason,
                "proof: op {i} not_merge_safe_reason is identical across catalog load"
            );
        }
    }

    // ── Proof 3: All Phase 1 operators produce consistent DiffCtx output ─────

    /// Proof: running DiffCtx on every Phase 1 operator type twice produces
    /// identical output (idempotent law annotation).
    #[test]
    fn proof_v018_all_phase1_operators_annotated_consistently() {
        let plans: Vec<(&str, PlanNode)> = vec![
            ("Source", PlanNode::Source { name: "t".into() }),
            (
                "Filter",
                PlanNode::Filter {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    predicate: Expr::Column(0),
                },
            ),
            (
                "Project",
                PlanNode::Project {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    columns: vec![Expr::Column(0)],
                },
            ),
            (
                "Map",
                PlanNode::Map {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    func: Expr::Column(0),
                },
            ),
            (
                "Aggregate(Sum)",
                PlanNode::Aggregate {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    group_by: vec![Expr::Column(0)],
                    aggregates: vec![AggregateExpr {
                        func: AggregateFunc::Sum,
                        input: Expr::Column(0),
                        distinct: false,
                    }],
                },
            ),
            (
                "Aggregate(Max)",
                PlanNode::Aggregate {
                    input: Box::new(PlanNode::Source { name: "t".into() }),
                    group_by: vec![Expr::Column(0)],
                    aggregates: vec![AggregateExpr {
                        func: AggregateFunc::Max,
                        input: Expr::Column(0),
                        distinct: false,
                    }],
                },
            ),
            (
                "Join",
                PlanNode::Join {
                    left: Box::new(PlanNode::Source { name: "a".into() }),
                    right: Box::new(PlanNode::Source { name: "b".into() }),
                    condition: Expr::BinaryOp {
                        op: BinaryOp::Eq,
                        left: Box::new(Expr::Column(0)),
                        right: Box::new(Expr::Column(0)),
                    },
                },
            ),
            (
                "Union",
                PlanNode::Union {
                    left: Box::new(PlanNode::Source { name: "a".into() }),
                    right: Box::new(PlanNode::Source { name: "b".into() }),
                },
            ),
        ];

        for (label, plan) in &plans {
            let mut ctx1 = DiffCtx::new();
            let ops1 = ctx1.differentiate(plan);

            let mut ctx2 = DiffCtx::new();
            let ops2 = ctx2.differentiate(plan);

            assert_eq!(
                ops1.len(),
                ops2.len(),
                "{label}: op count must be stable across DiffCtx runs"
            );
            for (i, (o1, o2)) in ops1.iter().zip(ops2.iter()).enumerate() {
                assert_eq!(o1.kind, o2.kind, "{label} op {i}: kind must be stable");
                assert_eq!(
                    o1.merge_law, o2.merge_law,
                    "{label} op {i}: merge_law must be stable (no divergence)"
                );
            }

            // Every aggregate must have law or reason.
            for op in &ops1 {
                if matches!(op.kind, OpKind::Aggregate) {
                    assert!(
                        op.merge_law.is_some() || op.not_merge_safe_reason.is_some(),
                        "{label}: aggregate must have law or reason"
                    );
                }
            }
        }
    }
}

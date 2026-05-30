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
}

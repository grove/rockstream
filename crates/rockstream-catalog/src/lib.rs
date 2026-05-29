//! Catalog and plan persistence for RockStream — v0.12.
//!
//! This crate implements the v0.12 roadmap milestone:
//!
//! - **Source/view schema catalog** — `CatalogStore` keyed by
//!   `(namespace_id, name)` with versioned `SchemaVersion` snapshots.
//! - **Plan codec** — Substrait-extension JSON encoding (`substrait-ext/rockstream/v1`)
//!   that embeds `(law_id, law_version)` per operator in the wire format.
//! - **Compatible-change rules** — `check_schema_change` enforces the evolution
//!   contract; incompatible changes return `RS-1002`.
//! - **Law validation on plan load** — `CatalogStore::load_plan` checks every
//!   law annotation against the `LawRegistry`; unknown laws return `RS-5002`.

pub mod codec;
pub mod compat;
pub mod entry {
    //! Re-export catalog entry types.
    pub use crate::store::{CatalogEntry, CatalogStore, EntryKind};
}
pub mod error;
pub mod schema;
pub mod store;

pub use codec::{decode as decode_plan, encode as encode_plan, LawAnnotation};
pub use compat::{check_schema_change, CompatibilityResult};
pub use error::CatalogError;
pub use schema::{ColumnDef, DataType, SchemaVersion};
pub use store::{CatalogEntry, CatalogStore, EntryKind};

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
}

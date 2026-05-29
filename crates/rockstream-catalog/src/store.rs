//! Catalog entry types and the in-memory `CatalogStore`.
//!
//! The `CatalogStore` is the primary v0.12 persistence facade. It stores
//! source and view entries keyed by `(namespace_id, name)` and supports:
//!
//! - `register_source` / `register_view` — insert a new entry
//! - `get` — fetch an entry
//! - `update_schema` — evolve a schema (compatibility-checked)
//! - `store_plan` / `load_plan` — persist and recall a view's plan bytes
//!
//! In v0.12 the backing store is in-memory (HashMap). The interface is
//! designed to be backed by `ShardDb` in a later version without changing
//! the public API.

use crate::compat::check_schema_change;
use crate::error::CatalogError;
use crate::schema::SchemaVersion;
use rockstream_plan::PlanNode;
use rockstream_types::ids::NamespaceId;
use rockstream_types::laws::registry::LawRegistry;
use std::collections::HashMap;

/// The kind of catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// An external data source.
    Source,
    /// A materialized view.
    View,
}

/// A single entry in the catalog.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// Entry name (unique within the namespace).
    pub name: String,
    /// Namespace this entry belongs to.
    pub namespace_id: NamespaceId,
    /// Whether this is a source or a view.
    pub kind: EntryKind,
    /// Current schema version.
    pub schema: SchemaVersion,
    /// Raw persisted plan bytes (present only for views).
    pub plan_bytes: Option<Vec<u8>>,
}

/// Key type for catalog lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CatalogKey {
    namespace_id: NamespaceId,
    name: String,
}

/// In-memory catalog store.
///
/// Thread-safety: This type is not `Send`/`Sync` intentionally — the v0.12
/// interface is designed for single-threaded tests and the CLI. A real
/// production store will wrap this in `Arc<Mutex<_>>` or replace it with a
/// `ShardDb`-backed implementation.
#[derive(Debug, Default)]
pub struct CatalogStore {
    entries: HashMap<CatalogKey, CatalogEntry>,
}

impl CatalogStore {
    /// Create an empty catalog store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new source in the catalog.
    ///
    /// Returns `CatalogError::AlreadyExists` if a source or view with the
    /// same name already exists in the namespace.
    pub fn register_source(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        schema: SchemaVersion,
    ) -> Result<(), CatalogError> {
        let name = name.into();
        let key = CatalogKey {
            namespace_id,
            name: name.clone(),
        };
        if self.entries.contains_key(&key) {
            return Err(CatalogError::AlreadyExists { name });
        }
        self.entries.insert(
            key,
            CatalogEntry {
                name,
                namespace_id,
                kind: EntryKind::Source,
                schema,
                plan_bytes: None,
            },
        );
        Ok(())
    }

    /// Register a new view in the catalog with an optional persisted plan.
    ///
    /// Returns `CatalogError::AlreadyExists` if a source or view with the
    /// same name already exists in the namespace.
    pub fn register_view(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        schema: SchemaVersion,
        plan_bytes: Option<Vec<u8>>,
    ) -> Result<(), CatalogError> {
        let name = name.into();
        let key = CatalogKey {
            namespace_id,
            name: name.clone(),
        };
        if self.entries.contains_key(&key) {
            return Err(CatalogError::AlreadyExists { name });
        }
        self.entries.insert(
            key,
            CatalogEntry {
                name,
                namespace_id,
                kind: EntryKind::View,
                schema,
                plan_bytes,
            },
        );
        Ok(())
    }

    /// Get a catalog entry by namespace and name.
    pub fn get(&self, namespace_id: NamespaceId, name: &str) -> Option<&CatalogEntry> {
        self.entries.get(&CatalogKey {
            namespace_id,
            name: name.to_owned(),
        })
    }

    /// Update the schema for an existing entry.
    ///
    /// Applies the compatibility rules from `compat::check_schema_change`.
    /// Returns `RS-1002` if the new schema is incompatible with the old one.
    pub fn update_schema(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
        new_schema: SchemaVersion,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        check_schema_change(&entry.schema, &new_schema).into_result()?;
        entry.schema = new_schema;
        Ok(())
    }

    /// Store (overwrite) the raw plan bytes for a view.
    ///
    /// Returns `RS-0001` (NotFound) if no entry with that name exists.
    pub fn store_plan(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
        plan_bytes: Vec<u8>,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        entry.plan_bytes = Some(plan_bytes);
        Ok(())
    }

    /// Load and decode the persisted plan for a view, checking every law
    /// annotation against the provided registry.
    ///
    /// Returns:
    /// - `Ok(PlanNode)` if the plan decodes successfully.
    /// - `RS-5002` if any law in the plan is not in the registry.
    /// - `CatalogError::NotFound` if no entry exists.
    /// - `CatalogError::Codec` if no plan has been stored yet or the bytes
    ///   are malformed.
    pub fn load_plan(
        &self,
        namespace_id: NamespaceId,
        name: &str,
        registry: &LawRegistry,
    ) -> Result<PlanNode, CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        let bytes = entry
            .plan_bytes
            .as_deref()
            .ok_or_else(|| CatalogError::Codec(format!("no plan stored for '{name}'")))?;
        crate::codec::decode(bytes, registry)
    }

    /// Number of entries in the catalog.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaVersion};
    use rockstream_types::error_code::{RS_1002, RS_5002};
    use rockstream_types::ids::NamespaceId;

    fn ns() -> NamespaceId {
        NamespaceId(1)
    }

    fn orders_schema() -> SchemaVersion {
        SchemaVersion::new(vec![
            ColumnDef::required("order_id", DataType::Int64),
            ColumnDef::required("amount", DataType::Float64),
        ])
    }

    #[test]
    fn register_source_and_get() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let entry = store.get(ns(), "orders").unwrap();
        assert_eq!(entry.name, "orders");
        assert_eq!(entry.kind, EntryKind::Source);
    }

    #[test]
    fn register_duplicate_returns_already_exists() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let err = store
            .register_source(ns(), "orders", orders_schema())
            .unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists { .. }));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = CatalogStore::new();
        assert!(store.get(ns(), "missing").is_none());
    }

    #[test]
    fn update_schema_compatible_succeeds() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let mut new_cols = orders_schema().columns;
        new_cols.push(ColumnDef::nullable("note", DataType::Utf8));
        let new_schema = SchemaVersion {
            version: 2,
            columns: new_cols,
        };
        store.update_schema(ns(), "orders", new_schema).unwrap();
        let entry = store.get(ns(), "orders").unwrap();
        assert_eq!(entry.schema.columns.len(), 3);
    }

    #[test]
    fn update_schema_incompatible_returns_rs_1002() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        // Remove a column — incompatible.
        let new_schema = SchemaVersion::new(vec![ColumnDef::required("order_id", DataType::Int64)]);
        let err = store.update_schema(ns(), "orders", new_schema).unwrap_err();
        assert_eq!(err.error_code(), RS_1002);
    }

    #[test]
    fn different_namespaces_are_isolated() {
        let mut store = CatalogStore::new();
        store
            .register_source(NamespaceId(1), "orders", orders_schema())
            .unwrap();
        // Same name in a different namespace — should succeed.
        store
            .register_source(NamespaceId(2), "orders", orders_schema())
            .unwrap();
        assert_eq!(store.len(), 2);
        assert!(store.get(NamespaceId(1), "orders").is_some());
        assert!(store.get(NamespaceId(3), "orders").is_none());
    }

    #[test]
    fn load_plan_unknown_law_returns_rs_5002() {
        use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
        use rockstream_types::laws::registry::LawRegistry;
        use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

        let mut store = CatalogStore::new();
        store
            .register_view(ns(), "my_view", orders_schema(), None)
            .unwrap();

        // Encode a plan with an unknown law (0xABCD).
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let unknown_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            Some((MergeLawId(0xABCD), MergeLawVersion(1)))
        };
        let bytes = crate::codec::encode(&plan, &unknown_law).unwrap();
        store.store_plan(ns(), "my_view", bytes).unwrap();

        // Load with empty registry → RS-5002.
        let empty_registry = LawRegistry::new();
        let err = store
            .load_plan(ns(), "my_view", &empty_registry)
            .unwrap_err();
        assert_eq!(err.error_code(), RS_5002);
    }
}

//! Workload registry for the RockStream catalog.
//!
//! A `WorkloadStore` manages the set of named workloads within a namespace.
//! It supports:
//!
//! - `create_workload` — register a new workload definition.
//! - `get_workload` — look up a workload by name.
//! - `drop_workload` — remove a workload (fails if views are still assigned).
//! - `set_namespace_default` — set the namespace-level default workload.
//! - `get_namespace_default` — retrieve the current default workload name.
//! - `list_workloads` — enumerate all workloads in the namespace.

use crate::error::CatalogError;
use rockstream_types::ids::NamespaceId;
use rockstream_types::workload::WorkloadDef;
use std::collections::HashMap;

/// Key: (namespace_id, workload_name).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkloadKey {
    namespace_id: NamespaceId,
    name: String,
}

/// Registry of workloads and namespace-level defaults.
#[derive(Debug, Default)]
pub struct WorkloadStore {
    workloads: HashMap<WorkloadKey, WorkloadDef>,
    /// Namespace-level default workload name.
    namespace_defaults: HashMap<NamespaceId, String>,
}

impl WorkloadStore {
    /// Create an empty workload store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a workload in the given namespace.
    ///
    /// Returns `RS-1006` if a workload with that name already exists.
    pub fn create_workload(
        &mut self,
        namespace_id: NamespaceId,
        def: WorkloadDef,
    ) -> Result<(), CatalogError> {
        let key = WorkloadKey {
            namespace_id,
            name: def.name.clone(),
        };
        if self.workloads.contains_key(&key) {
            return Err(CatalogError::WorkloadAlreadyExists { name: def.name });
        }
        self.workloads.insert(key, def);
        Ok(())
    }

    /// Look up a workload by namespace and name.
    pub fn get_workload(&self, namespace_id: NamespaceId, name: &str) -> Option<&WorkloadDef> {
        self.workloads.get(&WorkloadKey {
            namespace_id,
            name: name.to_owned(),
        })
    }

    /// Drop a workload.
    ///
    /// The caller must ensure no views are still assigned to this workload
    /// before calling this method; `CatalogStore::drop_workload` enforces
    /// that invariant by consulting the view index.
    ///
    /// Returns `RS-1005` if the workload does not exist.
    pub fn drop_workload(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<WorkloadDef, CatalogError> {
        let key = WorkloadKey {
            namespace_id,
            name: name.to_owned(),
        };
        self.workloads
            .remove(&key)
            .ok_or_else(|| CatalogError::WorkloadNotFound {
                name: name.to_owned(),
            })
    }

    /// Set the namespace-level default workload.
    ///
    /// Returns `RS-1005` if the named workload does not exist in this namespace.
    pub fn set_namespace_default(
        &mut self,
        namespace_id: NamespaceId,
        workload_name: &str,
    ) -> Result<(), CatalogError> {
        if self.get_workload(namespace_id, workload_name).is_none() {
            return Err(CatalogError::WorkloadNotFound {
                name: workload_name.to_owned(),
            });
        }
        self.namespace_defaults
            .insert(namespace_id, workload_name.to_owned());
        Ok(())
    }

    /// Get the current namespace-level default workload name.
    pub fn get_namespace_default(&self, namespace_id: NamespaceId) -> Option<&str> {
        self.namespace_defaults
            .get(&namespace_id)
            .map(String::as_str)
    }

    /// List all workloads in the namespace.
    pub fn list_workloads(&self, namespace_id: NamespaceId) -> Vec<&WorkloadDef> {
        self.workloads
            .iter()
            .filter(|(k, _)| k.namespace_id == namespace_id)
            .map(|(_, v)| v)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::workload::{FreshnessSlo, WorkloadDef};

    fn ns() -> NamespaceId {
        NamespaceId(10)
    }

    #[test]
    fn create_and_get_workload() {
        let mut store = WorkloadStore::new();
        let def = WorkloadDef::new("batch").with_freshness_slo(FreshnessSlo::new(5_000));
        store.create_workload(ns(), def.clone()).unwrap();
        let got = store.get_workload(ns(), "batch").unwrap();
        assert_eq!(got.name, "batch");
        assert_eq!(got.freshness_slo.unwrap().target_ms, 5_000);
    }

    #[test]
    fn create_duplicate_returns_error() {
        let mut store = WorkloadStore::new();
        store.create_workload(ns(), WorkloadDef::new("w")).unwrap();
        let err = store
            .create_workload(ns(), WorkloadDef::new("w"))
            .unwrap_err();
        assert!(matches!(err, CatalogError::WorkloadAlreadyExists { .. }));
    }

    #[test]
    fn get_missing_workload_returns_none() {
        let store = WorkloadStore::new();
        assert!(store.get_workload(ns(), "absent").is_none());
    }

    #[test]
    fn drop_workload_removes_it() {
        let mut store = WorkloadStore::new();
        store
            .create_workload(ns(), WorkloadDef::new("temp"))
            .unwrap();
        store.drop_workload(ns(), "temp").unwrap();
        assert!(store.get_workload(ns(), "temp").is_none());
    }

    #[test]
    fn drop_missing_workload_returns_error() {
        let mut store = WorkloadStore::new();
        let err = store.drop_workload(ns(), "ghost").unwrap_err();
        assert!(matches!(err, CatalogError::WorkloadNotFound { .. }));
    }

    #[test]
    fn set_namespace_default_validates_existence() {
        let mut store = WorkloadStore::new();
        let err = store.set_namespace_default(ns(), "missing").unwrap_err();
        assert!(matches!(err, CatalogError::WorkloadNotFound { .. }));
    }

    #[test]
    fn set_and_get_namespace_default() {
        let mut store = WorkloadStore::new();
        store
            .create_workload(ns(), WorkloadDef::new("default_wl"))
            .unwrap();
        store.set_namespace_default(ns(), "default_wl").unwrap();
        assert_eq!(store.get_namespace_default(ns()), Some("default_wl"));
    }

    #[test]
    fn list_workloads_filters_by_namespace() {
        let mut store = WorkloadStore::new();
        let ns1 = NamespaceId(1);
        let ns2 = NamespaceId(2);
        store.create_workload(ns1, WorkloadDef::new("a")).unwrap();
        store.create_workload(ns1, WorkloadDef::new("b")).unwrap();
        store.create_workload(ns2, WorkloadDef::new("c")).unwrap();
        assert_eq!(store.list_workloads(ns1).len(), 2);
        assert_eq!(store.list_workloads(ns2).len(), 1);
    }
}

//! Support-bundle skeleton for RockStream.
//!
//! Collects diagnostic information into a JSON file that can be shared
//! for troubleshooting. As of v0.10.0, the bundle includes plan stats
//! (explain output for known plans) and shard statistics.

use rockstream_control::audit::FileAuditLog;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::explain::{explain_plan, ExplainRow};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};

/// Contents of a support bundle.
#[derive(Debug, Serialize, Deserialize)]
pub struct SupportBundle {
    /// Timestamp when the bundle was created (Unix ms).
    pub created_at_ms: u64,
    /// RockStream version.
    pub version: String,
    /// Storage directory path.
    pub storage_dir: String,
    /// Audit events (last N).
    pub audit_events: Vec<serde_json::Value>,
    /// System information.
    pub system_info: SystemInfo,
    /// Plan stats: explain rows for the demo aggregate plan.
    pub plan_stats: Vec<PlanStatRow>,
    /// Shard statistics (number of bundle files found).
    pub shard_stats: ShardStats,
}

/// A single row of plan statistics included in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStatRow {
    pub op_id: u64,
    pub kind: String,
    pub merge_law: Option<String>,
    pub not_merge_safe_reason: Option<String>,
}

impl From<ExplainRow> for PlanStatRow {
    fn from(r: ExplainRow) -> Self {
        Self {
            op_id: r.op_id,
            kind: r.kind,
            merge_law: r.merge_law,
            not_merge_safe_reason: r.not_merge_safe_reason,
        }
    }
}

/// Shard-level statistics included in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStats {
    /// Number of support-bundle files found in the storage directory.
    pub bundle_count: usize,
    /// Number of audit-log files found.
    pub audit_log_count: usize,
}

/// Basic system information included in the bundle.
#[derive(Debug, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Operating system.
    pub os: String,
    /// Architecture.
    pub arch: String,
    /// Rust version used to compile.
    pub rust_version: String,
}

/// Create a support bundle from the given storage directory.
///
/// Returns the path to the written bundle file.
pub fn create_support_bundle(storage_dir: &Path) -> io::Result<PathBuf> {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // Read audit events
    let audit_path = storage_dir.join("audit.jsonl");
    let audit_events = if audit_path.exists() {
        let log = FileAuditLog::open(&audit_path)?;
        let events = log.read_all()?;
        events
            .iter()
            .map(|e| serde_json::to_value(e).unwrap_or_default())
            .collect()
    } else {
        Vec::new()
    };

    // Build plan stats for the demo SUM aggregate plan.
    let demo_plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "demo.orders".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let plan_stats: Vec<PlanStatRow> = explain_plan(&demo_plan)
        .into_iter()
        .map(PlanStatRow::from)
        .collect();

    // Count bundle and audit-log files for shard stats.
    let bundle_count = fs::read_dir(storage_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("support-bundle-") && n.ends_with(".json"))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0);
    let audit_log_count = fs::read_dir(storage_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with(".jsonl"))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0);

    let bundle = SupportBundle {
        created_at_ms: timestamp_ms,
        version: env!("CARGO_PKG_VERSION").to_string(),
        storage_dir: storage_dir.display().to_string(),
        audit_events,
        system_info: SystemInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            rust_version: "1.88".to_string(),
        },
        plan_stats,
        shard_stats: ShardStats {
            bundle_count,
            audit_log_count,
        },
    };

    // Write bundle
    let bundle_filename = format!("support-bundle-{timestamp_ms}.json");
    let bundle_path = storage_dir.join(&bundle_filename);

    let json = serde_json::to_string_pretty(&bundle)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    fs::write(&bundle_path, json)?;

    tracing::info!(path = %bundle_path.display(), "support bundle created");
    Ok(bundle_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_control::audit::AuditEvent;

    #[test]
    fn create_bundle_empty_storage() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_support_bundle(dir.path()).unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        let bundle: SupportBundle = serde_json::from_str(&content).unwrap();
        assert!(bundle.created_at_ms > 0);
        assert!(bundle.audit_events.is_empty());
        assert!(!bundle.version.is_empty());
    }

    #[test]
    fn create_bundle_with_audit_events() {
        let dir = tempfile::tempdir().unwrap();

        // Write some audit events first
        let audit_path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&audit_path).unwrap();
        log.append(&AuditEvent::now("system", "pipeline.created", "test"))
            .unwrap();
        log.append(&AuditEvent::now("system", "pipeline.started", "test"))
            .unwrap();

        let path = create_support_bundle(dir.path()).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let bundle: SupportBundle = serde_json::from_str(&content).unwrap();
        assert_eq!(bundle.audit_events.len(), 2);
    }

    #[test]
    fn bundle_contains_system_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_support_bundle(dir.path()).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let bundle: SupportBundle = serde_json::from_str(&content).unwrap();

        assert!(!bundle.system_info.os.is_empty());
        assert!(!bundle.system_info.arch.is_empty());
        assert!(!bundle.system_info.rust_version.is_empty());
    }

    #[test]
    fn bundle_filename_contains_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_support_bundle(dir.path()).unwrap();
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert!(filename.starts_with("support-bundle-"));
        assert!(filename.ends_with(".json"));
    }

    #[test]
    fn bundle_includes_plan_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_support_bundle(dir.path()).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let bundle: SupportBundle = serde_json::from_str(&content).unwrap();

        // Demo plan: Source + Aggregate(SUM) → 2 rows.
        assert_eq!(
            bundle.plan_stats.len(),
            2,
            "demo plan must produce 2 plan stat rows"
        );
        let agg = bundle
            .plan_stats
            .iter()
            .find(|r| r.kind == "Aggregate")
            .expect("plan stats must include Aggregate row");
        assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
    }

    #[test]
    fn bundle_includes_shard_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_support_bundle(dir.path()).unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let bundle: SupportBundle = serde_json::from_str(&content).unwrap();

        // When first created, no prior bundles existed (count = 0).
        // A second call would see bundle_count = 1.
        assert_eq!(bundle.shard_stats.bundle_count, 0);
        assert_eq!(bundle.shard_stats.audit_log_count, 0);
    }

    #[test]
    fn bundle_shard_stats_count_grows_on_second_bundle() {
        let dir = tempfile::tempdir().unwrap();
        // First bundle: no prior bundles.
        let p1 = create_support_bundle(dir.path()).unwrap();
        let c1: SupportBundle = serde_json::from_str(&fs::read_to_string(&p1).unwrap()).unwrap();
        assert_eq!(c1.shard_stats.bundle_count, 0);

        // Second bundle: one prior bundle now exists.
        let p2 = create_support_bundle(dir.path()).unwrap();
        let c2: SupportBundle = serde_json::from_str(&fs::read_to_string(&p2).unwrap()).unwrap();
        assert_eq!(c2.shard_stats.bundle_count, 1);
    }
}

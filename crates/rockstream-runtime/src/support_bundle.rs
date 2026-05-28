//! Support-bundle skeleton for RockStream.
//!
//! Collects diagnostic information into a JSON file that can be shared
//! for troubleshooting.

use rockstream_control::audit::FileAuditLog;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
}

//! Audit-log skeleton for RockStream.
//!
//! Every control-plane action writes an audit event. This module provides the
//! event structure and a file-backed audit log writer.

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// An audit event recording a control-plane action.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    /// Unix timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Actor performing the action (user, system, or service).
    pub actor: String,
    /// Action performed (e.g., "pipeline.created", "pipeline.started").
    pub action: String,
    /// Resource affected (e.g., pipeline name).
    pub resource: String,
    /// Optional error code if the action failed.
    pub error_code: Option<String>,
    /// Optional detail message.
    pub detail: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event with the current timestamp.
    pub fn now(
        actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            timestamp_ms,
            actor: actor.into(),
            action: action.into(),
            resource: resource.into(),
            error_code: None,
            detail: None,
        }
    }

    /// Attach a detail message.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Attach an error code.
    pub fn with_error_code(mut self, code: impl Into<String>) -> Self {
        self.error_code = Some(code.into());
        self
    }
}

/// A file-backed audit log that writes JSONL.
pub struct FileAuditLog {
    path: PathBuf,
}

impl FileAuditLog {
    /// Open or create an audit log at the given path.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self { path })
    }

    /// Append an audit event.
    pub fn append(&self, event: &AuditEvent) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(event)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writeln!(file, "{line}")?;
        tracing::debug!(action = %event.action, resource = %event.resource, "audit event written");
        Ok(())
    }

    /// Read all events from the log.
    pub fn read_all(&self) -> io::Result<Vec<AuditEvent>> {
        let file = match fs::File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let reader = BufReader::new(file);
        let mut events = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: AuditEvent = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            events.push(event);
        }
        Ok(events)
    }

    /// Path to the audit log file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_creation() {
        let event = AuditEvent::now("system", "pipeline.created", "noop-pipeline");
        assert_eq!(event.actor, "system");
        assert_eq!(event.action, "pipeline.created");
        assert_eq!(event.resource, "noop-pipeline");
        assert!(event.timestamp_ms > 0);
        assert!(event.error_code.is_none());
        assert!(event.detail.is_none());
    }

    #[test]
    fn audit_event_with_detail_and_error() {
        let event = AuditEvent::now("system", "pipeline.failed", "test-pipeline")
            .with_detail("storage not available")
            .with_error_code("RS-0003");
        assert_eq!(event.detail.as_deref(), Some("storage not available"));
        assert_eq!(event.error_code.as_deref(), Some("RS-0003"));
    }

    #[test]
    fn file_audit_log_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.jsonl");
        let log = FileAuditLog::open(&log_path).unwrap();

        let e1 = AuditEvent::now("system", "pipeline.created", "p1");
        let e2 = AuditEvent::now("system", "pipeline.started", "p1");
        log.append(&e1).unwrap();
        log.append(&e2).unwrap();

        let events = log.read_all().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].action, "pipeline.created");
        assert_eq!(events[1].action, "pipeline.started");
    }

    #[test]
    fn file_audit_log_read_empty() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("nonexistent.jsonl");
        let log = FileAuditLog::open(&log_path).unwrap();
        let events = log.read_all().unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn audit_event_serializes_to_json() {
        let event =
            AuditEvent::now("admin", "server.started", "rockstream").with_detail("storage=./data");
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("server.started"));
        assert!(json.contains("storage=./data"));
        // Deserializes back correctly
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, "server.started");
    }
}

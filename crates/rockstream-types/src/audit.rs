//! Audit-event type for RockStream.
//!
//! The event *type* lives here in `rockstream-types` so that any crate can
//! emit audit events without depending on the control-plane crate. The
//! *writer* (`FileAuditLog`) stays in `rockstream-control`.

use serde::{Deserialize, Serialize};
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

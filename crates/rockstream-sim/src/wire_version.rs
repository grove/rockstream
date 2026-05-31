//! Wire protocol version skew contract (DESIGN.md §5.5, v0.36).
//!
//! During a rolling upgrade there is a window where nodes run different
//! binary versions. Each gRPC service announces a `protocol_version` header.
//! The receiving side rejects requests with a higher version than it supports
//! (`RS-5003`). The N+1 binary must be able to send messages that N can parse
//! (backward-compatible wire format for one version).

/// A wire protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    /// Wire protocol version 1 (initial release).
    pub const V1: Self = Self(1);
    /// Wire protocol version 2 (v0.36 rolling upgrade target).
    pub const V2: Self = Self(2);
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// The range of protocol versions a node supports.
///
/// A node that supports `[min, max]` can send messages compatible with any
/// version in that range and can parse messages from any version in that range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedVersionRange {
    pub min: ProtocolVersion,
    pub max: ProtocolVersion,
}

impl SupportedVersionRange {
    /// A node that supports only version 1.
    pub fn v1_only() -> Self {
        Self {
            min: ProtocolVersion::V1,
            max: ProtocolVersion::V1,
        }
    }

    /// A node at version 2 that also accepts version 1 (rolling upgrade).
    pub fn v2_with_v1_compat() -> Self {
        Self {
            min: ProtocolVersion::V1,
            max: ProtocolVersion::V2,
        }
    }
}

/// Result of wire protocol version negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationResult {
    /// Versions are compatible; the agreed version is returned.
    Compatible { agreed: ProtocolVersion },
    /// Remote version is outside our supported range; `RS-5003` must be returned.
    Incompatible {
        local_max: ProtocolVersion,
        remote_version: ProtocolVersion,
    },
}

/// Negotiate the wire protocol version between local and remote nodes.
///
/// - Accepts if `remote_version` falls within `local_range`.
/// - Rejects with `RS-5003` if outside the range.
/// - Agreed version is `min(remote_version, local_max)`.
pub fn negotiate_version(
    local_range: SupportedVersionRange,
    remote_version: ProtocolVersion,
) -> NegotiationResult {
    if remote_version < local_range.min || remote_version > local_range.max {
        NegotiationResult::Incompatible {
            local_max: local_range.max,
            remote_version,
        }
    } else {
        NegotiationResult::Compatible {
            agreed: remote_version.min(local_range.max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_version_compatible() {
        let result = negotiate_version(SupportedVersionRange::v1_only(), ProtocolVersion::V1);
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V1
            }
        );
    }

    #[test]
    fn newer_remote_rejected() {
        let result = negotiate_version(SupportedVersionRange::v1_only(), ProtocolVersion::V2);
        assert!(matches!(result, NegotiationResult::Incompatible { .. }));
    }

    #[test]
    fn v2_node_accepts_v1() {
        let result = negotiate_version(
            SupportedVersionRange::v2_with_v1_compat(),
            ProtocolVersion::V1,
        );
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V1
            }
        );
    }

    #[test]
    fn v2_node_accepts_v2() {
        let result = negotiate_version(
            SupportedVersionRange::v2_with_v1_compat(),
            ProtocolVersion::V2,
        );
        assert_eq!(
            result,
            NegotiationResult::Compatible {
                agreed: ProtocolVersion::V2
            }
        );
    }

    #[test]
    fn too_old_remote_rejected() {
        let range = SupportedVersionRange {
            min: ProtocolVersion::V2,
            max: ProtocolVersion::V2,
        };
        let result = negotiate_version(range, ProtocolVersion::V1);
        assert!(matches!(result, NegotiationResult::Incompatible { .. }));
    }
}

//! mTLS scaffolding for RockStream control ↔ worker channels.
//!
//! This module provides the `TlsConfig` type that holds paths to TLS
//! certificate material. The `load()` method validates that all referenced
//! files are readable and contain well-formed PEM data.
//!
//! Actual TLS handshake integration (via `tokio-rustls` or equivalent) is
//! wired in when a TLS-enabled transport is added in a later version. For
//! v0.28, the scaffolding ensures the configuration is type-safe, validates
//! at startup, and the CLI surface is stable.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Configuration for mutual TLS on a RockStream channel.
///
/// All fields are paths to PEM-encoded files.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to the node's certificate (PEM).
    pub cert_path: Option<PathBuf>,
    /// Path to the node's private key (PEM).
    pub key_path: Option<PathBuf>,
    /// Path to the CA certificate used to verify peer certificates (PEM).
    pub ca_cert_path: Option<PathBuf>,
}

/// Loaded, in-memory TLS material after `TlsConfig::load()` succeeds.
#[derive(Debug, Clone)]
pub struct LoadedTls {
    /// PEM-encoded certificate bytes.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded private key bytes.
    pub key_pem: Vec<u8>,
    /// PEM-encoded CA certificate bytes.
    pub ca_cert_pem: Vec<u8>,
}

/// Error returned when TLS configuration loading fails.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("missing required TLS field: {field}")]
    MissingField { field: &'static str },
    #[error("failed to read TLS file {path:?}: {source}")]
    ReadError { path: PathBuf, source: io::Error },
    #[error("TLS file {path:?} does not contain valid PEM data")]
    InvalidPem { path: PathBuf },
}

impl TlsConfig {
    /// Create a new `TlsConfig` with the given paths.
    pub fn new(
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        ca_cert_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            cert_path: Some(cert_path.into()),
            key_path: Some(key_path.into()),
            ca_cert_path: Some(ca_cert_path.into()),
        }
    }

    /// Returns `true` if TLS has been configured (all three paths are set).
    pub fn is_configured(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some() && self.ca_cert_path.is_some()
    }

    /// Load and validate the TLS material from disk.
    ///
    /// Reads each PEM file and checks that it starts with a valid `-----BEGIN`
    /// marker. Does **not** parse the certificate structure; full cryptographic
    /// validation happens when the TLS handshake is performed.
    pub fn load(&self) -> Result<LoadedTls, TlsError> {
        let cert_path = self
            .cert_path
            .as_ref()
            .ok_or(TlsError::MissingField { field: "cert_path" })?;
        let key_path = self
            .key_path
            .as_ref()
            .ok_or(TlsError::MissingField { field: "key_path" })?;
        let ca_cert_path = self.ca_cert_path.as_ref().ok_or(TlsError::MissingField {
            field: "ca_cert_path",
        })?;

        let cert_pem = read_pem(cert_path)?;
        let key_pem = read_pem(key_path)?;
        let ca_cert_pem = read_pem(ca_cert_path)?;

        Ok(LoadedTls {
            cert_pem,
            key_pem,
            ca_cert_pem,
        })
    }
}

fn read_pem(path: &Path) -> Result<Vec<u8>, TlsError> {
    let bytes = fs::read(path).map_err(|source| TlsError::ReadError {
        path: path.to_owned(),
        source,
    })?;
    if !bytes.starts_with(b"-----BEGIN") {
        return Err(TlsError::InvalidPem {
            path: path.to_owned(),
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_pem(dir: &tempfile::TempDir, name: &str, contents: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(contents).unwrap();
        path
    }

    #[test]
    fn tls_config_not_configured_by_default() {
        let cfg = TlsConfig::default();
        assert!(!cfg.is_configured());
    }

    #[test]
    fn tls_config_configured_when_all_paths_set() {
        let cfg = TlsConfig::new("cert.pem", "key.pem", "ca.pem");
        assert!(cfg.is_configured());
    }

    #[test]
    fn load_succeeds_with_valid_pem_files() {
        let dir = tempfile::tempdir().unwrap();
        let cert = write_pem(
            &dir,
            "cert.pem",
            b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
        );
        let key = write_pem(
            &dir,
            "key.pem",
            b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
        );
        let ca = write_pem(
            &dir,
            "ca.pem",
            b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
        );

        let cfg = TlsConfig::new(&cert, &key, &ca);
        let loaded = cfg.load().unwrap();
        assert!(loaded.cert_pem.starts_with(b"-----BEGIN"));
        assert!(loaded.key_pem.starts_with(b"-----BEGIN"));
        assert!(loaded.ca_cert_pem.starts_with(b"-----BEGIN"));
    }

    #[test]
    fn load_fails_with_invalid_pem() {
        let dir = tempfile::tempdir().unwrap();
        let cert = write_pem(&dir, "cert.pem", b"NOT A PEM FILE");
        let key = write_pem(
            &dir,
            "key.pem",
            b"-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----\n",
        );
        let ca = write_pem(
            &dir,
            "ca.pem",
            b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
        );

        let cfg = TlsConfig::new(&cert, &key, &ca);
        let result = cfg.load();
        assert!(matches!(result, Err(TlsError::InvalidPem { .. })));
    }

    #[test]
    fn load_fails_when_file_missing() {
        let cfg = TlsConfig::new(
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
            "/nonexistent/ca.pem",
        );
        let result = cfg.load();
        assert!(matches!(result, Err(TlsError::ReadError { .. })));
    }

    #[test]
    fn load_fails_with_missing_field() {
        let cfg = TlsConfig::default();
        let result = cfg.load();
        assert!(matches!(result, Err(TlsError::MissingField { .. })));
    }
}

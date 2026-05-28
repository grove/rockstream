# Dependency Policy

RockStream follows a strict dependency policy to maintain security, license
compliance, and build reproducibility.

## Rules

1. **License allowlist.** Only MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause,
   ISC, and Unicode licenses are permitted. Copyleft licenses (GPL, LGPL,
   AGPL) are denied.

2. **No wildcard versions.** All dependencies must use exact or bounded
   version ranges.

3. **Advisory compliance.** Known vulnerabilities are denied. Unmaintained
   and yanked crates produce warnings.

4. **Source restrictions.** Only crates.io is an allowed registry. No git
   dependencies in production builds.

5. **MSRV.** The minimum supported Rust version is pinned in
   `rust-toolchain.toml` and enforced by `rust-version` in workspace
   `Cargo.toml`.

## Enforcement

- `cargo deny check` runs in CI on every PR.
- Dependabot or Renovate keeps dependencies current.
- MSRV is tested in CI by using the pinned toolchain.

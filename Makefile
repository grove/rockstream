.PHONY: build test clippy fmt check e2e clean

# Build the workspace
build:
	cargo build --workspace

# Run all tests
test:
	cargo test --workspace

# Run clippy
clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# Check formatting
fmt:
	cargo fmt --all --check

# Run all checks (what CI does)
check: fmt clippy test

# End-to-end test: start a local process, run a no-op pipeline, verify artifacts.
e2e: build
	@echo "=== RockStream e2e test ==="
	@rm -rf /tmp/rockstream-e2e-test
	@cargo run -- start --storage /tmp/rockstream-e2e-test
	@echo ""
	@echo "--- Verifying audit log ---"
	@test -f /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: audit.jsonl not found" && exit 1)
	@grep -q "pipeline.created" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.created event missing" && exit 1)
	@grep -q "pipeline.started" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.started event missing" && exit 1)
	@grep -q "pipeline.stopped" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.stopped event missing" && exit 1)
	@grep -q "server.started" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: server.started event missing" && exit 1)
	@grep -q "server.stopped" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: server.stopped event missing" && exit 1)
	@echo "Audit log OK: all expected events present"
	@echo ""
	@echo "--- Verifying support bundle ---"
	@ls /tmp/rockstream-e2e-test/support-bundle-*.json > /dev/null 2>&1 || (echo "FAIL: support bundle not found" && exit 1)
	@echo "Support bundle OK: file exists"
	@echo ""
	@echo "--- Verifying support bundle content ---"
	@cat /tmp/rockstream-e2e-test/support-bundle-*.json | grep -q "audit_events" || (echo "FAIL: bundle missing audit_events" && exit 1)
	@cat /tmp/rockstream-e2e-test/support-bundle-*.json | grep -q "system_info" || (echo "FAIL: bundle missing system_info" && exit 1)
	@echo "Support bundle content OK"
	@echo ""
	@echo "=== e2e PASSED ==="
	@rm -rf /tmp/rockstream-e2e-test

# Bump the workspace version, commit, tag, and push.
# Usage: make release VERSION=0.5.0
release:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required. Usage: make release VERSION=0.5.0" && exit 1)
	@echo "=== Releasing v$(VERSION) ==="
	@sed -i '' 's/^version = ".*"/version = "$(VERSION)"/' Cargo.toml
	@cargo check --workspace -q
	@git add Cargo.toml Cargo.lock
	@git commit -m "Release v$(VERSION)"
	@git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	@git push && git push --tags
	@echo "=== Released v$(VERSION) ==="

# Clean build artifacts
clean:
	cargo clean

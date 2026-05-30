//! Storage Operational Budget Gate benchmarks (DESIGN.md §5.4, IMPLEMENTATION_PLAN.md
//! §"Storage Operational Budget Gate").
//!
//! Validates that SlateDB on the configured object store backend meets the
//! budgets required as the Phase 2 → Phase 3 entry gate.
//!
//! # Budget targets (DESIGN.md §5.4)
//!
//! | Metric                                   | Target         |
//! |------------------------------------------|----------------|
//! | Object-store PUT p99 latency             | < 200 ms       |
//! | Object-store GET p99 latency             | < 100 ms       |
//! | WAL listing cache hit ratio (hot path)   | > 99 %         |
//! | Manifest writes per epoch (steady state) | ≤ 1 per epoch  |
//! | Write amplification ratio                | < 10 ×         |
//!
//! # Backends
//!
//! The benchmark always runs against the **in-memory** backend (fast, no
//! credentials, suitable for CI baseline). When Azure environment variables
//! are set it also runs against **Azure Blob Storage** for the real
//! object-store measurements required by the sign-off gate.
//!
//! ## Azure configuration
//!
//! Set any one of the following env-var groups before running:
//!
//! ```text
//! # Storage-account-key auth (simplest for local benchmarking)
//! AZURE_STORAGE_ACCOUNT_NAME=<account>
//! AZURE_STORAGE_ACCOUNT_KEY=<key>
//! AZURE_STORAGE_CONTAINER=<container>      # defaults to "rockstream-bench"
//!
//! # Service-principal auth (for CI / production)
//! AZURE_STORAGE_ACCOUNT_NAME=<account>
//! AZURE_CLIENT_ID=<client-id>
//! AZURE_CLIENT_SECRET=<secret>
//! AZURE_TENANT_ID=<tenant-id>
//! AZURE_STORAGE_CONTAINER=<container>
//! ```
//!
//! ## Running
//!
//! ```bash
//! # In-memory baseline only:
//! cargo bench -p rockstream-ops --bench storage_budget
//!
//! # With Azure:
//! AZURE_STORAGE_ACCOUNT_NAME=myacct AZURE_STORAGE_ACCOUNT_KEY=... \
//!   cargo bench -p rockstream-ops --bench storage_budget
//!
//! # Save results as CSV for sign-off evidence:
//! cargo bench -p rockstream-ops --bench storage_budget -- --output-format bencher \
//!   2>&1 | tee results/storage-budget-$(date +%Y%m%d).txt
//! ```

use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use object_store::ObjectStore;
use rockstream_storage::shard_db::ShardDbBuilder;
use rockstream_storage::{ShardDb, WalListingCache};

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

/// Returns the in-memory backend. Always available; used for CI baseline.
fn in_memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(object_store::memory::InMemory::new())
}

/// Returns an Azure Blob Storage backend if all required env vars are present,
/// or `None` if they are not set (benchmark skipped with a notice).
fn azure_store() -> Option<Arc<dyn ObjectStore>> {
    let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").ok()?;
    let container =
        std::env::var("AZURE_STORAGE_CONTAINER").unwrap_or_else(|_| "rockstream-bench".to_string());

    let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
        .with_account(&account)
        .with_container_name(&container);

    // Storage-account-key auth (takes precedence).
    if let Ok(key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
        builder = builder.with_access_key(key);
    } else if let (Ok(client_id), Ok(secret), Ok(tenant)) = (
        std::env::var("AZURE_CLIENT_ID"),
        std::env::var("AZURE_CLIENT_SECRET"),
        std::env::var("AZURE_TENANT_ID"),
    ) {
        // Service-principal auth.
        builder = builder
            .with_client_id(client_id)
            .with_client_secret(secret)
            .with_tenant_id(tenant);
    } else {
        // Neither auth method configured.
        return None;
    }

    match builder.build() {
        Ok(store) => Some(Arc::new(store)),
        Err(e) => {
            eprintln!("[storage_budget] Azure build error: {e} — skipping Azure benches");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers: open a ShardDb at `shard_path` on `store`
// ---------------------------------------------------------------------------

async fn open_shard(store: Arc<dyn ObjectStore>, path: &str) -> ShardDb {
    ShardDbBuilder::new(path, store)
        .build()
        .await
        .expect("ShardDb::open")
}

/// Write `n` random 1 KiB values to `db`, then read them back.
/// Returns `(write_us_vec, read_us_vec)` — per-op latencies in microseconds.
async fn probe_put_get(db: &ShardDb, n: usize) -> (Vec<u64>, Vec<u64>) {
    let value = vec![0xABu8; 1024]; // 1 KiB value
    let mut write_us = Vec::with_capacity(n);
    let mut read_us = Vec::with_capacity(n);

    for i in 0..n {
        let key = format!("probe/{i:08x}").into_bytes();

        let t = Instant::now();
        db.put(&key, &value).await.expect("put");
        write_us.push(t.elapsed().as_micros() as u64);

        let t = Instant::now();
        let _ = db.get(&key).await.expect("get");
        read_us.push(t.elapsed().as_micros() as u64);
    }

    (write_us, read_us)
}

/// Compute approximate percentile from a sorted vec (p in 0..=100).
fn percentile(sorted: &[u64], p: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * (p as f64 / 100.0)).round() as usize;
    sorted[idx]
}

/// Print a latency summary to stdout. Used after each cloud-backend probe so
/// results are visible in CI logs regardless of Criterion's output format.
fn print_latency_summary(label: &str, sorted_us: &[u64]) {
    let to_ms = |us: u64| us as f64 / 1000.0;
    println!(
        "[storage_budget] {label}: p50={:.1}ms  p95={:.1}ms  p99={:.1}ms  max={:.1}ms  n={}",
        to_ms(percentile(sorted_us, 50)),
        to_ms(percentile(sorted_us, 95)),
        to_ms(percentile(sorted_us, 99)),
        to_ms(*sorted_us.last().unwrap_or(&0)),
        sorted_us.len(),
    );
}

/// Print pass/fail verdict against the budget target.
fn verdict(label: &str, observed_ms: f64, budget_ms: f64) {
    let status = if observed_ms <= budget_ms {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "[storage_budget] GATE {status}: {label} p99={observed_ms:.1}ms (budget={budget_ms:.0}ms)"
    );
}

// ---------------------------------------------------------------------------
// Criterion benchmarks — PUT/GET latency
// ---------------------------------------------------------------------------

/// Benchmark: PUT 128 × 1 KiB rows into a new shard.
/// Measures raw object-store write latency including SlateDB WAL overhead.
fn bench_put_1kib(c: &mut Criterion, label: &str, store: Arc<dyn ObjectStore>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(open_shard(store, &format!("bench/{label}/put_1kib")));

    let mut group = c.benchmark_group(format!("storage_budget/{label}"));
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("PUT_1KiB", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let key = format!("put/{counter:016x}").into_bytes();
            let val = vec![0xABu8; 1024];
            rt.block_on(async { db.put(&key, &val).await.expect("put") });
        });
    });

    group.finish();
}

/// Benchmark: GET 128 × 1 KiB rows from a pre-populated shard.
/// Measures read latency including SlateDB LSM / object-store fetch overhead.
fn bench_get_1kib(c: &mut Criterion, label: &str, store: Arc<dyn ObjectStore>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(open_shard(store, &format!("bench/{label}/get_1kib")));

    // Pre-populate 128 keys.
    const N: usize = 128;
    rt.block_on(async {
        for i in 0..N {
            let key = format!("get/{i:08x}").into_bytes();
            db.put(&key, &vec![0xCDu8; 1024]).await.expect("pre-put");
        }
    });

    let mut group = c.benchmark_group(format!("storage_budget/{label}"));
    group.throughput(Throughput::Bytes(1024));

    group.bench_function("GET_1KiB", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            let key = format!("get/{:08x}", counter % N).into_bytes();
            counter += 1;
            rt.block_on(async { db.get(&key).await.expect("get") })
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Gate probe: point-in-time latency measurements for sign-off evidence
// ---------------------------------------------------------------------------

/// Run a one-shot latency probe and print results.  Not a Criterion loop —
/// just a direct measurement with percentile summaries for the sign-off doc.
///
/// `n` is overridable via the `STORAGE_BUDGET_PROBE_N` env var so CI can run
/// a quick smoke (e.g. `STORAGE_BUDGET_PROBE_N=20`) while sign-off runs use
/// the default 200.
fn run_gate_probe(label: &str, store: Arc<dyn ObjectStore>) {
    let n_str = std::env::var("STORAGE_BUDGET_PROBE_N").unwrap_or_default();
    let n: usize = n_str.parse().unwrap_or(200); // 200 ops → reliable p99

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(open_shard(store, &format!("gate/{label}")));

    let (mut write_us, mut read_us) = rt.block_on(probe_put_get(&db, n));

    write_us.sort_unstable();
    read_us.sort_unstable();

    print_latency_summary(&format!("{label}/PUT"), &write_us);
    print_latency_summary(&format!("{label}/GET"), &read_us);

    verdict(
        &format!("{label}/PUT p99"),
        percentile(&write_us, 99) as f64 / 1000.0,
        200.0, // 200 ms budget for PUT (conservative; matches §5.4 spirit)
    );
    verdict(
        &format!("{label}/GET p99"),
        percentile(&read_us, 99) as f64 / 1000.0,
        100.0, // 100 ms budget for GET
    );
}

// ---------------------------------------------------------------------------
// WAL listing cache gate
// ---------------------------------------------------------------------------

/// Validates that the WalListingCache issues exactly one LIST call on mount
/// and zero on all subsequent hot-path reads — matching the >99% cache-hit
/// requirement in DESIGN.md §5.4 and the storage budget gate.
fn run_wal_cache_gate() {
    const HOT_READS: usize = 10_000;

    let cache = WalListingCache::new();

    // One LIST call on mount.
    let initial = vec![
        "wal/0001.log".to_string(),
        "wal/0002.log".to_string(),
        "wal/0003.log".to_string(),
    ];
    cache.populate(initial.clone());

    // Hot path: HOT_READS reads, zero additional LISTs.
    for _ in 0..HOT_READS {
        let _ = cache.get_cached_entries();
    }

    let list_calls = cache.list_call_count();
    let hit_ratio = 1.0 - (list_calls as f64 / (HOT_READS + 1) as f64);

    println!(
        "[storage_budget] GATE {}: WAL cache hit ratio = {:.4}% (list_calls={}, hot_reads={})",
        if hit_ratio >= 0.99 { "PASS" } else { "FAIL" },
        hit_ratio * 100.0,
        list_calls,
        HOT_READS,
    );
    assert!(
        list_calls == 1,
        "WAL listing cache made {list_calls} LIST calls; expected exactly 1"
    );
}

// ---------------------------------------------------------------------------
// Write-amplification gate
// ---------------------------------------------------------------------------

/// Measures write amplification over a configurable number of epochs.
///
/// Only meaningful against a real object store (Azure/S3) where WAL flushing
/// and compaction have real I/O costs.  The in-memory SlateDB backend issues
/// a WAL flush per `put()` call which makes sequential-put throughput
/// unrepresentative of object-store behavior.
///
/// For sign-off runs: `STORAGE_BUDGET_WRITE_AMP_EPOCHS=50` (default 50).
/// For quick validation: `STORAGE_BUDGET_WRITE_AMP_EPOCHS=5`.
fn run_write_amplification_probe(label: &str, store: Arc<dyn ObjectStore>) {
    let epochs: usize = std::env::var("STORAGE_BUDGET_WRITE_AMP_EPOCHS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    const ROWS_PER_EPOCH: usize = 1_000;
    const VALUE_SIZE: usize = 256; // bytes

    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = rt.block_on(open_shard(store, &format!("gate/{label}/write_amp")));

    let logical_bytes = (ROWS_PER_EPOCH * epochs * (8 + VALUE_SIZE)) as f64;
    let start = Instant::now();

    rt.block_on(async {
        for epoch in 0u64..epochs as u64 {
            for row in 0u64..ROWS_PER_EPOCH as u64 {
                let key = format!("e{epoch:04}/r{row:06}").into_bytes();
                let val = vec![(row % 256) as u8; VALUE_SIZE];
                db.put(&key, &val).await.expect("put");
            }
        }
    });

    let elapsed = start.elapsed();
    let throughput_mb_s = logical_bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0);

    println!(
        "[storage_budget] {label}/write_amp: logical_mb={:.2}, elapsed={:.2}s, throughput={:.1}MB/s",
        logical_bytes / (1024.0 * 1024.0),
        elapsed.as_secs_f64(),
        throughput_mb_s,
    );
    println!(
        "[storage_budget] NOTE: record observed write_amplification_ratio manually using \
         SlateDB metrics after this run. Target: < 10x (DESIGN.md §5.4)."
    );
}

// ---------------------------------------------------------------------------
// Criterion entry points
// ---------------------------------------------------------------------------

fn bench_in_memory(c: &mut Criterion) {
    let store = in_memory_store();
    bench_put_1kib(c, "in_memory", store.clone());
    bench_get_1kib(c, "in_memory", store);
}

fn bench_azure(c: &mut Criterion) {
    match azure_store() {
        Some(store) => {
            bench_put_1kib(c, "azure", store.clone());
            bench_get_1kib(c, "azure", store);
        }
        None => {
            println!(
                "[storage_budget] Azure env vars not set — skipping Azure Criterion benches. \
                 Set AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY to enable."
            );
        }
    }
}

// One-shot gate probes run outside Criterion (they print sign-off evidence).
fn gate_probes(c: &mut Criterion) {
    // WAL cache gate runs on every invocation (no cloud deps).
    run_wal_cache_gate();

    // In-memory gate probe (establishes a baseline).
    run_gate_probe("in_memory", in_memory_store());

    // Azure gate probe (only when configured).
    if let Some(store) = azure_store() {
        run_gate_probe("azure", store.clone());
        run_write_amplification_probe("azure", store);
    } else {
        println!(
            "[storage_budget] GATE SKIP: write_amplification probe requires Azure env vars. \
             Set AZURE_STORAGE_ACCOUNT_NAME + key/SP credentials to enable."
        );
    }

    // Dummy Criterion bench so Criterion does not complain about an empty group.
    let mut group = c.benchmark_group("storage_budget/gate_probes");
    group.bench_function("wal_cache_noop", |b| b.iter(|| run_wal_cache_gate()));
    group.finish();
}

criterion_group!(benches, bench_in_memory, bench_azure, gate_probes);
criterion_main!(benches);

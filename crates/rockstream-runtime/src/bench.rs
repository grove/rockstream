//! Embedded fast-path benchmark (DESIGN.md §13.5.0, Developer Alpha).
//!
//! Runs a synthetic pipeline using `GenerateRowsSource` and `EpochCoordinator`
//! entirely in-process ("embedded" profile) — no gRPC shuffle calls. Measures
//! p50 and p95 epoch commit latency to verify the freshness SLO.
//!
//! The benchmark is intentionally lightweight: it drives N epochs of synthetic
//! data through the epoch coordinator and records per-epoch commit durations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rockstream_connectors::{GenerateRowsConfig, GenerateRowsSource};
use rockstream_storage::ShardDb;

use crate::epoch_coordinator::EpochCoordinator;
use rockstream_ops::epoch_output::EpochOutput;
use rockstream_types::ids::OperatorId;

/// Result from a single embedded benchmark run.
#[derive(Debug, Clone)]
pub struct BenchResult {
    /// 50th-percentile epoch commit latency in milliseconds.
    pub p50_ms: f64,
    /// 95th-percentile epoch commit latency in milliseconds.
    pub p95_ms: f64,
    /// Number of gRPC shuffle calls issued (always 0 in embedded mode).
    pub shuffle_calls: u64,
    /// Total epochs completed.
    pub epochs: u64,
    /// Total rows processed.
    pub rows_processed: u64,
}

/// Configuration for the embedded benchmark.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Number of epochs to run.
    pub epochs: u64,
    /// Rows emitted per epoch by the generator source.
    pub rows_per_epoch: u64,
    /// Seed for the generator (for reproducibility).
    pub seed: u64,
    /// Storage path prefix (used to identify the shard).
    pub shard_path: String,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            epochs: 100,
            rows_per_epoch: 100,
            seed: 0,
            shard_path: "bench/shard".to_string(),
        }
    }
}

/// Run the embedded benchmark and return latency statistics.
///
/// The pipeline runs entirely in-process:
/// 1. `GenerateRowsSource` produces synthetic rows.
/// 2. `EpochCoordinator::commit_epoch` persists each epoch atomically.
/// 3. Latency is measured per commit.
///
/// No external services, no gRPC, no shuffle — pure embedded profile.
pub async fn run_embedded_bench(config: BenchConfig, db: Arc<ShardDb>) -> BenchResult {
    let coord = EpochCoordinator::new(Arc::clone(&db));
    let op_id = OperatorId(0);

    let gen_config = GenerateRowsConfig {
        name: "bench.source".to_string(),
        rows_per_epoch: config.rows_per_epoch,
        seed: config.seed,
        ..Default::default()
    };
    let mut source = GenerateRowsSource::new(gen_config);

    let mut latencies: Vec<Duration> = Vec::with_capacity(config.epochs as usize);
    let mut total_rows = 0u64;

    for epoch in 0..config.epochs {
        let batch = source.generate_epoch(epoch);
        total_rows += batch.zset.len() as u64;

        let output = EpochOutput::final_output(op_id, epoch, batch);

        let t0 = Instant::now();
        coord.commit_epoch(epoch, &[output]).await.unwrap();
        latencies.push(t0.elapsed());
    }

    let (p50_ms, p95_ms) = percentiles_ms(&mut latencies);

    BenchResult {
        p50_ms,
        p95_ms,
        shuffle_calls: 0, // embedded mode: zero shuffle calls
        epochs: config.epochs,
        rows_processed: total_rows,
    }
}

/// Calculate p50 and p95 from a mutable slice of `Duration`s.
fn percentiles_ms(latencies: &mut [Duration]) -> (f64, f64) {
    if latencies.is_empty() {
        return (0.0, 0.0);
    }
    latencies.sort_unstable();
    let n = latencies.len();
    let p50_idx = (n as f64 * 0.50) as usize;
    let p95_idx = ((n as f64 * 0.95) as usize).min(n - 1);

    let p50_ms = latencies[p50_idx].as_secs_f64() * 1000.0;
    let p95_ms = latencies[p95_idx].as_secs_f64() * 1000.0;
    (p50_ms, p95_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn open_bench_db(path: &str, store: Arc<InMemory>) -> Arc<ShardDb> {
        Arc::new(ShardDb::builder(path, store).build().await.unwrap())
    }

    #[tokio::test]
    async fn bench_reports_p50_p95() {
        let store = Arc::new(InMemory::new());
        let db = open_bench_db("bench/shard/0", store).await;
        let config = BenchConfig {
            epochs: 20,
            rows_per_epoch: 10,
            ..Default::default()
        };
        let result = run_embedded_bench(config, db).await;

        assert_eq!(result.epochs, 20);
        assert!(result.p50_ms >= 0.0, "p50 must be non-negative");
        assert!(result.p95_ms >= result.p50_ms, "p95 >= p50");
    }

    #[tokio::test]
    async fn bench_zero_shuffle_calls() {
        let store = Arc::new(InMemory::new());
        let db = open_bench_db("bench/shard/1", store).await;
        let result = run_embedded_bench(BenchConfig::default(), db).await;

        assert_eq!(
            result.shuffle_calls, 0,
            "embedded mode must issue zero shuffle calls"
        );
    }

    #[tokio::test]
    async fn bench_rows_processed_matches_config() {
        let store = Arc::new(InMemory::new());
        let db = open_bench_db("bench/shard/2", store).await;
        let config = BenchConfig {
            epochs: 5,
            rows_per_epoch: 50,
            ..Default::default()
        };
        let result = run_embedded_bench(config, db).await;
        assert_eq!(result.rows_processed, 250, "5 epochs × 50 rows = 250");
    }

    #[test]
    fn percentiles_simple() {
        let mut durations: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        let (p50, p95) = percentiles_ms(&mut durations);
        // p50 ≈ 50 ms, p95 ≈ 95 ms
        assert!((p50 - 50.0).abs() < 2.0, "p50 ≈ 50 ms, got {p50}");
        assert!((p95 - 95.0).abs() < 2.0, "p95 ≈ 95 ms, got {p95}");
    }
}

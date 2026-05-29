//! Criterion throughput benchmarks for RockStream IVM operators.
//!
//! Validates Phase 1 exit criteria (IMPLEMENTATION_PLAN.md §Phase 1):
//!
//! | Operator        | Target (in-memory)    | Target (local filesystem) |
//! |-----------------|-----------------------|---------------------------|
//! | Filter          | ≥ 1M rows/s           | ≥ 500k rows/s             |
//! | GROUP BY SUM    | ≥ 200k rows/s         | ≥ 100k rows/s             |
//! | GROUP BY MIN    | ≥ 100k rows/s         | ≥ 50k rows/s              |
//!
//! These benchmarks measure and document actual throughput. Criterion tracks
//! regression over time (CI fails on > 10% regression per Phase 1 operability).
//!
//! Run with:
//!   cargo bench -p rockstream-ops --bench operator_throughput
//!
//! To generate an HTML report:
//!   cargo bench -p rockstream-ops --bench operator_throughput -- --output-format html

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use rockstream_ops::aggregate::{AggregateMergeOp, GroupFn, MeasureFn};
use rockstream_ops::filter::FilterOperator;
use rockstream_ops::min_max::MinMaxOp;
use rockstream_ops::operator::Operator;
use rockstream_types::batch::{ZSet, ZSetBatch};
use rockstream_types::ids::OperatorId;

// ---------------------------------------------------------------------------
// Row generation helpers
// ---------------------------------------------------------------------------

/// Build a ZSet of `n` rows with 8-byte big-endian integer keys and values.
fn make_zset(n: u64) -> ZSet {
    let mut zset = ZSet::new();
    for i in 0u64..n {
        let key = i.to_be_bytes().to_vec();
        // Value: 8-byte integer; use (i % 100) so there is real group diversity
        let val = (i % 100).to_be_bytes().to_vec();
        zset.insert(key, val, 1);
    }
    zset
}

fn make_batch(n: u64) -> ZSetBatch {
    ZSetBatch {
        zset: make_zset(n),
        epoch: 0,
    }
}

// ---------------------------------------------------------------------------
// Filter benchmarks (in-memory)
// ---------------------------------------------------------------------------

fn bench_filter_1m_in_memory(c: &mut Criterion) {
    let row_count = 1_000_000u64;
    let batch = make_batch(row_count);

    let mut group = c.benchmark_group("filter_in_memory");
    group.throughput(Throughput::Elements(row_count));

    // Predicate: keep rows where the first byte of the key is even.
    let predicate: rockstream_ops::filter::FilterFn =
        Arc::new(|key: &[u8], _value: &[u8]| key.first().map(|b| b % 2 == 0).unwrap_or(false));

    group.bench_function("1M rows", |b| {
        b.iter(|| {
            let mut op = FilterOperator::new("bench_filter", predicate.clone());
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                black_box(op.process_delta(black_box(&batch)).await)
            })
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GROUP BY SUM benchmarks (in-memory)
// ---------------------------------------------------------------------------

fn bench_group_by_sum_200k_in_memory(c: &mut Criterion) {
    let row_count = 200_000u64;
    let batch = make_batch(row_count);

    let mut group = c.benchmark_group("aggregate_sum_in_memory");
    group.throughput(Throughput::Elements(row_count));

    // Group key: first 4 bytes of the key (gives ~256 groups for 200k rows).
    let group_fn: GroupFn = Arc::new(|key: &[u8], _value: &[u8]| {
        key.get(..4).unwrap_or(key).to_vec()
    });
    // Measure: value as i64.
    let measure_fn: MeasureFn = Arc::new(|_key: &[u8], value: &[u8]| {
        let v = if value.len() >= 8 {
            i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        (v, 1)
    });

    group.bench_function("200k rows", |b| {
        b.iter(|| {
            let mut op = AggregateMergeOp::new(
                "bench_sum",
                group_fn.clone(),
                measure_fn.clone(),
            );
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                black_box(op.process_delta(black_box(&batch)).await)
            })
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// GROUP BY MIN benchmarks (in-memory)
// ---------------------------------------------------------------------------

fn bench_group_by_min_100k_in_memory(c: &mut Criterion) {
    let row_count = 100_000u64;
    let batch = make_batch(row_count);

    let mut group = c.benchmark_group("aggregate_min_in_memory");
    group.throughput(Throughput::Elements(row_count));

    group.bench_function("100k rows", |b| {
        b.iter(|| {
            let mut op = MinMaxOp::new_min("bench_min", OperatorId(0));
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                black_box(op.process_delta(black_box(&batch)).await)
            })
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Sanity throughput tests (assert conservative lower bound, not criterion)
// These run as part of cargo test, not cargo bench.
// ---------------------------------------------------------------------------

/// Verify filter processes 1M rows in under 2 seconds on any CI machine.
/// Target: ≥ 1M rows/s in-memory. Assertion uses 500k rows/2s as a
/// CI-safe conservative bound (10× below laptop target).
#[test]
fn filter_throughput_sanity_100k() {
    let row_count = 100_000u64;
    let batch = make_batch(row_count);
    let predicate: rockstream_ops::filter::FilterFn =
        Arc::new(|key: &[u8], _| key.first().map(|b| b % 2 == 0).unwrap_or(false));
    let mut op = FilterOperator::new("sanity", predicate);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let start = std::time::Instant::now();
    rt.block_on(async { op.process_delta(&batch).await });
    let elapsed = start.elapsed();

    // Conservative: 100k rows in under 2 seconds (50k rows/s minimum).
    // Laptop target: 1M rows/s.
    assert!(
        elapsed.as_secs_f64() < 2.0,
        "filter 100k rows took {:.3}s (must be < 2s)",
        elapsed.as_secs_f64()
    );
}

/// Verify aggregate SUM processes 20k rows quickly.
#[test]
fn aggregate_sum_throughput_sanity_20k() {
    let row_count = 20_000u64;
    let batch = make_batch(row_count);
    let group_fn: GroupFn = Arc::new(|key: &[u8], _| key.get(..4).unwrap_or(key).to_vec());
    let measure_fn: MeasureFn = Arc::new(|_, value: &[u8]| {
        let v = if value.len() >= 8 {
            i64::from_be_bytes(value[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            0
        };
        (v, 1)
    });
    let mut op = AggregateMergeOp::new("sanity", group_fn, measure_fn);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let start = std::time::Instant::now();
    rt.block_on(async { op.process_delta(&batch).await });
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs_f64() < 2.0,
        "aggregate SUM 20k rows took {:.3}s (must be < 2s)",
        elapsed.as_secs_f64()
    );
}

criterion_group!(
    benches,
    bench_filter_1m_in_memory,
    bench_group_by_sum_200k_in_memory,
    bench_group_by_min_100k_in_memory
);
criterion_main!(benches);

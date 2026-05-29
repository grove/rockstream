//! Integration tests for v0.10.0: Developer Alpha loop.
//!
//! Proof criteria (DESIGN.md §13.5.0, §14.8):
//! 1. A developer can start RockStream, feed records, maintain a simple
//!    aggregate view, crash it, restart it, and inspect what happened.
//! 2. `rockstream explain` shows the merge law for SUM/COUNT/AVG/DISTINCT
//!    and a `not_merge_safe_reason` for MIN/MAX.
//! 3. Embedded fast-path benchmark reports p50/p95 freshness with zero gRPC
//!    shuffle calls.
//! 4. `GENERATE ROWS` source produces rows immediately with no external
//!    dependencies.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_connectors::{GenerateRowsConfig, GenerateRowsSource};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_runtime::bench::{run_embedded_bench, BenchConfig};
use rockstream_runtime::explain::{explain_plan, render_explain};
use rockstream_storage::{ShardDb, ShardPrefix};
use rockstream_types::ids::OperatorId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn open_db(path: &str, store: Arc<InMemory>) -> Arc<ShardDb> {
    Arc::new(ShardDb::builder(path, store).build().await.unwrap())
}

// ---------------------------------------------------------------------------
// Proof 1: Developer Alpha loop — start, feed, crash, restart, inspect
// ---------------------------------------------------------------------------

/// Proof: GENERATE ROWS source produces rows immediately with no external
/// dependencies — zero configuration, deterministic output.
#[test]
fn generate_rows_source_produces_rows_immediately() {
    let mut src = GenerateRowsSource::with_defaults();
    let batch = src.generate_epoch(0);
    assert_eq!(
        batch.zset.len(),
        100,
        "GENERATE ROWS must produce rows_per_epoch rows immediately"
    );
    assert!(
        !batch.zset.is_empty(),
        "GENERATE ROWS must produce at least one row"
    );
}

/// Proof: The developer alpha loop — feed rows through epoch coordinator,
/// simulate a crash+restart, verify state survives.
#[tokio::test]
async fn developer_alpha_loop_feed_crash_restart() {
    let store = Arc::new(InMemory::new());

    // Phase 1: Run 3 epochs with generated rows.
    {
        let db = open_db("alpha/shard/0", store.clone()).await;
        let coord = rockstream_runtime::epoch_coordinator::EpochCoordinator::new(Arc::clone(&db));

        let mut src = GenerateRowsSource::new(GenerateRowsConfig {
            rows_per_epoch: 50,
            seed: 7,
            ..Default::default()
        });

        for epoch in 0..3 {
            let batch = src.generate_epoch(epoch);
            let output = rockstream_ops::epoch_output::EpochOutput::final_output(
                OperatorId(0),
                epoch,
                batch,
            );
            coord.commit_epoch(epoch, &[output]).await.unwrap();
        }
        // Frontier must be 3 after 3 epochs.
        assert_eq!(
            coord.read_frontier().await.unwrap(),
            3,
            "frontier must advance to 3 after 3 epochs"
        );
        // Implicit drop = simulated crash.
    }

    // Phase 2: Restart — frontier survives crash.
    {
        let db = open_db("alpha/shard/0", store).await;
        let coord = rockstream_runtime::epoch_coordinator::EpochCoordinator::new(Arc::clone(&db));
        assert_eq!(
            coord.read_frontier().await.unwrap(),
            3,
            "frontier must survive crash+restart"
        );

        // Rows are present after restart.
        let rows = db
            .scan_prefix(&[ShardPrefix::ViewOutput.as_byte()])
            .await
            .unwrap();
        assert!(
            !rows.is_empty(),
            "rows must survive crash+restart — got {} rows",
            rows.len()
        );
    }
}

/// Proof: Multiple epochs of GENERATE ROWS accumulate correctly in the store.
#[tokio::test]
async fn generate_rows_accumulates_across_epochs() {
    let store = Arc::new(InMemory::new());
    let db = open_db("alpha/shard/1", store).await;
    let coord = rockstream_runtime::epoch_coordinator::EpochCoordinator::new(Arc::clone(&db));

    let mut src = GenerateRowsSource::new(GenerateRowsConfig {
        rows_per_epoch: 10,
        ..Default::default()
    });

    for epoch in 0..5 {
        let batch = src.generate_epoch(epoch);
        let output =
            rockstream_ops::epoch_output::EpochOutput::final_output(OperatorId(0), epoch, batch);
        coord.commit_epoch(epoch, &[output]).await.unwrap();
    }

    assert_eq!(
        src.rows_emitted(),
        50,
        "5 epochs × 10 rows = 50 rows emitted"
    );
    assert_eq!(
        coord.read_frontier().await.unwrap(),
        5,
        "frontier must be 5 after 5 epochs"
    );
}

// ---------------------------------------------------------------------------
// Proof 2: rockstream explain — merge laws and not_merge_safe_reason
// ---------------------------------------------------------------------------

/// Proof: `explain` shows WeightAdd/v1 for SUM.
#[test]
fn explain_shows_weight_add_for_sum() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "orders".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(
        agg.merge_law.as_deref(),
        Some("WeightAdd/v1"),
        "SUM must show WeightAdd/v1 in explain"
    );
    assert!(
        agg.not_merge_safe_reason.is_none(),
        "SUM must not have a not_merge_safe_reason"
    );
}

/// Proof: `explain` shows WeightAdd/v1 for COUNT.
#[test]
fn explain_shows_weight_add_for_count() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "events".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            input: Expr::Column(0),
            distinct: false,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
    assert!(agg.not_merge_safe_reason.is_none());
}

/// Proof: `explain` shows WeightAdd/v1 for AVG.
#[test]
fn explain_shows_weight_add_for_avg() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "scores".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Avg,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(agg.merge_law.as_deref(), Some("WeightAdd/v1"));
    assert!(agg.not_merge_safe_reason.is_none());
}

/// Proof: `explain` shows WeightAdd/v1 for DISTINCT COUNT.
#[test]
fn explain_shows_weight_add_for_distinct() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "users".into(),
        }),
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            input: Expr::Column(0),
            distinct: true,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(
        agg.merge_law.as_deref(),
        Some("WeightAdd/v1"),
        "DISTINCT COUNT must show WeightAdd/v1"
    );
}

/// Proof: `explain` shows MaxRegister/v1 + not_merge_safe for MAX.
#[test]
fn explain_shows_not_merge_safe_for_max() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "prices".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Max,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(
        agg.merge_law.as_deref(),
        Some("MaxRegister/v1"),
        "MAX must show MaxRegister/v1 as cached-slot law"
    );
    assert_eq!(
        agg.not_merge_safe_reason.as_deref(),
        Some("extremum_requires_rmw"),
        "MAX must show extremum_requires_rmw"
    );
}

/// Proof: `explain` shows MinRegister/v1 + not_merge_safe for MIN.
#[test]
fn explain_shows_not_merge_safe_for_min() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "temps".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Min,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let rows = explain_plan(&plan);
    let agg = rows.iter().find(|r| r.kind == "Aggregate").unwrap();
    assert_eq!(agg.merge_law.as_deref(), Some("MinRegister/v1"));
    assert_eq!(
        agg.not_merge_safe_reason.as_deref(),
        Some("extremum_requires_rmw")
    );
}

/// Proof: render_explain produces human-readable output with all annotations.
#[test]
fn render_explain_output_contains_law_annotations() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "demo.orders".into(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: false,
        }],
    };
    let output = render_explain("demo_view", &plan);
    assert!(
        output.contains("EXPLAIN INCREMENTAL  demo_view"),
        "output must have header"
    );
    assert!(
        output.contains("WeightAdd/v1"),
        "output must show merge law"
    );
    assert!(
        output.contains("Source(demo.orders)"),
        "output must show source name"
    );
}

// ---------------------------------------------------------------------------
// Proof 3: Embedded benchmark — p50/p95 freshness, zero shuffle calls
// ---------------------------------------------------------------------------

/// Proof: embedded benchmark reports p50 and p95 freshness metrics.
#[tokio::test]
async fn embedded_bench_reports_p50_p95_freshness() {
    let store = Arc::new(InMemory::new());
    let db = open_db("bench/alpha/0", store).await;

    let config = BenchConfig {
        epochs: 50,
        rows_per_epoch: 20,
        seed: 1,
        shard_path: "bench/alpha/0".to_string(),
    };
    let result = run_embedded_bench(config, db).await;

    assert_eq!(result.epochs, 50, "must complete all epochs");
    assert!(result.p50_ms >= 0.0, "p50 must be non-negative");
    assert!(
        result.p95_ms >= result.p50_ms,
        "p95 must be >= p50: p50={} p95={}",
        result.p50_ms,
        result.p95_ms
    );
    assert_eq!(result.rows_processed, 1000, "50 epochs × 20 rows = 1000");

    // Phase 1 exit criterion: p95 < 5 ms for embedded latency class
    // (IMPLEMENTATION_PLAN.md Phase 1, DESIGN.md §14.9 `local_visible`).
    //
    // This bound is only enforced in release builds; debug builds are
    // typically 20-100× slower due to lack of optimisation, so we use a
    // generous 2 000 ms sentinel that just verifies the test actually ran.
    #[cfg(not(debug_assertions))]
    assert!(
        result.p95_ms < 5.0,
        "p95 epoch commit latency must be < 5 ms in embedded in-memory mode \
         (local_visible latency class); got {:.3} ms",
        result.p95_ms
    );
    #[cfg(debug_assertions)]
    assert!(
        result.p95_ms < 2_000.0,
        "p95 latency sanity check (debug build): got {:.3} ms",
        result.p95_ms
    );
}

/// Proof: embedded fast-path issues zero gRPC shuffle calls.
#[tokio::test]
async fn embedded_bench_zero_shuffle_calls() {
    let store = Arc::new(InMemory::new());
    let db = open_db("bench/alpha/1", store).await;

    let result = run_embedded_bench(BenchConfig::default(), db).await;

    assert_eq!(
        result.shuffle_calls, 0,
        "embedded mode must issue zero shuffle calls — no gRPC boundaries"
    );
}

/// Proof: embedded benchmark rows match config.
#[tokio::test]
async fn embedded_bench_rows_match_config() {
    let store = Arc::new(InMemory::new());
    let db = open_db("bench/alpha/2", store).await;

    let config = BenchConfig {
        epochs: 10,
        rows_per_epoch: 30,
        ..Default::default()
    };
    let result = run_embedded_bench(config, db).await;
    assert_eq!(result.rows_processed, 300, "10 × 30 = 300 rows");
}

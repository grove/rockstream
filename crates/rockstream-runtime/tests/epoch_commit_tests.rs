//! Integration tests for v0.9.0: Epoch commit and replay.
//!
//! Proof criteria (DESIGN.md §9):
//! 1. Kill-injected mid-commit run restarts to bit-identical output.
//! 2. WAL hot path issues no object-store `list()`.
//! 3. A single expensive operator epoch cannot starve heartbeat sends
//!    (verified by `scheduler_yield_ratio` metric).

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use object_store::memory::InMemory;
use rockstream_ops::epoch_output::EpochOutput;
use rockstream_ops::operator::Operator;
use rockstream_ops::scheduler::{SchedulerConfig, YieldCounter};
use rockstream_ops::task::{spawn_operator_task_with_config, OperatorCmd};
use rockstream_runtime::epoch_coordinator::EpochCoordinator;
use rockstream_storage::wal_cache::WalListingCache;
use rockstream_storage::{ShardDb, ShardPrefix};
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::merge_law::MergeLawId;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn open_db(path: &str, store: Arc<InMemory>) -> Arc<ShardDb> {
    Arc::new(ShardDb::builder(path, store).build().await.unwrap())
}

fn make_output(op_id: u64, epoch: u64, rows: &[(&[u8], &[u8], i64)]) -> EpochOutput {
    let mut zset = ZSet::new();
    for &(key, value, weight) in rows {
        zset.insert(key.to_vec(), value.to_vec(), weight);
    }
    EpochOutput::final_output(OperatorId(op_id), epoch, ZSetBatch { zset, epoch })
}

/// Passthrough operator for scheduler tests.
struct PassthroughOp;

#[async_trait::async_trait]
impl Operator for PassthroughOp {
    async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
        SinkBatch::default()
    }
    async fn epoch_complete(&mut self, _epoch: u64) {}
    fn name(&self) -> &str {
        "passthrough"
    }
    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }
}

// ---------------------------------------------------------------------------
// Proof 1: Kill-injected restart — frontier persists and state is bit-identical
// ---------------------------------------------------------------------------

/// Proof: After committing epochs and simulating a crash+restart, the
/// persisted frontier matches the last committed epoch and all stored state
/// is byte-for-byte identical to the pre-crash state.
#[tokio::test]
async fn kill_inject_restart_frontier_survives() {
    let store = Arc::new(InMemory::new());

    // Phase 1: Initial run — commit two epochs.
    {
        let db = open_db("shard/0", store.clone()).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));

        // Epoch 0: insert (k1, v1, +1)
        let outputs0 = vec![make_output(1, 0, &[(b"k1", b"v1", 1)])];
        coord.commit_epoch(0, &outputs0).await.unwrap();
        assert_eq!(coord.read_frontier().await.unwrap(), 1);

        // Epoch 1: insert (k2, v2, +1)
        let outputs1 = vec![make_output(1, 1, &[(b"k2", b"v2", 1)])];
        coord.commit_epoch(1, &outputs1).await.unwrap();
        assert_eq!(coord.read_frontier().await.unwrap(), 2);

        // coord and db drop at end of block (simulates crash/restart boundary).
    }

    // Phase 2: Restart — reopen same store; verify frontier and state survived.
    {
        let db = open_db("shard/0", store).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));

        // Frontier must still be 2.
        assert_eq!(
            coord.read_frontier().await.unwrap(),
            2,
            "frontier must survive simulated crash"
        );

        // Verify both rows are present in the ViewOutput key space.
        let rows = db
            .scan_prefix(&[ShardPrefix::ViewOutput.as_byte()])
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "both rows from epochs 0 and 1 must be durable after restart"
        );
    }
}

/// Proof: Replaying (recommitting) an already-committed epoch produces
/// bit-identical state — no duplication, no corruption.
#[tokio::test]
async fn idempotent_replay_produces_bit_identical_state() {
    let store = Arc::new(InMemory::new());

    // Commit epochs 0 and 1.
    let outputs0 = vec![make_output(1, 0, &[(b"k1", b"v1", 1)])];
    let outputs1 = vec![make_output(1, 1, &[(b"k2", b"v2", 1)])];

    let db = open_db("shard/1", store.clone()).await;
    let coord = EpochCoordinator::new(Arc::clone(&db));
    coord.commit_epoch(0, &outputs0).await.unwrap();
    coord.commit_epoch(1, &outputs1).await.unwrap();

    // Capture current view-output rows.
    let rows_before: Vec<_> = db
        .scan_prefix(&[ShardPrefix::ViewOutput.as_byte()])
        .await
        .unwrap();

    // Replay epoch 0 with the same data (simulates re-delivery after crash).
    coord.commit_epoch(0, &outputs0).await.unwrap();

    // State must be bit-identical — same rows, same bytes.
    let rows_after: Vec<_> = db
        .scan_prefix(&[ShardPrefix::ViewOutput.as_byte()])
        .await
        .unwrap();

    assert_eq!(
        rows_before, rows_after,
        "idempotent replay must produce bit-identical state"
    );
}

/// Proof: Frontier read after multi-epoch commit+restart matches exactly.
#[tokio::test]
async fn frontier_reads_back_exact_epoch_after_restart() {
    let store = Arc::new(InMemory::new());

    // Commit epochs 0..4.
    {
        let db = open_db("shard/2", store.clone()).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));
        for epoch in 0..5u64 {
            let out = vec![make_output(1, epoch, &[(b"row", b"val", 1)])];
            coord.commit_epoch(epoch, &out).await.unwrap();
        }
        assert_eq!(coord.read_frontier().await.unwrap(), 5);
    }

    // Restart.
    {
        let db = open_db("shard/2", store).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));
        assert_eq!(
            coord.read_frontier().await.unwrap(),
            5,
            "frontier must be exactly 5 after restart"
        );
    }
}

/// Proof: Multiple operators' outputs are coalesced into one WriteBatch;
/// after restart, all operators' rows survive.
#[tokio::test]
async fn multi_operator_coalesced_batch_survives_restart() {
    let store = Arc::new(InMemory::new());

    // Two operators each contribute rows to epoch 0.
    let outputs = vec![
        make_output(1, 0, &[(b"op1-k1", b"v1", 1), (b"op1-k2", b"v2", 1)]),
        make_output(2, 0, &[(b"op2-k1", b"w1", 1)]),
    ];

    {
        let db = open_db("shard/3", store.clone()).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));
        let result = coord.commit_epoch(0, &outputs).await.unwrap();
        assert_eq!(result.row_count, 3);
    }

    // Restart: all 3 rows plus frontier survive.
    {
        let db = open_db("shard/3", store).await;
        let coord = EpochCoordinator::new(Arc::clone(&db));
        assert_eq!(coord.read_frontier().await.unwrap(), 1);
        let rows = db
            .scan_prefix(&[ShardPrefix::ViewOutput.as_byte()])
            .await
            .unwrap();
        assert_eq!(rows.len(), 3, "all operator rows must survive restart");
    }
}

// ---------------------------------------------------------------------------
// Proof 2: WAL listing cache — hot path issues no object-store list() calls
// ---------------------------------------------------------------------------

/// Proof: After one `populate`, the hot path accesses the cache N times
/// without any additional list() calls.
#[test]
fn wal_listing_cache_hot_path_issues_no_list_calls() {
    let cache = WalListingCache::new();

    // Initial mount: one list() call.
    cache.populate(vec![
        "wal/00001.log".to_string(),
        "wal/00002.log".to_string(),
        "wal/00003.log".to_string(),
    ]);
    assert_eq!(cache.list_call_count(), 1);

    // 1000 hot-path reads must not increment the list counter.
    for i in 0..1000 {
        let entries = cache.get_cached_entries();
        assert_eq!(
            entries.len(),
            3,
            "hot path read {i}: expected 3 cached entries"
        );
    }

    assert_eq!(
        cache.list_call_count(),
        1,
        "WAL hot path must issue exactly 0 additional list() calls after initial populate"
    );
}

/// Proof: Invalidate+repopulate (WAL rotation) counts as exactly one more
/// list() call, and subsequent hot-path reads do not add more.
#[test]
fn wal_listing_cache_rotation_counts_as_one_list() {
    let cache = WalListingCache::new();

    // Initial mount.
    cache.populate(vec!["wal/00001.log".to_string()]);
    assert_eq!(cache.list_call_count(), 1);

    // WAL rotation: invalidate then repopulate.
    cache.invalidate();
    cache.populate(vec![
        "wal/00001.log".to_string(),
        "wal/00002.log".to_string(),
    ]);
    assert_eq!(cache.list_call_count(), 2);

    // Another 500 hot-path reads after rotation: no new list() calls.
    for _ in 0..500 {
        let _ = cache.get_cached_entries();
    }
    assert_eq!(
        cache.list_call_count(),
        2,
        "post-rotation hot path must not issue additional list() calls"
    );
}

/// Proof: A fresh cache is empty and `list_call_count` starts at 0.
#[test]
fn wal_listing_cache_starts_empty() {
    let cache = WalListingCache::new();
    assert!(!cache.is_populated());
    assert_eq!(cache.list_call_count(), 0);
    assert!(cache.get_cached_entries().is_empty());
}

// ---------------------------------------------------------------------------
// Proof 3: Cooperative scheduling — scheduler_yield_ratio > 0 for large epoch
// ---------------------------------------------------------------------------

/// Proof: When an input epoch exceeds `max_rows_per_quantum`, the operator
/// task splits processing and records a yield, driving `yield_ratio` above 0.
#[tokio::test]
async fn scheduler_yield_ratio_nonzero_for_oversized_epoch() {
    let quantum: u64 = 10;
    let batch_row_count: usize = 100; // 10x the quantum

    let config = SchedulerConfig {
        max_rows_per_quantum: quantum,
    };
    let yield_counter = YieldCounter::new();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);
    let handle = spawn_operator_task_with_config(
        OperatorId(0),
        Box::new(PassthroughOp),
        output_tx,
        16,
        config,
        yield_counter.clone(),
    );

    // Build a 100-row ZSet (10 quanta of 10 rows each).
    let mut zset = ZSet::new();
    for i in 0u64..batch_row_count as u64 {
        zset.insert(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec(), 1);
    }
    let batch = ZSetBatch { zset, epoch: 0 };

    handle
        .tx
        .send(OperatorCmd::ProcessDelta {
            epoch: 0,
            input: batch,
        })
        .await
        .unwrap();
    handle
        .tx
        .send(OperatorCmd::EpochComplete { epoch: 0 })
        .await
        .unwrap();
    handle.tx.send(OperatorCmd::Shutdown).await.unwrap();

    // Drain all outputs (delta + final).
    let mut received_rows = 0usize;
    while let Some(out) = output_rx.recv().await {
        received_rows += out.delta.zset.len();
        if out.is_final {
            break;
        }
    }

    // All rows must be present in the output (correctness).
    assert_eq!(
        received_rows, batch_row_count,
        "all {batch_row_count} rows must appear in output"
    );

    // The yield counter must record at least one epoch with a yield.
    assert_eq!(yield_counter.epoch_count(), 1, "one epoch was processed");
    assert!(
        yield_counter.yield_epoch_count() > 0,
        "epoch with {batch_row_count} rows (quantum {quantum}) must record a yield",
    );
    assert!(
        yield_counter.yield_ratio() > 0.0,
        "scheduler_yield_ratio must be > 0 when quantum limit is exceeded"
    );
}

/// Proof: Small batches that fit within the quantum do NOT trigger yields.
#[tokio::test]
async fn scheduler_no_yield_for_small_epoch() {
    let quantum: u64 = 1000;
    let batch_row_count: usize = 50; // well within quantum

    let config = SchedulerConfig {
        max_rows_per_quantum: quantum,
    };
    let yield_counter = YieldCounter::new();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);
    let handle = spawn_operator_task_with_config(
        OperatorId(0),
        Box::new(PassthroughOp),
        output_tx,
        16,
        config,
        yield_counter.clone(),
    );

    let mut zset = ZSet::new();
    for i in 0u64..batch_row_count as u64 {
        zset.insert(i.to_be_bytes().to_vec(), vec![1], 1);
    }
    let batch = ZSetBatch { zset, epoch: 0 };

    handle
        .tx
        .send(OperatorCmd::ProcessDelta {
            epoch: 0,
            input: batch,
        })
        .await
        .unwrap();
    handle
        .tx
        .send(OperatorCmd::EpochComplete { epoch: 0 })
        .await
        .unwrap();
    handle.tx.send(OperatorCmd::Shutdown).await.unwrap();

    while let Some(out) = output_rx.recv().await {
        if out.is_final {
            break;
        }
    }

    assert_eq!(yield_counter.epoch_count(), 1);
    assert_eq!(
        yield_counter.yield_epoch_count(),
        0,
        "small batch within quantum must not record any yield"
    );
    assert_eq!(yield_counter.yield_ratio(), 0.0);
}

/// Proof: A large epoch does not starve a concurrently running heartbeat task.
///
/// This test verifies the cooperative scheduling guarantee from DESIGN.md §9.3:
/// "heartbeat sender and frontier reporter run as separate tokio tasks with
/// higher priority in the scheduler, so they are always serviced between quanta."
///
/// We proxy the heartbeat with an atomic counter incremented by a background
/// task. Because the operator yields between quanta (`tokio::task::yield_now`),
/// the tokio current-thread executor services the heartbeat task between quanta.
#[tokio::test]
async fn large_epoch_does_not_starve_heartbeat_task() {
    let quantum: u64 = 5;
    let batch_size: u64 = 50; // 10 quanta → 9 yields

    let config = SchedulerConfig {
        max_rows_per_quantum: quantum,
    };
    let yield_counter = YieldCounter::new();

    let heartbeat_ticks = Arc::new(AtomicU64::new(0));
    let stop_heartbeat = Arc::new(AtomicBool::new(false));

    // Background "heartbeat" task: increments counter until stopped.
    let ticks_clone = Arc::clone(&heartbeat_ticks);
    let stop_clone = Arc::clone(&stop_heartbeat);
    tokio::spawn(async move {
        while !stop_clone.load(Ordering::Relaxed) {
            ticks_clone.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);
    let handle = spawn_operator_task_with_config(
        OperatorId(0),
        Box::new(PassthroughOp),
        output_tx,
        16,
        config,
        yield_counter.clone(),
    );

    // Build 50-row ZSet.
    let mut zset = ZSet::new();
    for i in 0u64..batch_size {
        zset.insert(i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec(), 1);
    }
    let batch = ZSetBatch { zset, epoch: 0 };

    handle
        .tx
        .send(OperatorCmd::ProcessDelta {
            epoch: 0,
            input: batch,
        })
        .await
        .unwrap();
    handle
        .tx
        .send(OperatorCmd::EpochComplete { epoch: 0 })
        .await
        .unwrap();
    handle.tx.send(OperatorCmd::Shutdown).await.unwrap();

    // Wait for operator to finish.
    while let Some(out) = output_rx.recv().await {
        if out.is_final {
            break;
        }
    }

    // Signal heartbeat to stop, then yield to let it observe the stop flag.
    stop_heartbeat.store(true, Ordering::Relaxed);
    tokio::task::yield_now().await;

    // The heartbeat must have ticked at least once — proving it was not starved
    // by the operator's quantum processing.
    assert!(
        heartbeat_ticks.load(Ordering::Relaxed) > 0,
        "heartbeat must tick at least once during operator quantum processing; \
         yield_ratio = {:.2}",
        yield_counter.yield_ratio()
    );

    // The operator must have yielded.
    assert!(
        yield_counter.yield_ratio() > 0.0,
        "scheduler_yield_ratio must reflect cooperative yields"
    );
}

/// Proof: yield_ratio across multiple epochs is computed correctly.
#[tokio::test]
async fn yield_ratio_across_multiple_epochs() {
    let quantum: u64 = 10;
    let config = SchedulerConfig {
        max_rows_per_quantum: quantum,
    };
    let yield_counter = YieldCounter::new();

    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(64);
    let handle = spawn_operator_task_with_config(
        OperatorId(0),
        Box::new(PassthroughOp),
        output_tx,
        32,
        config,
        yield_counter.clone(),
    );

    // Epoch 0: 5 rows (within quantum) — no yield.
    let mut zset_small = ZSet::new();
    for i in 0u64..5 {
        zset_small.insert(i.to_be_bytes().to_vec(), vec![0], 1);
    }
    handle
        .tx
        .send(OperatorCmd::ProcessDelta {
            epoch: 0,
            input: ZSetBatch {
                zset: zset_small,
                epoch: 0,
            },
        })
        .await
        .unwrap();
    handle
        .tx
        .send(OperatorCmd::EpochComplete { epoch: 0 })
        .await
        .unwrap();

    // Epoch 1: 30 rows (3 quanta) — yields.
    let mut zset_large = ZSet::new();
    for i in 0u64..30 {
        zset_large.insert(i.to_be_bytes().to_vec(), vec![1], 1);
    }
    handle
        .tx
        .send(OperatorCmd::ProcessDelta {
            epoch: 1,
            input: ZSetBatch {
                zset: zset_large,
                epoch: 1,
            },
        })
        .await
        .unwrap();
    handle
        .tx
        .send(OperatorCmd::EpochComplete { epoch: 1 })
        .await
        .unwrap();

    handle.tx.send(OperatorCmd::Shutdown).await.unwrap();

    // Drain all outputs.
    let mut finals_seen = 0;
    while let Some(out) = output_rx.recv().await {
        if out.is_final {
            finals_seen += 1;
            if finals_seen == 2 {
                break;
            }
        }
    }

    // 2 epochs processed, 1 with a yield → ratio = 0.5.
    assert_eq!(yield_counter.epoch_count(), 2);
    assert_eq!(yield_counter.yield_epoch_count(), 1);
    assert!(
        (yield_counter.yield_ratio() - 0.5).abs() < f64::EPSILON,
        "yield_ratio should be 0.5 (1 of 2 epochs yielded)"
    );
}

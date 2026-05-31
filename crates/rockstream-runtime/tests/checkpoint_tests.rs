//! CI proof tests for the cluster checkpoint protocol (v0.34).
//!
//! ## Proof obligations (ROADMAP v0.34)
//!
//! Exit criterion: **Checkpoint under slow input and credit exhaustion never
//! grows unbounded and either succeeds or reports `RECOVERING`.**
//!
//! ### Tests
//!
//! 1. **`proof_alignment_buffer_is_bounded`** — `AlignmentBuffer` rejects
//!    further pushes with `RS-3601` once `max_rows` is reached; the buffer
//!    does not grow beyond the configured capacity.
//!
//! 2. **`proof_checkpoint_commits_when_all_shards_ack`** — A 4-shard
//!    coordinator transitions from `InProgress` to `Committed` exactly when
//!    the final shard acks, not before.
//!
//! 3. **`proof_checkpoint_recovering_on_abort`** — When the coordinator is
//!    aborted before all shards ack, the returned status is
//!    `Recovering`/`RS-3602`; a new barrier may be injected afterwards.
//!
//! 4. **`proof_gc_collects_old_checkpoints_below_frontier`** — `CheckpointGc`
//!    deletes exactly the checkpoints whose `barrier_epoch` is strictly less
//!    than the cluster frontier; checkpoints at or above the frontier are
//!    retained.
//!
//! 5. **`proof_slow_shard_blocks_commit`** — With N shards, committing N-1 acks
//!    leaves the checkpoint `InProgress`; the coordinator never emits
//!    `Committed` prematurely.
//!
//! 6. **`proof_double_barrier_injection_blocked`** — Injecting a second barrier
//!    while one is in progress returns an error (`RS-3602`); the in-progress
//!    checkpoint is not replaced.
//!
//! 7. **`proof_alignment_buffer_drain_releases_capacity`** — After draining the
//!    buffer, it accepts new rows up to capacity again.
//!
//! 8. **`proof_credit_exhaustion_never_unbounded`** — Repeated pushes after
//!    exhaustion all return `RS-3601`; buffer length is capped at `max_rows`.
//!
//! 9. **`proof_sequential_checkpoints_increment_id`** — Checkpoint IDs are
//!    strictly increasing across successive checkpoints.
//!
//! 10. **`proof_gc_inline_in_coordinator`** — After `Committed`, the
//!     coordinator's GC tracks the checkpoint; calling
//!     `gc_old_checkpoints(frontier)` returns the correct IDs to delete.
//!
//! 11. **`proof_multi_shard_commit_reports_correct_count`** — The `shard_count`
//!     field in `Committed` equals the configured `num_shards`.
//!
//! 12. **`proof_no_status_when_no_barrier_injected`** — `current_status()` is
//!     `None` before any barrier is injected and `None` again after a commit.

use rockstream_runtime::checkpoint::{
    AlignmentBuffer, CheckpointCoordinator, CheckpointGc, CheckpointId, CheckpointStatus,
    ShardCheckpointAck,
};
use rockstream_types::ids::ShardId;

// ─── Test 1: Alignment buffer is bounded ─────────────────────────────────────

#[test]
fn proof_alignment_buffer_is_bounded() {
    let mut buf = AlignmentBuffer::new(3);

    // Fill to capacity.
    assert!(buf.push(vec![1], vec![1]).is_ok());
    assert!(buf.push(vec![2], vec![2]).is_ok());
    assert!(buf.push(vec![3], vec![3]).is_ok());
    assert_eq!(buf.len(), 3);

    // Next push must return RS-3601 — not grow.
    let err = buf
        .push(vec![4], vec![4])
        .expect_err("must reject when buffer is full");
    assert!(
        err.contains("RS-3601"),
        "error must cite RS-3601, got: {err}"
    );
    assert_eq!(buf.len(), 3, "buffer must not have grown");
}

// ─── Test 2: Checkpoint commits when all shards ack ──────────────────────────

#[test]
fn proof_checkpoint_commits_when_all_shards_ack() {
    const NUM_SHARDS: usize = 4;
    let mut coord = CheckpointCoordinator::new(NUM_SHARDS);
    let barrier = coord.inject_barrier(10).unwrap();
    assert_eq!(barrier.barrier_epoch, 10);

    for (i, shard_idx) in (0..NUM_SHARDS as u64).enumerate() {
        let ack = ShardCheckpointAck {
            shard_id: ShardId(shard_idx),
            checkpoint_id: barrier.checkpoint_id,
            epoch: 10,
            state_size_bytes: 512 * (i as u64 + 1),
        };
        let status = coord.ack_shard(ack).unwrap();

        let remaining = NUM_SHARDS - 1 - i;
        if remaining > 0 {
            assert!(
                matches!(
                    status,
                    CheckpointStatus::InProgress { pending_shards, .. } if pending_shards == remaining
                ),
                "expected InProgress with {remaining} pending, got {status:?}"
            );
        } else {
            // Final ack → Committed.
            assert!(
                matches!(
                    status,
                    CheckpointStatus::Committed {
                        barrier_epoch: 10,
                        shard_count: 4,
                        ..
                    }
                ),
                "expected Committed, got {status:?}"
            );
        }
    }

    // No checkpoint in flight after commit.
    assert!(coord.current_status().is_none());
}

// ─── Test 3: Recovering on abort ─────────────────────────────────────────────

#[test]
fn proof_checkpoint_recovering_on_abort() {
    let mut coord = CheckpointCoordinator::new(3);
    let barrier = coord.inject_barrier(20).unwrap();

    // Ack only one of three shards.
    coord
        .ack_shard(ShardCheckpointAck {
            shard_id: ShardId(0),
            checkpoint_id: barrier.checkpoint_id,
            epoch: 20,
            state_size_bytes: 128,
        })
        .unwrap();

    // Shard 1 fails — abort the checkpoint.
    let status = coord.abort("shard-1 lost lease during checkpoint");
    assert!(
        matches!(status, CheckpointStatus::Recovering { .. }),
        "expected Recovering, got {status:?}"
    );
    if let CheckpointStatus::Recovering { reason, .. } = &status {
        assert!(
            reason.contains("RS-3602"),
            "reason must cite RS-3602, got: {reason}"
        );
        assert!(
            reason.contains("shard-1 lost lease"),
            "reason must include the abort message"
        );
    }

    // After abort, a new barrier may be injected.
    let new_barrier = coord.inject_barrier(21);
    assert!(
        new_barrier.is_ok(),
        "must be able to inject a new barrier after abort: {new_barrier:?}"
    );
}

// ─── Test 4: GC collects old checkpoints below frontier ──────────────────────

#[test]
fn proof_gc_collects_old_checkpoints_below_frontier() {
    let mut gc = CheckpointGc::new();

    gc.track(CheckpointId(0), 5);
    gc.track(CheckpointId(1), 10);
    gc.track(CheckpointId(2), 15);
    gc.track(CheckpointId(3), 20);

    // Frontier at 11 — epochs 5 and 10 are strictly below.
    let deleted = gc.collect(11);
    assert_eq!(deleted.len(), 2, "expected 2 deleted, got {deleted:?}");
    assert!(deleted.contains(&CheckpointId(0)));
    assert!(deleted.contains(&CheckpointId(1)));
    assert_eq!(gc.len(), 2, "epochs 15 and 20 must remain");

    // Frontier exactly at 15 — epoch 15 is NOT strictly below, so not deleted.
    let deleted2 = gc.collect(15);
    assert!(
        deleted2.is_empty(),
        "epoch == frontier must not be collected: {deleted2:?}"
    );

    // Frontier at 16 — epoch 15 is now strictly below.
    let deleted3 = gc.collect(16);
    assert_eq!(deleted3, vec![CheckpointId(2)]);
}

// ─── Test 5: Slow shard blocks commit ────────────────────────────────────────

#[test]
fn proof_slow_shard_blocks_commit() {
    const NUM_SHARDS: usize = 5;
    let mut coord = CheckpointCoordinator::new(NUM_SHARDS);
    let barrier = coord.inject_barrier(30).unwrap();

    // Ack 4 of 5 shards — must stay InProgress.
    for i in 0..(NUM_SHARDS as u64 - 1) {
        let status = coord
            .ack_shard(ShardCheckpointAck {
                shard_id: ShardId(i),
                checkpoint_id: barrier.checkpoint_id,
                epoch: 30,
                state_size_bytes: 256,
            })
            .unwrap();
        assert!(
            matches!(status, CheckpointStatus::InProgress { .. }),
            "must stay InProgress while slow shard has not acked (after ack {i}): {status:?}"
        );
    }

    // Slow shard finally acks — now Committed.
    let final_status = coord
        .ack_shard(ShardCheckpointAck {
            shard_id: ShardId(NUM_SHARDS as u64 - 1),
            checkpoint_id: barrier.checkpoint_id,
            epoch: 30,
            state_size_bytes: 256,
        })
        .unwrap();
    assert!(
        matches!(final_status, CheckpointStatus::Committed { .. }),
        "expected Committed after last shard acks: {final_status:?}"
    );
}

// ─── Test 6: Double barrier injection is blocked ──────────────────────────────

#[test]
fn proof_double_barrier_injection_blocked() {
    let mut coord = CheckpointCoordinator::new(2);
    let _b1 = coord.inject_barrier(50).unwrap();

    // Second inject while first is in progress.
    let err = coord
        .inject_barrier(51)
        .expect_err("second barrier must be rejected while first is in progress");
    assert!(
        err.contains("RS-3602"),
        "error must cite RS-3602, got: {err}"
    );

    // Original checkpoint still in progress.
    assert!(
        coord.current_status().is_some(),
        "original checkpoint must still be in progress"
    );
}

// ─── Test 7: Drain releases buffer capacity ───────────────────────────────────

#[test]
fn proof_alignment_buffer_drain_releases_capacity() {
    let mut buf = AlignmentBuffer::new(2);
    buf.push(vec![1], vec![1]).unwrap();
    buf.push(vec![2], vec![2]).unwrap();

    // Full — rejects.
    assert!(buf.push(vec![3], vec![3]).is_err());

    // Drain clears the buffer.
    let rows = buf.drain();
    assert_eq!(rows.len(), 2);
    assert!(buf.is_empty());

    // After drain, capacity is restored.
    assert!(buf.push(vec![10], vec![10]).is_ok());
    assert!(buf.push(vec![11], vec![11]).is_ok());
    assert_eq!(buf.len(), 2);
}

// ─── Test 8: Credit exhaustion is bounded (never unbounded growth) ────────────

#[test]
fn proof_credit_exhaustion_never_unbounded() {
    const CAPACITY: usize = 10;
    let mut buf = AlignmentBuffer::new(CAPACITY);

    // Fill to capacity.
    for i in 0..CAPACITY {
        buf.push(vec![i as u8], vec![i as u8]).unwrap();
    }
    assert_eq!(buf.len(), CAPACITY);

    // 100 further pushes — ALL must fail with RS-3601.
    for extra in 0..100 {
        let err = buf
            .push(vec![extra as u8], vec![extra as u8])
            .expect_err("must reject when over capacity");
        assert!(
            err.contains("RS-3601"),
            "push {extra}: expected RS-3601, got: {err}"
        );
        // Critically: buffer length must never exceed CAPACITY.
        assert_eq!(
            buf.len(),
            CAPACITY,
            "buffer grew beyond CAPACITY after {extra} extra pushes"
        );
    }
}

// ─── Test 9: Sequential checkpoints have increasing IDs ──────────────────────

#[test]
fn proof_sequential_checkpoints_increment_id() {
    const NUM_SHARDS: usize = 1;
    let mut coord = CheckpointCoordinator::new(NUM_SHARDS);

    let mut last_id = CheckpointId(u64::MAX);
    for epoch in 0..5u64 {
        let barrier = coord.inject_barrier(epoch).unwrap();
        let id = barrier.checkpoint_id;

        if epoch > 0 {
            assert!(
                id > last_id,
                "checkpoint ID must be strictly increasing: got {id} after {last_id}"
            );
        }
        last_id = id;

        coord
            .ack_shard(ShardCheckpointAck {
                shard_id: ShardId(0),
                checkpoint_id: id,
                epoch,
                state_size_bytes: 0,
            })
            .unwrap();
    }
}

// ─── Test 10: GC inline in coordinator ───────────────────────────────────────

#[test]
fn proof_gc_inline_in_coordinator() {
    let mut coord = CheckpointCoordinator::new(1);

    // Commit 3 checkpoints at epochs 5, 15, 25.
    for barrier_epoch in [5u64, 15, 25] {
        let b = coord.inject_barrier(barrier_epoch).unwrap();
        coord
            .ack_shard(ShardCheckpointAck {
                shard_id: ShardId(0),
                checkpoint_id: b.checkpoint_id,
                epoch: barrier_epoch,
                state_size_bytes: 0,
            })
            .unwrap();
    }
    assert_eq!(
        coord.gc_len(),
        3,
        "all 3 committed checkpoints should be tracked"
    );

    // Frontier at 16 collects epochs 5 and 15.
    let deleted = coord.gc_old_checkpoints(16);
    assert_eq!(deleted.len(), 2, "expected 2 deletions: {deleted:?}");
    assert_eq!(coord.gc_len(), 1, "epoch-25 checkpoint should remain");

    // Frontier at 26 collects epoch 25.
    let deleted2 = coord.gc_old_checkpoints(26);
    assert_eq!(deleted2.len(), 1);
    assert_eq!(coord.gc_len(), 0);
}

// ─── Test 11: Committed shard_count equals num_shards ────────────────────────

#[test]
fn proof_multi_shard_commit_reports_correct_count() {
    const N: usize = 7;
    let mut coord = CheckpointCoordinator::new(N);
    let barrier = coord.inject_barrier(100).unwrap();

    let mut last = None;
    for i in 0..N as u64 {
        last = Some(
            coord
                .ack_shard(ShardCheckpointAck {
                    shard_id: ShardId(i),
                    checkpoint_id: barrier.checkpoint_id,
                    epoch: 100,
                    state_size_bytes: i * 1024,
                })
                .unwrap(),
        );
    }
    if let Some(CheckpointStatus::Committed { shard_count, .. }) = last {
        assert_eq!(shard_count, N);
    } else {
        panic!("expected Committed after all {N} shard acks");
    }
}

// ─── Test 12: No status before barrier / after commit ─────────────────────────

#[test]
fn proof_no_status_when_no_barrier_injected() {
    let mut coord = CheckpointCoordinator::new(2);

    // Before any barrier: None.
    assert!(
        coord.current_status().is_none(),
        "expected None before first barrier"
    );

    let barrier = coord.inject_barrier(7).unwrap();

    // After inject: InProgress.
    assert!(coord.current_status().is_some());

    // After all acks: None again.
    for i in 0..2u64 {
        coord
            .ack_shard(ShardCheckpointAck {
                shard_id: ShardId(i),
                checkpoint_id: barrier.checkpoint_id,
                epoch: 7,
                state_size_bytes: 0,
            })
            .unwrap();
    }
    assert!(
        coord.current_status().is_none(),
        "expected None after commit"
    );
}

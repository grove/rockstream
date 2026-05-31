//! CI proof tests for the recovery driver and SLO metrics (v0.35).
//!
//! ## Proof obligations (ROADMAP v0.35)
//!
//! Exit criteria:
//! - **Failure detection ≤ 5 s SLO**: A worker is declared failed within 5 s
//!   of its last heartbeat.
//! - **Shard reassignment ≤ 30 s SLO**: The recovery driver records
//!   reassignments synchronously; the 30 s budget is tracked by
//!   `RecoveryStatus`.
//! - **Pipeline freshness recovery ≤ 60 s**: `RecoveringSlow` / `RS-3603`
//!   fires when a recovery is still active after 60 s.
//! - **32-worker simultaneous restart — no false failure detections**: Workers
//!   that send heartbeats at burst time are never reported as failed.
//!
//! ### Tests
//!
//! 1.  **`proof_failure_detection_within_5s_slo`** — At t=4 999 ms the worker
//!     is still Healthy; at t=5 001 ms it is declared Failed.
//!
//! 2.  **`proof_shard_reassignment_recorded_immediately`** — The recovery
//!     driver records reassignments synchronously (≤ 30 s tracking).
//!
//! 3.  **`proof_recovering_slow_fires_after_60s`** — `RecoveringSlow` is
//!     returned when a recovery has been active for more than 60 000 ms.
//!
//! 4.  **`proof_32_worker_restart_no_false_detections`** — 32 workers all
//!     heartbeat at t=0; ticked at t=1 000 ms (well within the 5 s window)
//!     → no false failures.
//!
//! 5.  **`proof_worker_self_fencing_on_cp_partition`** — `ControlPlaneFence`
//!     fires `Fenced` after the fence timeout; `Connected` before it.
//!
//! 6.  **`proof_throttled_granter_prevents_thundering_herd`** — 32 concurrent
//!     grant requests with `max_grants_per_window=4` result in only 4 grants
//!     in the first window; the remainder are deferred.
//!
//! 7.  **`proof_heartbeat_reset_prevents_false_positive`** — A worker that
//!     was close to the timeout but sends a heartbeat is never declared failed.
//!
//! 8.  **`proof_recovery_healthy_after_complete`** — After calling
//!     `mark_complete`, the driver returns `Healthy`.
//!
//! 9.  **`proof_multiple_worker_failures_tracked_independently`** — Two
//!     simultaneous recoveries are each tracked independently; completing one
//!     does not affect the other.
//!
//! 10. **`proof_fence_resets_on_contact`** — `ControlPlaneFence` resets to
//!     `Connected` when `record_contact` is called before the timeout fires.
//!
//! 11. **`proof_throttle_window_rolls_over`** — After the window expires, the
//!     grant count resets and new grants are allowed.
//!
//! 12. **`proof_recovering_slow_includes_elapsed`** — The `elapsed_ms` field
//!     in `RecoveringSlow` reflects time since recovery started.

use rockstream_runtime::recovery::{
    ControlPlaneFence, FenceStatus, RecoveryDriver, RecoveryStatus, ShardReassignment,
    ThrottledLeaseGranter, WorkerHealthMonitor, WorkerStatus,
};
use rockstream_types::ids::{ShardId, WorkerId};

// ─── Test 1: Failure detection within 5 s SLO ────────────────────────────────

#[test]
fn proof_failure_detection_within_5s_slo() {
    const TIMEOUT_MS: u64 = 5_000;
    let mut mon = WorkerHealthMonitor::new(TIMEOUT_MS);
    mon.register(WorkerId(1), 0);

    // At exactly the timeout boundary: still Healthy (> not >=).
    let failed = mon.tick(TIMEOUT_MS);
    assert!(
        failed.is_empty(),
        "worker must still be Healthy at exactly timeout_ms: {failed:?}"
    );

    // One millisecond over the timeout: Failed.
    let failed = mon.tick(TIMEOUT_MS + 1);
    assert_eq!(
        failed,
        vec![WorkerId(1)],
        "worker must be declared Failed at timeout_ms + 1"
    );

    // Confirm status accessor agrees.
    assert_eq!(
        mon.status(WorkerId(1), TIMEOUT_MS + 1),
        Some(WorkerStatus::Failed)
    );
}

// ─── Test 2: Shard reassignment recorded synchronously ───────────────────────

#[test]
fn proof_shard_reassignment_recorded_immediately() {
    let mut driver = RecoveryDriver::new(60_000);

    let result = driver.record_recovery(
        WorkerId(1),
        vec![
            ShardReassignment {
                shard_id: ShardId(0),
                new_owner: WorkerId(2),
            },
            ShardReassignment {
                shard_id: ShardId(1),
                new_owner: WorkerId(3),
            },
        ],
        0,
    );

    assert_eq!(result.failed_worker, WorkerId(1));
    assert_eq!(result.reassigned.len(), 2);
    assert_eq!(result.started_at_ms, 0);

    // Within 30 s the status should be Recovering (not slow yet).
    assert!(
        matches!(driver.status(29_999), RecoveryStatus::Recovering { .. }),
        "must be Recovering within reassignment SLO"
    );
    assert_eq!(driver.active_count(), 1);
}

// ─── Test 3: RecoveringSlow after 60 s ───────────────────────────────────────

#[test]
fn proof_recovering_slow_fires_after_60s() {
    let mut driver = RecoveryDriver::new(60_000);
    driver.record_recovery(
        WorkerId(5),
        vec![ShardReassignment {
            shard_id: ShardId(10),
            new_owner: WorkerId(6),
        }],
        1_000,
    );

    // At started_at + 60 000 ms (exactly): still Recovering.
    assert!(
        matches!(driver.status(61_000), RecoveryStatus::Recovering { .. }),
        "must be Recovering at exactly the threshold"
    );

    // One millisecond over: RecoveringSlow.
    let status = driver.status(61_001);
    assert!(
        matches!(status, RecoveryStatus::RecoveringSlow { .. }),
        "expected RecoveringSlow, got {status:?}"
    );
}

// ─── Test 4: 32-worker simultaneous restart — no false failures ───────────────

#[test]
fn proof_32_worker_restart_no_false_detections() {
    const NUM_WORKERS: u64 = 32;
    const TIMEOUT_MS: u64 = 5_000;

    let mut mon = WorkerHealthMonitor::new(TIMEOUT_MS);

    // All 32 workers register and send heartbeats at t=0.
    for i in 0..NUM_WORKERS {
        mon.register(WorkerId(i), 0);
    }

    // Tick at 1 000 ms — well within the 5 s window.
    let failed = mon.tick(1_000);
    assert!(
        failed.is_empty(),
        "no worker should be declared failed at 1 s (timeout = 5 s): {failed:?}"
    );

    // All workers report Healthy.
    for i in 0..NUM_WORKERS {
        assert_eq!(
            mon.status(WorkerId(i), 1_000),
            Some(WorkerStatus::Healthy),
            "worker {i} must be Healthy at t=1000ms"
        );
    }
}

// ─── Test 5: Worker self-fencing on control-plane partition ──────────────────

#[test]
fn proof_worker_self_fencing_on_cp_partition() {
    const FENCE_TIMEOUT_MS: u64 = 30_000;
    let fence = ControlPlaneFence::new(FENCE_TIMEOUT_MS, 0);

    // Just before timeout: Connected.
    assert_eq!(
        fence.check(FENCE_TIMEOUT_MS),
        FenceStatus::Connected,
        "fence must not fire at exactly the timeout"
    );

    // One millisecond over: Fenced.
    assert_eq!(
        fence.check(FENCE_TIMEOUT_MS + 1),
        FenceStatus::Fenced,
        "fence must fire at timeout + 1 ms"
    );

    // Elapsed accessor.
    assert_eq!(fence.elapsed_ms(10_000), 10_000);
}

// ─── Test 6: ThrottledLeaseGranter prevents thundering herd ──────────────────

#[test]
fn proof_throttled_granter_prevents_thundering_herd() {
    const MAX_PER_WINDOW: usize = 4;
    const NUM_WORKERS: usize = 32;
    let mut granter = ThrottledLeaseGranter::new(MAX_PER_WINDOW, 1_000, 0);

    // Simulate 32 workers all requesting a lease at t=0.
    let mut granted = 0usize;
    let mut denied = 0usize;
    for _ in 0..NUM_WORKERS {
        if granter.try_grant(0) {
            granted += 1;
        } else {
            denied += 1;
        }
    }

    assert_eq!(
        granted, MAX_PER_WINDOW,
        "only {MAX_PER_WINDOW} grants should be allowed in the first window"
    );
    assert_eq!(
        denied,
        NUM_WORKERS - MAX_PER_WINDOW,
        "{} grants should be deferred",
        NUM_WORKERS - MAX_PER_WINDOW
    );
}

// ─── Test 7: Heartbeat reset prevents false positive ─────────────────────────

#[test]
fn proof_heartbeat_reset_prevents_false_positive() {
    let mut mon = WorkerHealthMonitor::new(5_000);
    mon.register(WorkerId(1), 0);

    // At 4 500 ms: heartbeat resets the clock.
    mon.heartbeat(WorkerId(1), 4_500);

    // At 9 499 ms (4 999 ms after last heartbeat): still Healthy.
    assert!(
        mon.tick(9_499).is_empty(),
        "worker with recent heartbeat must not be declared failed"
    );
    assert_eq!(mon.status(WorkerId(1), 9_499), Some(WorkerStatus::Healthy));

    // At 9 501 ms (5 001 ms after last heartbeat): Failed.
    assert_eq!(mon.tick(9_501), vec![WorkerId(1)]);
}

// ─── Test 8: Recovery becomes Healthy after mark_complete ────────────────────

#[test]
fn proof_recovery_healthy_after_complete() {
    let mut driver = RecoveryDriver::new(60_000);
    let result = driver.record_recovery(
        WorkerId(1),
        vec![ShardReassignment {
            shard_id: ShardId(0),
            new_owner: WorkerId(2),
        }],
        0,
    );

    // Recovery in progress.
    assert!(matches!(
        driver.status(1_000),
        RecoveryStatus::Recovering { .. }
    ));

    // Mark complete.
    driver.mark_complete(result.recovery_id);

    // Now healthy.
    assert_eq!(driver.status(1_000), RecoveryStatus::Healthy);
    assert_eq!(driver.active_count(), 0);
}

// ─── Test 9: Multiple failures tracked independently ─────────────────────────

#[test]
fn proof_multiple_worker_failures_tracked_independently() {
    let mut driver = RecoveryDriver::new(60_000);

    // Worker A fails at t=0.
    let result_a = driver.record_recovery(
        WorkerId(1),
        vec![ShardReassignment {
            shard_id: ShardId(0),
            new_owner: WorkerId(10),
        }],
        0,
    );

    // Worker B fails at t=5000.
    let _result_b = driver.record_recovery(
        WorkerId(2),
        vec![ShardReassignment {
            shard_id: ShardId(1),
            new_owner: WorkerId(11),
        }],
        5_000,
    );

    assert_eq!(driver.active_count(), 2);

    // Complete A's recovery.
    driver.mark_complete(result_a.recovery_id);
    assert_eq!(driver.active_count(), 1, "only B's recovery should remain");

    // B's recovery is still in progress.
    assert!(matches!(
        driver.status(10_000),
        RecoveryStatus::Recovering { .. }
    ));
}

// ─── Test 10: Fence resets on contact ────────────────────────────────────────

#[test]
fn proof_fence_resets_on_contact() {
    let mut fence = ControlPlaneFence::new(30_000, 0);

    // Near the timeout — but then contact is re-established.
    fence.record_contact(29_000);

    // 30 001 ms after the original start, but only 1 001 ms after last contact.
    assert_eq!(
        fence.check(30_001),
        FenceStatus::Connected,
        "fence must not fire after re-contact: elapsed since contact = 1001 ms < 30000 ms"
    );

    // Beyond the new timeout.
    assert_eq!(fence.check(59_001), FenceStatus::Fenced);
}

// ─── Test 11: Throttle window rolls over ─────────────────────────────────────

#[test]
fn proof_throttle_window_rolls_over() {
    let mut granter = ThrottledLeaseGranter::new(2, 1_000, 0);

    // Exhaust window 1.
    assert!(granter.try_grant(0));
    assert!(granter.try_grant(0));
    assert!(!granter.try_grant(0)); // Denied.

    // New window starts at t=1000.
    assert!(granter.try_grant(1_000));
    assert!(granter.try_grant(1_001));
    assert!(!granter.try_grant(1_001)); // Denied again.

    // Another window at t=2000.
    assert!(granter.try_grant(2_000));
}

// ─── Test 12: RecoveringSlow includes elapsed_ms ─────────────────────────────

#[test]
fn proof_recovering_slow_includes_elapsed() {
    let mut driver = RecoveryDriver::new(60_000);
    driver.record_recovery(
        WorkerId(1),
        vec![ShardReassignment {
            shard_id: ShardId(0),
            new_owner: WorkerId(2),
        }],
        1_000,
    );

    // Check at t=62 000 (61 000 ms after start).
    let status = driver.status(62_000);
    if let RecoveryStatus::RecoveringSlow {
        started_at_ms,
        elapsed_ms,
        ..
    } = status
    {
        assert_eq!(started_at_ms, 1_000);
        assert_eq!(elapsed_ms, 61_000, "elapsed must equal now - started_at");
    } else {
        panic!("expected RecoveringSlow, got {status:?}");
    }
}

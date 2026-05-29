//! OperatorTask: per-operator tokio task event loop (IVM.md §8.2).
//!
//! Each operator instance runs as an independent tokio task. Tasks receive
//! input delta batches via a channel, call `Operator::process_delta`, and
//! send `EpochOutput` fragments to the shard-level epoch commit coordinator
//! via an output channel.
//!
//! Credit-based backpressure: the task holds a credit token for each in-flight
//! input batch. When credits are exhausted, the upstream source is implicitly
//! paused because the bounded channel fills up.
//!
//! Cooperative scheduling (DESIGN.md §9.3): when a `ProcessDelta` input
//! contains more rows than `SchedulerConfig::max_rows_per_quantum`, the task
//! splits the work into chunks and calls `tokio::task::yield_now()` between
//! each chunk. This prevents a single expensive epoch from starving heartbeat
//! sends and frontier reports running as separate tokio tasks.

use tokio::sync::mpsc;

use rockstream_sim::{Spawner, TokioRuntime};
use rockstream_types::batch::{ZSet, ZSetBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::timestamp::Epoch;

use crate::epoch_output::EpochOutput;
use crate::operator::Operator;
use crate::scheduler::{SchedulerConfig, YieldCounter};

/// Command sent to an operator task.
#[derive(Debug)]
pub enum OperatorCmd {
    /// Process a new input delta for the given epoch.
    ProcessDelta { epoch: Epoch, input: ZSetBatch },
    /// Notify that an epoch is complete (no more deltas for this epoch).
    EpochComplete { epoch: Epoch },
    /// Shut down the task cleanly.
    Shutdown,
}

/// Handle to a running operator task.
///
/// Dropping this handle does NOT shut down the task; send `Shutdown` first.
pub struct OperatorTaskHandle {
    pub operator_id: OperatorId,
    /// Channel for sending commands to the task.
    pub tx: mpsc::Sender<OperatorCmd>,
}

/// Run an operator as a background tokio task.
///
/// Returns an `OperatorTaskHandle` for sending commands to the task and a
/// receiver for collecting `EpochOutput` fragments.
///
/// Uses `SchedulerConfig::default()` (quantum = 65536). For custom quantum
/// sizing or yield-ratio metrics, use `spawn_operator_task_with_config`.
///
/// Uses `TokioRuntime` as the spawner. To test with deterministic scheduling
/// or fault injection, use `spawn_operator_task_with_config` and pass a
/// `SimRuntime` as the spawner.
///
/// # Parameters
/// - `operator_id`: Unique ID for this operator instance.
/// - `operator`: The boxed `Operator` implementation.
/// - `output_tx`: Channel for sending `EpochOutput` fragments to the
///   shard-level epoch commit coordinator.
/// - `cmd_buffer`: Number of commands that can be buffered before backpressure
///   is applied to the sender.
pub fn spawn_operator_task(
    operator_id: OperatorId,
    operator: Box<dyn Operator>,
    output_tx: mpsc::Sender<EpochOutput>,
    cmd_buffer: usize,
) -> OperatorTaskHandle {
    let spawner = TokioRuntime::new(0);
    spawn_operator_task_with_config(
        operator_id,
        operator,
        output_tx,
        cmd_buffer,
        SchedulerConfig::default(),
        YieldCounter::new(),
        &spawner,
    )
}

/// Run an operator as a background tokio task with explicit scheduler config.
///
/// Identical to `spawn_operator_task` but accepts a `SchedulerConfig` for
/// quantum sizing, a `YieldCounter` for metric reporting, and a `Spawner`
/// for task execution.
///
/// Pass `&TokioRuntime::new(0)` for production use. Pass `&SimRuntime::new(seed)`
/// in tests for deterministic scheduling and fault injection via `buggify!()`.
///
/// When `input.zset.len() > config.max_rows_per_quantum`, the task splits
/// the input into chunks of `max_rows_per_quantum` rows, processes each
/// chunk, calls `tokio::task::yield_now()` between chunks, and records the
/// yield in `yield_counter`. This is the cooperative scheduling contract
/// described in DESIGN.md §9.3.
pub fn spawn_operator_task_with_config(
    operator_id: OperatorId,
    mut operator: Box<dyn Operator>,
    output_tx: mpsc::Sender<EpochOutput>,
    cmd_buffer: usize,
    config: SchedulerConfig,
    yield_counter: YieldCounter,
    spawner: &dyn Spawner,
) -> OperatorTaskHandle {
    let (tx, mut rx) = mpsc::channel::<OperatorCmd>(cmd_buffer);

    spawner.spawn_box(
        "operator-task",
        Box::pin(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    OperatorCmd::ProcessDelta { epoch, input } => {
                        yield_counter.record_epoch();
                        let row_count = input.zset.len() as u64;
                        let quantum = config.max_rows_per_quantum;

                        let delta = if row_count > quantum {
                            // Quantum-bounded path: split input into chunks, yielding
                            // between each so other tokio tasks (heartbeats, frontier
                            // reporters) get scheduling opportunities.
                            let rows: Vec<_> = input.zset.iter().collect();
                            let chunk_size = quantum as usize;
                            let total_chunks = rows.len().div_ceil(chunk_size);
                            let mut accumulated = ZSet::new();
                            let mut did_yield = false;

                            for (i, chunk) in rows.chunks(chunk_size).enumerate() {
                                let mut chunk_zset = ZSet::new();
                                for row in chunk {
                                    chunk_zset.insert(
                                        row.key.clone(),
                                        row.value.clone(),
                                        row.weight,
                                    );
                                }
                                let chunk_batch = ZSetBatch {
                                    zset: chunk_zset,
                                    epoch,
                                };
                                let partial = operator.process_delta(&chunk_batch).await;
                                accumulated.merge(&partial.zset);

                                // Yield between chunks (not after the last one).
                                if i + 1 < total_chunks {
                                    did_yield = true;
                                    tokio::task::yield_now().await;
                                }
                            }

                            if did_yield {
                                yield_counter.record_yield();
                            }

                            ZSetBatch {
                                zset: accumulated,
                                epoch,
                            }
                        } else {
                            // Fast path: batch fits within one quantum.
                            operator.process_delta(&input).await
                        };

                        let output = EpochOutput::new(operator_id, epoch, delta, false);
                        if output_tx.send(output).await.is_err() {
                            break;
                        }
                    }
                    OperatorCmd::EpochComplete { epoch } => {
                        operator.epoch_complete(epoch).await;
                        // Send final fragment to signal epoch boundary.
                        let final_out = EpochOutput::new(
                            operator_id,
                            epoch,
                            ZSetBatch {
                                zset: ZSet::new(),
                                epoch,
                            },
                            true,
                        );
                        if output_tx.send(final_out).await.is_err() {
                            break;
                        }
                    }
                    OperatorCmd::Shutdown => break,
                }
            }
        }),
    );

    OperatorTaskHandle { operator_id, tx }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::Operator;
    use async_trait::async_trait;
    use rockstream_types::batch::{SinkBatch, SourceBatch, ZSet, ZSetBatch};
    use rockstream_types::merge_law::MergeLawId;
    use tokio::sync::mpsc;

    struct PassthroughOp;

    #[async_trait]
    impl Operator for PassthroughOp {
        async fn process(&mut self, _input: &SourceBatch) -> SinkBatch {
            SinkBatch::default()
        }
        async fn epoch_complete(&mut self, _epoch: rockstream_types::timestamp::Epoch) {}
        fn name(&self) -> &str {
            "passthrough"
        }
        fn merge_law(&self) -> Option<MergeLawId> {
            None
        }
    }

    #[tokio::test]
    async fn operator_task_processes_delta() {
        let (output_tx, mut output_rx) = mpsc::channel(16);
        let handle = spawn_operator_task(OperatorId(0), Box::new(PassthroughOp), output_tx, 8);

        let input = ZSetBatch {
            zset: ZSet::new(),
            epoch: 1,
        };
        handle
            .tx
            .send(OperatorCmd::ProcessDelta {
                epoch: 1,
                input: input.clone(),
            })
            .await
            .unwrap();
        handle
            .tx
            .send(OperatorCmd::EpochComplete { epoch: 1 })
            .await
            .unwrap();
        handle.tx.send(OperatorCmd::Shutdown).await.unwrap();

        // Receive delta fragment
        let frag = output_rx.recv().await.unwrap();
        assert_eq!(frag.epoch, 1);
        assert!(!frag.is_final);

        // Receive final fragment
        let final_frag = output_rx.recv().await.unwrap();
        assert_eq!(final_frag.epoch, 1);
        assert!(final_frag.is_final);
    }
}

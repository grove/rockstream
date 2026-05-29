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

use tokio::sync::mpsc;

use rockstream_types::batch::ZSetBatch;
use rockstream_types::ids::OperatorId;
use rockstream_types::timestamp::Epoch;

use crate::epoch_output::EpochOutput;
use crate::operator::Operator;

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
/// # Parameters
/// - `operator_id`: Unique ID for this operator instance.
/// - `operator`: The boxed `Operator` implementation.
/// - `output_tx`: Channel for sending `EpochOutput` fragments to the
///   shard-level epoch commit coordinator.
/// - `cmd_buffer`: Number of commands that can be buffered before backpressure
///   is applied to the sender.
pub fn spawn_operator_task(
    operator_id: OperatorId,
    mut operator: Box<dyn Operator>,
    output_tx: mpsc::Sender<EpochOutput>,
    cmd_buffer: usize,
) -> OperatorTaskHandle {
    let (tx, mut rx) = mpsc::channel::<OperatorCmd>(cmd_buffer);

    tokio::spawn(async move {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                OperatorCmd::ProcessDelta { epoch, input } => {
                    let delta = operator.process_delta(&input).await;
                    let output = EpochOutput::new(operator_id, epoch, delta, false);
                    // Best-effort send; if the coordinator has shut down, stop.
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
                        rockstream_types::batch::ZSetBatch {
                            zset: rockstream_types::batch::ZSet::new(),
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
    });

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

use std::sync::Arc;
use std::task::{Context, Waker};

use rayexec_error::Result;

use super::util::resizer::{BatchResizer, DEFAULT_TARGET_BATCH_SIZE};
use super::{
    ExecutableOperator,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionState,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::execution::computed_batch::ComputedBatches;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct BatchResizerPartitionState {
    /// Buffered batches.
    buffered: ComputedBatches,
    /// The actual resizer. Internally buffers some number of batches.
    resizer: BatchResizer,
    /// Pull waker. Woken when resizer produces a batch of the target size.
    pull_waker: Option<Waker>,
    /// Push waker. Woken when buffered batches get pulled.
    push_waker: Option<Waker>,
    exhausted: bool,
}

/// Wrapper around the resizer util to resize batches during pipeline execution.
#[derive(Debug)]
pub struct PhysicalBatchResizer;

impl ExecutableOperator for PhysicalBatchResizer {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: (0..partitions[0])
                    .map(|_| {
                        PartitionState::BatchResizer(BatchResizerPartitionState {
                            buffered: ComputedBatches::None,
                            resizer: BatchResizer::new(DEFAULT_TARGET_BATCH_SIZE),
                            pull_waker: None,
                            push_waker: None,
                            exhausted: false,
                        })
                    })
                    .collect(),
            },
        })
    }

    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::BatchResizer(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if !state.buffered.is_empty() {
            state.push_waker = Some(cx.waker().clone());

            // Trigger pull to make room.
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollPush::Pending(batch));
        }

        let computed = state.resizer.try_push(batch)?;
        state.buffered = computed;

        // Only wake pull side if it's reasonable to do so.
        if state.buffered.has_batches() {
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            Ok(PollPush::Pushed)
        } else {
            // Otherwise we need more batches.
            Ok(PollPush::NeedsMore)
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::BatchResizer(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if !state.buffered.is_empty() {
            state.push_waker = Some(cx.waker().clone());

            // Trigger pull.
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }

            return Ok(PollFinalize::Pending);
        }

        state.exhausted = true;
        state.buffered = state.resizer.flush_remaining()?;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::BatchResizer(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        if state.buffered.is_empty() {
            if state.exhausted {
                return Ok(PollPull::Exhausted);
            }

            // Register wakeup.
            state.pull_waker = Some(cx.waker().clone());
            if let Some(waker) = state.push_waker.take() {
                waker.wake()
            }

            return Ok(PollPull::Pending);
        }

        let buffered = state.buffered.take();

        if let Some(waker) = state.push_waker.take() {
            waker.wake()
        }

        Ok(PollPull::Computed(buffered))
    }
}

impl Explainable for PhysicalBatchResizer {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("BatchResizer")
    }
}

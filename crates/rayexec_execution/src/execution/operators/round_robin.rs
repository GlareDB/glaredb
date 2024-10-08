use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Waker};

use crate::database::DatabaseContext;
use crate::execution::operators::{
    ExecutableOperator, OperatorState, PartitionState, PollPull, PollPush,
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::{ExecutionStates, InputOutputStates, PollFinalize};

/// Partition state on the pull side.
#[derive(Debug)]
pub struct RoundRobinPushPartitionState {
    /// Partition index corresponding to this state. This is used for storing a
    /// waker in the batch buffer at the appropriate index.
    own_idx: usize,

    /// Output partition to push to on the next input.
    push_to: usize,

    /// Buffers to the output partition.
    output_buffers: Vec<BatchBuffer>,

    /// Maximum capacity of the batch buffer in each output buffer.
    max_buffer_capacity: usize,
}

/// Partition state on the push side.
#[derive(Debug)]
pub struct RoundRobinPullPartitionState {
    /// The buffer specific to this output partition.
    buffer: BatchBuffer,
}

/// Global repartitioning state.
#[derive(Debug)]
pub struct RoundRobinOperatorState {
    /// Number of input partitions still pushing.
    ///
    /// Once this reaches zero, all output partitions will no longer receive any
    /// batches.
    num_inputs_remaining: AtomicUsize,
}

/// Repartition 'n' input partitions into 'm' output partitions.
#[derive(Debug)]
pub struct PhysicalRoundRobinRepartition;

impl ExecutableOperator for PhysicalRoundRobinRepartition {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        if partitions.len() != 2 {
            return Err(RayexecError::new(
                "Round robin expects to values (input, output) in partition vec",
            ));
        }

        // TODO: Unsure if I like this.
        let input_partitions = partitions[0];
        let output_partitions = partitions[1];

        let operator_state = RoundRobinOperatorState {
            num_inputs_remaining: AtomicUsize::new(input_partitions),
        };

        // Should probably tweak this. I'm currently setting this to match the
        // number of input partitions because it _feels_ right, but I'm not able to
        // put into words why I think that.
        let buffer_cap = input_partitions;

        let output_buffers: Vec<_> = (0..output_partitions)
            .map(|_| BatchBuffer {
                inner: Arc::new(Mutex::new(BatchBufferInner {
                    batches: VecDeque::with_capacity(buffer_cap),
                    recv_waker: None,
                    send_wakers: vec![None; input_partitions],
                    exhausted: false,
                })),
            })
            .collect();

        let mut push_states = Vec::with_capacity(input_partitions);
        let mut push_to = 0;
        for idx in 0..input_partitions {
            let state = PartitionState::RoundRobinPush(RoundRobinPushPartitionState {
                own_idx: idx,
                push_to,
                output_buffers: output_buffers.clone(),
                max_buffer_capacity: buffer_cap,
            });
            push_states.push(state);

            // Each state should be initialized to push to a different output
            // partition to avoid immediate contention as soon as execution begins.
            push_to = (push_to + 1) % output_partitions;
        }

        let pull_states: Vec<_> = output_buffers
            .into_iter()
            .map(|buffer| PartitionState::RoundRobinPull(RoundRobinPullPartitionState { buffer }))
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::RoundRobin(operator_state)),
            partition_states: InputOutputStates::SeparateInputOutput {
                push_states,
                pull_states,
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
            PartitionState::RoundRobinPush(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let output = &mut state
            .output_buffers
            .get(state.push_to)
            .expect("buffer to exist")
            .inner
            .lock();

        // There's already enough buffered batches. Register ourselves for later
        // wakeup when there's room.
        if output.batches.len() >= state.max_buffer_capacity {
            output.send_wakers[state.own_idx] = Some(cx.waker().clone());
            return Ok(PollPush::Pending(batch));
        }

        // Otherwise push our batch.
        output.batches.push_back(batch);

        if let Some(waker) = output.recv_waker.take() {
            waker.wake();
        }

        // And update the state to push to the next output partition on the next
        // call to `poll_push`.
        state.push_to = (state.push_to + 1) % state.output_buffers.len();

        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let operator_state = match operator_state {
            OperatorState::RoundRobin(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        let prev = operator_state
            .num_inputs_remaining
            .fetch_sub(1, Ordering::SeqCst);

        // If we're the last input partition, wake up all pullers so that
        // they're notified that all partitions are exhausted.
        //
        // Since this is round-robin, any input batch can be routed to any
        // output partition, so we can't do anything if we still have
        // non-exhausted inputs.
        if prev == 1 {
            let state = match partition_state {
                PartitionState::RoundRobinPush(state) => state,
                other => panic!("invalid partition state: {other:?}"),
            };

            for output in state.output_buffers.iter() {
                let inner = &mut output.inner.lock();
                inner.exhausted = true;
                if let Some(waker) = inner.recv_waker.take() {
                    waker.wake();
                }
            }
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
            PartitionState::RoundRobinPull(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let inner = &mut state.buffer.inner.lock();

        match inner.batches.pop_front() {
            Some(batch) => {
                inner.wake_n_senders(1);
                Ok(PollPull::Computed(batch.into()))
            }
            None => {
                if inner.exhausted {
                    return Ok(PollPull::Exhausted);
                }
                // Register ourselves for wakeup.
                inner.recv_waker = Some(cx.waker().clone());

                // Try to wake up any pushers to fill up the buffer.
                inner.wake_all_senders();

                Ok(PollPull::Pending)
            }
        }
    }
}

/// A simple mpsc-like buffer for coordinating repartitioning of batches.
#[derive(Debug, Clone)]
struct BatchBuffer {
    inner: Arc<Mutex<BatchBufferInner>>,
}

#[derive(Debug)]
struct BatchBufferInner {
    /// Batches buffer.
    ///
    /// Should be bounded to some capacity.
    batches: VecDeque<Batch>,

    /// Waker on the receiving side of the buffer.
    recv_waker: Option<Waker>,

    /// Wakers on the sending side of the buffer.
    ///
    /// The length of this vec should match the number of input partitions going
    /// into the repartitioning operator.
    ///
    /// Only the latest waker for an input partition should be stored.
    send_wakers: Vec<Option<Waker>>,

    /// Boolean for if there's no more batches that will be produced.
    exhausted: bool,
}

impl BatchBufferInner {
    /// Try to wake up to n wakers on the send side.
    fn wake_n_senders(&mut self, mut n: usize) {
        assert_ne!(0, n);

        for waker in self.send_wakers.iter_mut() {
            if let Some(waker) = waker.take() {
                waker.wake();
                n -= 1;
                if n == 0 {
                    return;
                }
            }
        }
    }

    /// Wake up all senders on the send side.
    fn wake_all_senders(&mut self) {
        for waker in self.send_wakers.iter_mut() {
            if let Some(waker) = waker.take() {
                waker.wake();
            }
        }
    }
}

impl Explainable for PhysicalRoundRobinRepartition {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("RoundRobinRepartition")
    }
}

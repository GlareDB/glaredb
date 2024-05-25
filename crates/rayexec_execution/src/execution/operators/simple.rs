use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::fmt::Debug;
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct SimplePartitionState {
    /// A batch that's waiting to be pulled.
    buffered: Option<Batch>,

    /// Waker on the pull side.
    ///
    /// Waken when batch is placed into `buffered`.
    pull_waker: Option<Waker>,

    /// Waker on the push side.
    ///
    /// Waken when batch is take out of `buffered`.
    push_waker: Option<Waker>,

    /// Mark if this partition is exhausted.
    ///
    /// Needed to propagate batch exhaustion through the pipelines.
    exhausted: bool,
}

impl Default for SimplePartitionState {
    fn default() -> Self {
        Self::new()
    }
}

impl SimplePartitionState {
    pub fn new() -> Self {
        SimplePartitionState {
            buffered: None,
            pull_waker: None,
            push_waker: None,
            exhausted: false,
        }
    }
}

/// A stateless operation on a batch.
pub trait StatelessOperation: Sync + Send + Debug {
    fn execute(&self, batch: Batch) -> Result<Batch>;
}

#[derive(Debug)]
pub struct SimpleOperator<S> {
    operation: S,
}

impl<S: StatelessOperation> SimpleOperator<S> {
    pub fn new(operation: S) -> Self {
        SimpleOperator { operation }
    }
}

impl<S: StatelessOperation> PhysicalOperator for SimpleOperator<S> {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let state = match partition_state {
            PartitionState::Simple(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        // Need to wait for buffered batch to be pulled.
        if state.buffered.is_some() {
            state.push_waker = Some(cx.waker().clone());
            if let Some(waker) = state.pull_waker.take() {
                waker.wake();
            }
            return Ok(PollPush::Pending(batch));
        }

        // Otherwise we're good to go.
        let out = self.operation.execute(batch)?;

        state.buffered = Some(out);
        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(PollPush::Pushed)
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<()> {
        let state = match partition_state {
            PartitionState::Simple(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.exhausted = true;

        if let Some(waker) = state.pull_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::Simple(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.buffered.take() {
            Some(out) => {
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }
                Ok(PollPull::Batch(out))
            }
            None => {
                if state.exhausted {
                    return Ok(PollPull::Exhausted);
                }

                state.pull_waker = Some(cx.waker().clone());
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }
                Ok(PollPull::Pending)
            }
        }
    }
}

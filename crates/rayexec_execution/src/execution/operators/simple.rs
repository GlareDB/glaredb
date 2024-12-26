use std::fmt::Debug;
use std::sync::Arc;
use std::task::{Context, Waker};

use crate::arrays::batch::Batch;
use rayexec_error::Result;

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
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

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
pub trait StatelessOperation: Sync + Send + Debug + Explainable {
    fn execute(&self, batch: Batch) -> Result<Batch>;
}

/// A simple operator is an operator that wraps a function that requires no
/// state. Batch goes in, batch comes out.
///
/// Filter and Project are both examples that use this.
#[derive(Debug)]
pub struct SimpleOperator<S> {
    pub(crate) operation: S,
}

impl<S: StatelessOperation> SimpleOperator<S> {
    pub fn new(operation: S) -> Self {
        SimpleOperator { operation }
    }
}

impl<S: StatelessOperation> ExecutableOperator for SimpleOperator<S> {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: (0..partitions[0])
                    .map(|_| PartitionState::Simple(SimplePartitionState::new()))
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

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::Simple(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.exhausted = true;

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
            PartitionState::Simple(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.buffered.take() {
            Some(out) => {
                if let Some(waker) = state.push_waker.take() {
                    waker.wake();
                }
                Ok(PollPull::Computed(out.into()))
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

impl<S: StatelessOperation> Explainable for SimpleOperator<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.operation.explain_entry(conf)
    }
}

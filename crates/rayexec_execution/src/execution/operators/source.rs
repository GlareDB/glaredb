use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
};

use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use std::fmt::Debug;

use super::{
    util::futures::make_static, ExecutableOperator, ExecutionStates, InputOutputStates,
    OperatorState, PartitionState, PollFinalize, PollPull, PollPush,
};

/// Operation for reading batches from somewhere.
pub trait SourceOperation: Debug + Send + Sync + Explainable {
    /// Create an exact number of partition sources for the query.
    ///
    /// This is called once during executable pipeline planning.
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>>;

    /// Return an optional partitioning requirement for this source.
    fn partition_requirement(&self) -> Option<usize>;
}

impl SourceOperation for Box<dyn SourceOperation> {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>> {
        self.as_ref().create_partition_sources(num_sources)
    }

    fn partition_requirement(&self) -> Option<usize> {
        self.as_ref().partition_requirement()
    }
}

impl Explainable for Box<dyn SourceOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
}

pub trait PartitionSource: Debug + Send {
    /// Pull the enxt batch from the source.
    ///
    /// Returns None when there's no batches remaining in the source.
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}

pub struct SourcePartitionState {
    source: Box<dyn PartitionSource>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for SourcePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuerySourcePartitionState")
            .finish_non_exhaustive()
    }
}

/// Operater for reading batches from a source.
// TODO: Deduplicate with table scan and table function scan.
#[derive(Debug)]
pub struct SourceOperator<S: SourceOperation> {
    pub(crate) source: S,
}

impl<S: SourceOperation> SourceOperator<S> {
    pub fn new(source: S) -> Self {
        SourceOperator { source }
    }
}

impl<S: SourceOperation> ExecutableOperator for SourceOperator<S> {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let states = self
            .source
            .create_partition_sources(partitions[0])
            .into_iter()
            .map(|source| {
                PartitionState::Source(SourcePartitionState {
                    source,
                    future: None,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::None),
            partition_states: InputOutputStates::OneToOne {
                partition_states: states,
            },
        })
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        _batch: Batch,
    ) -> Result<PollPush> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Err(RayexecError::new("Cannot push to physical scan"))
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Source(state) => {
                if let Some(future) = &mut state.future {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(Some(batch))) => {
                            state.future = None; // Future complete, next pull with create a new one.
                            return Ok(PollPull::Batch(batch));
                        }
                        Poll::Ready(Ok(None)) => return Ok(PollPull::Exhausted),
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(PollPull::Pending),
                    }
                }

                let mut future = state.source.pull();
                match future.poll_unpin(cx) {
                    Poll::Ready(Ok(Some(batch))) => Ok(PollPull::Batch(batch)),
                    Poll::Ready(Ok(None)) => Ok(PollPull::Exhausted),
                    Poll::Ready(Err(e)) => Err(e),
                    Poll::Pending => {
                        // SAFETY: Source lives on the partition state and
                        // outlives this future.
                        state.future = Some(unsafe { make_static(future) });
                        Ok(PollPull::Pending)
                    }
                }
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl<S: SourceOperation> Explainable for SourceOperator<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.source.explain_entry(conf)
    }
}

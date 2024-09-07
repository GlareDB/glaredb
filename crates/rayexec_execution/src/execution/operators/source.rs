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

pub trait QuerySource: Debug + Send + Sync + Explainable {
    fn create_partition_sources(&self, num_sources: usize) -> Vec<Box<dyn PartitionSource>>;
    fn partition_requirement(&self) -> Option<usize>;
}

pub trait PartitionSource: Debug + Send {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>>;
}

pub struct QuerySourcePartitionState {
    source: Box<dyn PartitionSource>,
    /// In progress pull we're working on.
    future: Option<BoxFuture<'static, Result<Option<Batch>>>>,
}

impl fmt::Debug for QuerySourcePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuerySourcePartitionState")
            .finish_non_exhaustive()
    }
}

// TODO: Deduplicate with table scan and table function scan.
#[derive(Debug)]
pub struct PhysicalQuerySource {
    source: Box<dyn QuerySource>,
}

impl PhysicalQuerySource {
    pub fn new(source: Box<dyn QuerySource>) -> Self {
        PhysicalQuerySource { source }
    }
}

impl ExecutableOperator for PhysicalQuerySource {
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
                PartitionState::QuerySource(QuerySourcePartitionState {
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
            PartitionState::QuerySource(state) => {
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

impl Explainable for PhysicalQuerySource {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.source.explain_entry(conf)
    }
}

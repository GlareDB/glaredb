use crate::{
    database::DatabaseContext,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;
use std::task::{Context, Waker};
use std::{
    fmt::{self, Debug},
    task::Poll,
};

use super::{
    util::futures::make_static, ExecutableOperator, ExecutionStates, InputOutputStates,
    OperatorState, PartitionState, PollFinalize, PollPull, PollPush,
};

pub trait QuerySink: Debug + Send + Sync + Explainable {
    /// Create an exact number of partition sinks for the query.
    ///
    /// This is guaranteed to only be called once during pipeline execution.
    fn create_partition_sinks(&self, num_sinks: usize) -> Vec<Box<dyn PartitionSink>>;

    fn partition_requirement(&self) -> Option<usize>;
}

pub trait PartitionSink: Debug + Send {
    /// Push a batch to the sink.
    ///
    /// Batches are pushed in the order they're received in.
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>>;

    /// Finalize the sink.
    ///
    /// Called once only after all batches have been pushed. If there's any
    /// pending work that needs to happen (flushing), it should happen here.
    /// Once this returns, the sink is complete.
    fn finalize(&mut self) -> BoxFuture<'_, Result<()>>;
}

pub enum QuerySinkPartitionState {
    Writing {
        inner: Option<QuerySinkInnerPartitionState>,
        /// Future we're working on for a pending write.
        future: Option<BoxFuture<'static, Result<()>>>,
    },
    Finalizing {
        inner: Option<QuerySinkInnerPartitionState>,
        /// Future we're working on for a pending finalize. This should never be
        /// None, as if there's nothing for us to do, the state should be in
        /// `Finished`.
        future: BoxFuture<'static, Result<()>>,
    },
    Finished,
}

impl fmt::Debug for QuerySinkPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuerySinkPartitionState")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct QuerySinkInnerPartitionState {
    sink: Box<dyn PartitionSink>,
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalQuerySink {
    sink: Box<dyn QuerySink>,
}

impl PhysicalQuerySink {
    pub fn new(sink: Box<dyn QuerySink>) -> Self {
        PhysicalQuerySink { sink }
    }
}

impl ExecutableOperator for PhysicalQuerySink {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let states: Vec<_> = self
            .sink
            .create_partition_sinks(partitions)
            .into_iter()
            .map(|sink| {
                PartitionState::QuerySink(QuerySinkPartitionState::Writing {
                    inner: Some(QuerySinkInnerPartitionState {
                        sink,
                        pull_waker: None,
                    }),
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
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::QuerySink(state) => match state {
                QuerySinkPartitionState::Writing { inner, future } => {
                    if let Some(curr_future) = future {
                        match curr_future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => {
                                // Future complete, unset and continue on.
                                //
                                // Unsetting is required here to avoid polling a
                                // completed future in the case of returning
                                // early due to a batch with 0 rows.
                                *future = None;
                            }
                            Poll::Ready(Err(e)) => return Err(e),
                            Poll::Pending => return Ok(PollPush::Pending(batch)),
                        }
                    }

                    // A "workaround" for the below hack. Not strictly
                    // necessary, but it makes me a feel a bit better than the
                    // hacky stuff is localized to just here.
                    if batch.num_rows() == 0 {
                        return Ok(PollPush::NeedsMore);
                    }

                    let mut push_future = inner.as_mut().unwrap().sink.push(batch);
                    match push_future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // Future completed, need more batches.
                            Ok(PollPush::NeedsMore)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // We successfully pushed.

                            // SAFETY: Lifetime of the CopyToSink (on the
                            // partition state) outlives the future.
                            *future = Some(unsafe { make_static(push_future) });

                            // TODO: This is a bit hacky. But we want to keep calling poll push
                            // until the future completes. I think the signature for poll push
                            // might change a bit a accommodate this.
                            //
                            // I think we'll want to do a similar thing for inserts so that
                            // we can implement them as "just" async functions.
                            Ok(PollPush::Pending(Batch::empty()))
                        }
                    }
                }
                other => Err(RayexecError::new(format!(
                    "CopyTo operator in wrong state: {other:?}"
                ))),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::QuerySink(state) => match state {
                QuerySinkPartitionState::Writing { inner, future } => {
                    if let Some(curr_future) = future {
                        // Still a writing future that needs to complete.
                        match curr_future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => {
                                // Continue on to flipping the state.
                                *future = None;
                            }
                            Poll::Ready(Err(e)) => return Err(e),
                            Poll::Pending => return Ok(PollFinalize::Pending),
                        }
                    }

                    let mut inner = inner.take().unwrap();
                    let mut finalize_future = inner.sink.finalize();
                    match finalize_future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // We're done.
                            if let Some(waker) = inner.pull_waker.take() {
                                waker.wake();
                            }
                            *state = QuerySinkPartitionState::Finished;

                            Ok(PollFinalize::Finalized)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // Waiting...

                            // SAFETY: Lifetime of copy to sink outlives this future.
                            let future = unsafe { make_static(finalize_future) };

                            *state = QuerySinkPartitionState::Finalizing {
                                inner: Some(inner),
                                future,
                            };

                            Ok(PollFinalize::Pending)
                        }
                    }
                }
                QuerySinkPartitionState::Finalizing { inner, future } => {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // We're done.
                            if let Some(waker) = inner.as_mut().unwrap().pull_waker.take() {
                                waker.wake();
                            }

                            *state = QuerySinkPartitionState::Finished;
                            Ok(PollFinalize::Finalized)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => Ok(PollFinalize::Pending),
                    }
                }
                other => Err(RayexecError::new(format!(
                    "CopyTo operator in wrong state: {other:?}"
                ))),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::QuerySink(state) => match state {
                QuerySinkPartitionState::Writing { inner, .. }
                | QuerySinkPartitionState::Finalizing { inner, .. } => {
                    if let Some(inner) = inner.as_mut() {
                        inner.pull_waker = Some(cx.waker().clone());
                    }
                    Ok(PollPull::Pending)
                }
                QuerySinkPartitionState::Finished => Ok(PollPull::Exhausted),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalQuerySink {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.sink.explain_entry(conf)
    }
}

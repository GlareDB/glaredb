use std::fmt::{self, Debug};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures::future::BoxFuture;
use futures::FutureExt;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};

use super::util::futures::make_static;
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
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Operation for sending batches somewhere.
pub trait SinkOperation: Debug + Send + Sync + Explainable {
    /// Create an exact number of partition sinks for the query.
    ///
    /// This is guaranteed to only be called once during pipeline execution.
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>>;

    /// Return an optional partitioning requirement for this sink.
    ///
    /// Called during executable pipeline planning.
    fn partition_requirement(&self) -> Option<usize>;
}

impl SinkOperation for Box<dyn SinkOperation> {
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        self.as_ref().create_partition_sinks(context, num_sinks)
    }

    fn partition_requirement(&self) -> Option<usize> {
        self.as_ref().partition_requirement()
    }
}

impl Explainable for Box<dyn SinkOperation> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.as_ref().explain_entry(conf)
    }
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

pub enum SinkPartitionState {
    Writing {
        inner: Option<SinkInnerPartitionState>,
        /// Future we're working on for a pending write.
        future: Option<BoxFuture<'static, Result<()>>>,
    },
    Finalizing {
        inner: Option<SinkInnerPartitionState>,
        /// Future we're working on for a pending finalize. This should never be
        /// None, as if there's nothing for us to do, the state should be in
        /// `Finished`.
        future: BoxFuture<'static, Result<()>>,
    },
    Finished,
}

impl fmt::Debug for SinkPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkPartitionState").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct SinkInnerPartitionState {
    sink: Box<dyn PartitionSink>,
    pull_waker: Option<Waker>,
    current_row_count: usize,
}

#[derive(Debug)]
pub struct SinkOperatorState {
    inner: Mutex<SinkOperatorStateInner>,
}

#[derive(Debug)]
struct SinkOperatorStateInner {
    /// Row count from all partitions.
    global_row_count: usize,

    /// If we've already returned the global row count.
    global_row_count_returned: bool,

    /// Number of partitions still writing to the sink.
    ///
    /// The last partition to complete should write out the global row count.
    partitions_remaining: usize,
}

/// An operator that writes batches to a partition sink.
///
/// The output batch for this operator is a single column containing the number
/// of rows passed through this operator.
///
/// Insert, CopyTo, CreateTable all use this. CreateTable uses this to enable
/// CTAS semantics easily.
#[derive(Debug)]
pub struct SinkOperator<S: SinkOperation> {
    pub(crate) sink: S,
}

impl<S: SinkOperation> SinkOperator<S> {
    pub fn new(sink: S) -> Self {
        SinkOperator { sink }
    }
}

impl<S: SinkOperation> ExecutableOperator for SinkOperator<S> {
    fn create_states(
        &self,
        context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let states: Vec<_> = self
            .sink
            .create_partition_sinks(context, partitions)?
            .into_iter()
            .map(|sink| {
                PartitionState::Sink(SinkPartitionState::Writing {
                    inner: Some(SinkInnerPartitionState {
                        sink,
                        pull_waker: None,
                        current_row_count: 0,
                    }),
                    future: None,
                })
            })
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::Sink(SinkOperatorState {
                inner: Mutex::new(SinkOperatorStateInner {
                    global_row_count: 0,
                    global_row_count_returned: false,
                    partitions_remaining: partitions,
                }),
            })),
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
            PartitionState::Sink(state) => match state {
                SinkPartitionState::Writing { inner, future } => {
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

                    let inner = inner.as_mut().unwrap();
                    inner.current_row_count += batch.num_rows();

                    let mut push_future = inner.sink.push(batch);
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
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::Sink(state) => match state {
                SinkPartitionState::Writing { inner, future } => {
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
                            // TODO: This is duplicated iwth the below `Finalizing` match arm.

                            // We're done.
                            if let Some(waker) = inner.pull_waker.take() {
                                waker.wake();
                            }

                            match operator_state {
                                OperatorState::Sink(state) => {
                                    let mut state = state.inner.lock();
                                    state.global_row_count += inner.current_row_count;
                                    state.partitions_remaining -= 1;
                                }
                                other => panic!("invalid operator state: {other:?}"),
                            }

                            *state = SinkPartitionState::Finished;

                            Ok(PollFinalize::Finalized)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // Waiting...

                            // SAFETY: Lifetime of copy to sink outlives this future.
                            let future = unsafe { make_static(finalize_future) };

                            *state = SinkPartitionState::Finalizing {
                                inner: Some(inner),
                                future,
                            };

                            Ok(PollFinalize::Pending)
                        }
                    }
                }
                SinkPartitionState::Finalizing { inner, future } => {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // We're done.
                            if let Some(waker) = inner.as_mut().unwrap().pull_waker.take() {
                                waker.wake();
                            }

                            match operator_state {
                                OperatorState::Sink(state) => {
                                    let mut state = state.inner.lock();
                                    state.global_row_count +=
                                        inner.as_ref().unwrap().current_row_count;
                                    state.partitions_remaining -= 1;
                                }
                                other => panic!("invalid operator state: {other:?}"),
                            }

                            *state = SinkPartitionState::Finished;

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
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        match partition_state {
            PartitionState::Sink(state) => match state {
                SinkPartitionState::Writing { inner, .. }
                | SinkPartitionState::Finalizing { inner, .. } => {
                    if let Some(inner) = inner.as_mut() {
                        inner.pull_waker = Some(cx.waker().clone());
                    }
                    Ok(PollPull::Pending)
                }
                SinkPartitionState::Finished => {
                    let mut shared = match operator_state {
                        OperatorState::Sink(state) => state.inner.lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    if shared.global_row_count_returned {
                        return Ok(PollPull::Exhausted);
                    }

                    if shared.partitions_remaining == 0 {
                        shared.global_row_count_returned = true;

                        let row_count = shared.global_row_count as u64;

                        let row_count_batch =
                            Batch::try_from_arrays([Array::from_iter([row_count])])?;

                        return Ok(PollPull::Computed(row_count_batch.into()));
                    }

                    Ok(PollPull::Exhausted)
                }
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl<S: SinkOperation> Explainable for SinkOperator<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.sink.explain_entry(conf)
    }
}

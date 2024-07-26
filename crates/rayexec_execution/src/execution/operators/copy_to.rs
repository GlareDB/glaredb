use crate::{
    functions::copy::{CopyToFunction, CopyToSink},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::{batch::Batch, field::Schema};
use rayexec_error::{RayexecError, Result};
use rayexec_io::location::FileLocation;
use std::task::{Context, Waker};
use std::{fmt, task::Poll};

use super::{
    util::futures::make_static, OperatorState, PartitionState, PhysicalOperator, PollFinalize,
    PollPull, PollPush,
};

pub enum CopyToPartitionState {
    Writing {
        inner: Option<CopyToInnerPartitionState>,
        /// Future we're working on for a pending write.
        future: Option<BoxFuture<'static, Result<()>>>,
    },
    Finalizing {
        inner: Option<CopyToInnerPartitionState>,
        /// Future we're working on for a pending finalize. This should never be
        /// None, as if there's nothing for us to do, the state should be in
        /// `Finished`.
        future: BoxFuture<'static, Result<()>>,
    },
    Finished,
}

impl fmt::Debug for CopyToPartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyToPartitionState")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct CopyToInnerPartitionState {
    sink: Box<dyn CopyToSink>,
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct PhysicalCopyTo {
    copy_to: Box<dyn CopyToFunction>,
    location: FileLocation,
}

impl PhysicalCopyTo {
    pub fn new(copy_to: Box<dyn CopyToFunction>, location: FileLocation) -> Self {
        PhysicalCopyTo { copy_to, location }
    }

    // TODO: Only allows a single input partition for now. Multiple partitions
    // would required writing to separate files. We'd want to append the
    // partition number to file location, but exact behavior is still tbd.
    pub fn try_create_states(
        &self,
        schema: Schema,
        num_partitions: usize,
    ) -> Result<Vec<CopyToPartitionState>> {
        if num_partitions != 1 {
            return Err(RayexecError::new(
                "CopyTo operator only supports a single partition for now",
            ));
        }

        let states = self
            .copy_to
            .create_sinks(schema, self.location.clone(), num_partitions)?
            .into_iter()
            .map(|sink| CopyToPartitionState::Writing {
                inner: Some(CopyToInnerPartitionState {
                    sink,
                    pull_waker: None,
                }),
                future: None,
            })
            .collect::<Vec<_>>();

        Ok(states)
    }
}

impl PhysicalOperator for PhysicalCopyTo {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing { inner, future } => {
                    if let Some(future) = future {
                        match future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => (), // Continue on.
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
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing { inner, future } => {
                    if let Some(future) = future {
                        // Still a writing future that needs to complete.
                        match future.poll_unpin(cx) {
                            Poll::Ready(Ok(_)) => (), // Continue on to flipping the state.
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
                            *state = CopyToPartitionState::Finished;

                            Ok(PollFinalize::Finalized)
                        }
                        Poll::Ready(Err(e)) => Err(e),
                        Poll::Pending => {
                            // Waiting...

                            // SAFETY: Lifetime of copy to sink outlives this future.
                            let future = unsafe { make_static(finalize_future) };

                            *state = CopyToPartitionState::Finalizing {
                                inner: Some(inner),
                                future,
                            };

                            Ok(PollFinalize::Pending)
                        }
                    }
                }
                CopyToPartitionState::Finalizing { inner, future } => {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(_)) => {
                            // We're done.
                            if let Some(waker) = inner.as_mut().unwrap().pull_waker.take() {
                                waker.wake();
                            }

                            *state = CopyToPartitionState::Finished;
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
            PartitionState::CopyTo(state) => match state {
                CopyToPartitionState::Writing { inner, .. }
                | CopyToPartitionState::Finalizing { inner, .. } => {
                    if let Some(inner) = inner.as_mut() {
                        inner.pull_waker = Some(cx.waker().clone());
                    }
                    Ok(PollPull::Pending)
                }
                CopyToPartitionState::Finished => Ok(PollPull::Exhausted),
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalCopyTo {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CopyTo")
    }
}

use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::task::{Context, Waker};

use super::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};

#[derive(Debug)]
pub struct UnionTopPartitionState {
    partition_idx: usize,
    batch: Option<Batch>,
    finished: bool,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

#[derive(Debug)]
pub struct UnionBottomPartitionState {
    partition_idx: usize,
}

#[derive(Debug)]
pub struct UnionOperatorState {
    shared: Vec<Mutex<SharedPartitionState>>,
}

#[derive(Debug)]
struct SharedPartitionState {
    batch: Option<Batch>,
    finished: bool,
    push_waker: Option<Waker>,
    pull_waker: Option<Waker>,
}

/// Unions two input operations.
///
/// The "top" input operator acts as a normal operator, and so the "top"
/// partition state is used for pushing and pulling.
///
/// The "bottom" input operator treats this operator as a sink, and will only
/// push. Every push from bottom will be writing to the global state.
///
/// The current implemenation prefers taking from the "top".
///
/// "top" and "bottom" are expected to have the same number of partitions.
#[derive(Debug)]
pub struct PhysicalUnion;

impl PhysicalUnion {
    pub fn create_states(
        &self,
        num_partitions: usize,
    ) -> (
        UnionOperatorState,
        Vec<UnionTopPartitionState>,
        Vec<UnionBottomPartitionState>,
    ) {
        let top_states = (0..num_partitions)
            .map(|idx| UnionTopPartitionState {
                partition_idx: idx,
                batch: None,
                finished: false,
                push_waker: None,
                pull_waker: None,
            })
            .collect();

        let bottom_states = (0..num_partitions)
            .map(|idx| UnionBottomPartitionState { partition_idx: idx })
            .collect();

        let operator_state = UnionOperatorState {
            shared: (0..num_partitions)
                .map(|_| {
                    Mutex::new(SharedPartitionState {
                        batch: None,
                        finished: false,
                        push_waker: None,
                        pull_waker: None,
                    })
                })
                .collect(),
        };

        (operator_state, top_states, bottom_states)
    }
}

impl PhysicalOperator for PhysicalUnion {
    fn poll_push(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        match partition_state {
            PartitionState::UnionTop(state) => {
                if state.batch.is_some() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }
                state.batch = Some(batch);

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPush::Pushed)
            }

            PartitionState::UnionBottom(state) => {
                let mut shared = match operator_state {
                    OperatorState::Union(operator_state) => {
                        operator_state.shared[state.partition_idx].lock()
                    }
                    other => panic!("invalid operator state: {other:?}"),
                };

                if shared.batch.is_some() {
                    shared.push_waker = Some(cx.waker().clone());
                    return Ok(PollPush::Pending(batch));
                }

                shared.batch = Some(batch);

                if let Some(waker) = shared.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPush::Pushed)
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn finalize_push(
        &self,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<()> {
        match partition_state {
            PartitionState::UnionTop(state) => {
                state.finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }
                Ok(())
            }

            PartitionState::UnionBottom(state) => {
                let mut shared = match operator_state {
                    OperatorState::Union(operator_state) => {
                        operator_state.shared[state.partition_idx].lock()
                    }
                    other => panic!("invalid operator state: {other:?}"),
                };

                shared.finished = true;
                if let Some(waker) = shared.pull_waker.take() {
                    waker.wake();
                }

                Ok(())
            }

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
            PartitionState::UnionTop(state) => match state.batch.take() {
                Some(batch) => {
                    if let Some(waker) = state.push_waker.take() {
                        waker.wake();
                    }
                    Ok(PollPull::Batch(batch))
                }
                None => {
                    let mut shared = match operator_state {
                        OperatorState::Union(operator_state) => {
                            operator_state.shared[state.partition_idx].lock()
                        }
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    // Check if we received batch from bottom.
                    if let Some(batch) = shared.batch.take() {
                        if let Some(waker) = shared.push_waker.take() {
                            waker.wake();
                        }
                        return Ok(PollPull::Batch(batch));
                    }

                    // If not, check if we're finished.
                    if shared.finished && state.finished {
                        return Ok(PollPull::Exhausted);
                    }

                    // No batches, and we're not finished. Need to wait.
                    shared.pull_waker = Some(cx.waker().clone());
                    if let Some(waker) = shared.push_waker.take() {
                        waker.wake();
                    }

                    Ok(PollPull::Pending)
                }
            },
            other => panic!("invalid partition state: {other:?}"),
        }
    }
}

impl Explainable for PhysicalUnion {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Union")
    }
}

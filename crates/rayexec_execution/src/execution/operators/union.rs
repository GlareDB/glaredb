use std::sync::Arc;
use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

use super::{
    ExecutableOperatorOld,
    ExecutionStates,
    InputOutputStates,
    OperatorStateOld,
    PartitionStateOld,
    PollFinalizeOld,
    PollPullOld,
    PollPushOld,
};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub struct UnionTopPartitionState {
    partition_idx: usize,
    batch: Option<BatchOld>,
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
    batch: Option<BatchOld>,
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
/// The current implementation prefers taking from the "top".
///
/// "top" and "bottom" are expected to have the same number of partitions.
#[derive(Debug)]
pub struct PhysicalUnion;

impl PhysicalUnion {
    /// Index of the input corresponding to the top of the union.
    pub const UNION_TOP_INPUT_INDEX: usize = 0;
    /// Index of the input corresponding to the bottom of the union.
    pub const UNION_BOTTOM_INPUT_INDEX: usize = 1;
}

impl ExecutableOperatorOld for PhysicalUnion {
    fn create_states_old(&self, _context: &DatabaseContext, partitions: Vec<usize>) -> Result<ExecutionStates> {
        let num_partitions = partitions[0];

        let top_states = (0..num_partitions)
            .map(|idx| {
                PartitionStateOld::UnionTop(UnionTopPartitionState {
                    partition_idx: idx,
                    batch: None,
                    finished: false,
                    push_waker: None,
                    pull_waker: None,
                })
            })
            .collect();

        let bottom_states = (0..num_partitions)
            .map(|idx| PartitionStateOld::UnionBottom(UnionBottomPartitionState { partition_idx: idx }))
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

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorStateOld::Union(operator_state)),
            partition_states: InputOutputStates::NaryInputSingleOutput {
                partition_states: vec![top_states, bottom_states],
                pull_states: Self::UNION_TOP_INPUT_INDEX,
            },
        })
    }

    fn poll_push_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        operator_state: &OperatorStateOld,
        batch: BatchOld,
    ) -> Result<PollPushOld> {
        match partition_state {
            PartitionStateOld::UnionTop(state) => {
                if state.batch.is_some() {
                    state.push_waker = Some(cx.waker().clone());
                    return Ok(PollPushOld::Pending(batch));
                }
                state.batch = Some(batch);

                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPushOld::Pushed)
            }

            PartitionStateOld::UnionBottom(state) => {
                let mut shared = match operator_state {
                    OperatorStateOld::Union(operator_state) => operator_state.shared[state.partition_idx].lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                if shared.batch.is_some() {
                    shared.push_waker = Some(cx.waker().clone());
                    return Ok(PollPushOld::Pending(batch));
                }

                shared.batch = Some(batch);

                if let Some(waker) = shared.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollPushOld::Pushed)
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize_push_old(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        operator_state: &OperatorStateOld,
    ) -> Result<PollFinalizeOld> {
        match partition_state {
            PartitionStateOld::UnionTop(state) => {
                state.finished = true;
                if let Some(waker) = state.pull_waker.take() {
                    waker.wake();
                }
                Ok(PollFinalizeOld::Finalized)
            }

            PartitionStateOld::UnionBottom(state) => {
                let mut shared = match operator_state {
                    OperatorStateOld::Union(operator_state) => operator_state.shared[state.partition_idx].lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                shared.finished = true;
                if let Some(waker) = shared.pull_waker.take() {
                    waker.wake();
                }

                Ok(PollFinalizeOld::Finalized)
            }

            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_pull_old(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionStateOld,
        operator_state: &OperatorStateOld,
    ) -> Result<PollPullOld> {
        match partition_state {
            PartitionStateOld::UnionTop(state) => match state.batch.take() {
                Some(batch) => {
                    if let Some(waker) = state.push_waker.take() {
                        waker.wake();
                    }
                    Ok(PollPullOld::Computed(batch.into()))
                }
                None => {
                    let mut shared = match operator_state {
                        OperatorStateOld::Union(operator_state) => operator_state.shared[state.partition_idx].lock(),
                        other => panic!("invalid operator state: {other:?}"),
                    };

                    // Check if we received batch from bottom.
                    if let Some(batch) = shared.batch.take() {
                        if let Some(waker) = shared.push_waker.take() {
                            waker.wake();
                        }
                        return Ok(PollPullOld::Computed(batch.into()));
                    }

                    // If not, check if we're finished.
                    if shared.finished && state.finished {
                        return Ok(PollPullOld::Exhausted);
                    }

                    // No batches, and we're not finished. Need to wait.
                    shared.pull_waker = Some(cx.waker().clone());
                    if let Some(waker) = shared.push_waker.take() {
                        waker.wake();
                    }

                    Ok(PollPullOld::Pending)
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

impl DatabaseProtoConv for PhysicalUnion {
    type ProtoType = rayexec_proto::generated::execution::PhysicalUnion;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self)
    }
}

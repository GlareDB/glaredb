use std::task::{Context, Waker};

use parking_lot::Mutex;

use super::{ExecutableOperator, PartitionAndOperatorStates, PartitionState};
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;

#[derive(Debug)]
pub enum HashJoinBuildPartitionState {
    /// Partition is building.
    Building(InProgressBuildState),
    /// Partition finished building.
    Finished,
}

#[derive(Debug)]
pub struct InProgressBuildState {
    // build_data: HashedBlockCollection<NopBufferManager>,
}

#[derive(Debug)]
pub enum HashJoinProbePartitionState {
    /// Partition waiting for build side to complete.
    Waiting(usize),
    /// Partition is probing.
    Probing(ProbeState),
    /// Left-join drain state.
    Draining(DrainState),
    /// Probing finished.
    Finished,
}

#[derive(Debug)]
pub struct ProbeState {}

#[derive(Debug)]
pub struct DrainState {}

#[derive(Debug)]
pub struct HashJoinOperatorState {
    inner: Mutex<HashJoinOperatorStateInner>,
}

#[derive(Debug)]
struct HashJoinOperatorStateInner {
    /// Wakers from the probe side that are waiting for the build side to
    /// complete.
    ///
    /// Keyed by probe-side partition index.
    build_waiting_probers: Vec<Option<Waker>>,
}

#[derive(Debug)]
pub struct PhysicalHashJoin {
    /// Data types from the left (build) side of the join.
    left_types: Vec<DataType>,
    /// Data types from the right (probe) side of the join.
    right_types: Vec<DataType>,
}

impl ExecutableOperator for PhysicalHashJoin {
    fn create_states(
        &self,
        context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        unimplemented!()
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        match partition_state {
            PartitionState::HashJoinBuild2(state) => {
                let state = match state {
                    HashJoinBuildPartitionState::Building(state) => state,
                    HashJoinBuildPartitionState::Finished => return Ok(PollExecute::Exhausted), // TODO: Probably should error instead.
                };

                let batch = inout.input.required("input batch required")?;
                state
                    .build_data
                    .push_batch(&NopBufferManager, &self.left_types, batch)?;

                Ok(PollExecute::NeedsMore)
            }
            PartitionState::HashJoinProbe2(state) => {
                match state {
                    HashJoinProbePartitionState::Waiting(probe_idx) => {
                        // Still waiting for build side to complete, just need
                        // to register a waker.

                        let mut operator_state = match operator_state {
                            OperatorState::HashJoin(state) => state.inner.lock(),
                            other => panic!("invalid operator state: {other:?}"),
                        };

                        operator_state.build_waiting_probers[*probe_idx] = Some(cx.waker().clone());

                        Ok(PollExecute::Pending)
                    }
                    _ => unimplemented!(),
                }
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        unimplemented!()
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        unimplemented!()
    }
}

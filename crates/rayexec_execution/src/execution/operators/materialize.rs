use crate::{
    database::DatabaseContext,
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Waker},
};

use super::{
    ExecutionStates, InputOutputStates, OperatorState, PartitionState, PhysicalOperator,
    PollFinalize, PollPull, PollPush,
};

#[derive(Debug)]
pub struct MaterializePushPartitionState {}

#[derive(Debug)]
pub struct MaterializePullPartitionState {
    /// Which shared output state we should be looking at.
    pipeline_state_idx: usize,

    /// Index of this partition, used for storing a waker.
    partition_idx: usize,
}

#[derive(Debug)]
pub struct MaterializeOperatorState {
    /// Output states, one per pull pipeline (not partition).
    output_states: Vec<Mutex<SharedPipelineOutputState>>,
}

#[derive(Debug)]
struct SharedPipelineOutputState {
    /// Buffered batches.
    // TODO: Probably want a size limit for back pressure.
    batches: VecDeque<Batch>,

    /// If pushing is finished.
    finished: bool,

    /// Pending pull wakers.
    ///
    /// Indexed by pull partition idx.
    pull_wakers: Vec<Option<Waker>>,
}

/// In-memory batch materialization.
///
/// Single input pipeline, some number of output pipelines.
#[derive(Debug)]
pub struct PhysicalMaterialize {
    num_outputs: usize,
}

impl PhysicalMaterialize {
    pub fn new(num_outputs: usize) -> Self {
        PhysicalMaterialize { num_outputs }
    }
}

impl PhysicalOperator for PhysicalMaterialize {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        partitions: Vec<usize>,
    ) -> Result<ExecutionStates> {
        let partitions = partitions[0];

        let shared_states: Vec<_> = (0..self.num_outputs)
            .map(|_| SharedPipelineOutputState {
                batches: VecDeque::new(),
                finished: false,
                pull_wakers: vec![None; partitions],
            })
            .collect();

        let operator_state = MaterializeOperatorState {
            output_states: shared_states.into_iter().map(Mutex::new).collect(),
        };

        let mut pull_pipeline_states = Vec::with_capacity(self.num_outputs);
        for pipeline_state_idx in 0..self.num_outputs {
            let states: Vec<_> = (0..partitions)
                .map(|partition_idx| {
                    PartitionState::MaterializePull(MaterializePullPartitionState {
                        partition_idx,
                        pipeline_state_idx,
                    })
                })
                .collect();
            pull_pipeline_states.push(states)
        }

        let push_states = (0..partitions)
            .map(|_| PartitionState::MaterializePush(MaterializePushPartitionState {}))
            .collect();

        Ok(ExecutionStates {
            operator_state: Arc::new(OperatorState::Materialize(operator_state)),
            partition_states: InputOutputStates::SingleInputNaryOutput {
                push_states,
                pull_states: pull_pipeline_states,
            },
        })
    }

    fn poll_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        batch: Batch,
    ) -> Result<PollPush> {
        let operator_state = match operator_state {
            OperatorState::Materialize(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        // Place batch in each output pipeline state.
        for output_state in &operator_state.output_states {
            let mut shared = output_state.lock();
            shared.batches.push_back(batch.clone());

            for waker in shared.pull_wakers.iter_mut() {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        }

        Ok(PollPush::Pushed)
    }

    fn poll_finalize_push(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let operator_state = match operator_state {
            OperatorState::Materialize(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        for output_state in &operator_state.output_states {
            let mut shared = output_state.lock();
            shared.finished = true;

            for waker in shared.pull_wakers.iter_mut() {
                if let Some(waker) = waker.take() {
                    waker.wake();
                }
            }
        }

        Ok(PollFinalize::Finalized)
    }

    fn poll_pull(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollPull> {
        let state = match partition_state {
            PartitionState::MaterializePull(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        let operator_state = match operator_state {
            OperatorState::Materialize(state) => state,
            other => panic!("invalid operator state: {other:?}"),
        };

        // Pull from the shared state corresponding to the pipeline this
        // partition is in.
        let mut shared = operator_state.output_states[state.pipeline_state_idx].lock();

        match shared.batches.pop_front() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => {
                if shared.finished {
                    return Ok(PollPull::Exhausted);
                }

                shared.pull_wakers[state.partition_idx] = Some(cx.waker().clone());
                Ok(PollPull::Pending)
            }
        }
    }
}

impl Explainable for PhysicalMaterialize {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("PhysicalMaterialize")
    }
}

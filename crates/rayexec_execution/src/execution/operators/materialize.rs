use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};
use parking_lot::Mutex;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;
use std::{
    collections::VecDeque,
    task::{Context, Waker},
};

use super::{OperatorState, PartitionState, PhysicalOperator, PollFinalize, PollPull, PollPush};

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
pub struct PhysicalMaterialize;

impl PhysicalMaterialize {
    /// Create states for this operator.
    ///
    /// The len of `output_pipline_partitions` indicates the number of pipelines
    /// that will be pulling from the operator.
    ///
    /// Each usize in `output_pipeline_partitions` corresponds to the number of
    /// partitions for that pipeline.
    pub fn create_states(
        &self,
        input_partitions: usize,
        output_pipeline_partitions: Vec<usize>,
    ) -> (
        MaterializeOperatorState,
        Vec<MaterializePushPartitionState>,
        Vec<Vec<MaterializePullPartitionState>>,
    ) {
        let shared_states: Vec<_> = output_pipeline_partitions
            .iter()
            .map(|num_partitions| SharedPipelineOutputState {
                batches: VecDeque::new(),
                finished: false,
                pull_wakers: vec![None; *num_partitions],
            })
            .collect();

        let operator_state = MaterializeOperatorState {
            output_states: shared_states.into_iter().map(Mutex::new).collect(),
        };

        let mut pull_pipeline_states = Vec::with_capacity(output_pipeline_partitions.len());
        for (pipeline_state_idx, num_partitions) in
            output_pipeline_partitions.into_iter().enumerate()
        {
            let states: Vec<_> = (0..num_partitions)
                .map(|partition_idx| MaterializePullPartitionState {
                    partition_idx,
                    pipeline_state_idx,
                })
                .collect();
            pull_pipeline_states.push(states)
        }

        let push_states = (0..input_partitions)
            .map(|_| MaterializePushPartitionState {})
            .collect();

        (operator_state, push_states, pull_pipeline_states)
    }
}

impl PhysicalOperator for PhysicalMaterialize {
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

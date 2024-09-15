use crate::{
    execution::operators::PollFinalize,
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    runtime::time::{RuntimeInstant, Timer},
};

use crate::execution::operators::{
    ExecutableOperator, OperatorState, PartitionState, PollPull, PollPush,
};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
};
use tracing::trace;

use super::profiler::OperatorProfileData;

// TODO: Include intermedate pipeline to track lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(pub usize);

/// A pipeline represents execution across a sequence of operators.
///
/// Pipelines are made of multiple partition pipelines, where all partition
/// pipelines are doing the same work across the same operators, just in a
/// different partition.
#[derive(Debug)]
pub struct ExecutablePipeline {
    /// ID of this pipeline. Unique to the query graph.
    ///
    /// Informational only.
    #[allow(dead_code)]
    pub(crate) pipeline_id: PipelineId,

    /// Parition pipelines that make up this pipeline.
    pub(crate) partitions: Vec<ExecutablePartitionPipeline>,
}

impl ExecutablePipeline {
    pub(crate) fn new(pipeline_id: PipelineId, num_partitions: usize) -> Self {
        assert_ne!(0, num_partitions);
        let partitions = (0..num_partitions)
            .map(|partition| ExecutablePartitionPipeline::new(pipeline_id, partition))
            .collect();
        ExecutablePipeline {
            pipeline_id,
            partitions,
        }
    }

    /// Return number of partitions in this pipeline.
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Return the number of operators in this pipeline.
    pub fn num_operators(&self) -> usize {
        self.partitions
            .first()
            .expect("at least one partition")
            .operators
            .len()
    }

    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = ExecutablePartitionPipeline> {
        self.partitions.into_iter()
    }

    /// Push an operator onto the pipeline.
    ///
    /// This will push the operator along with its state onto each of the inner
    /// partition pipelines.
    ///
    /// `partition_states` are the unique states per partition and must equal
    /// the number of partitions in this pipeline.
    pub(crate) fn push_operator(
        &mut self,
        physical: Arc<dyn ExecutableOperator>,
        operator_state: Arc<OperatorState>,
        partition_states: Vec<PartitionState>,
    ) -> Result<()> {
        if partition_states.len() != self.num_partitions() {
            return Err(RayexecError::new(format!(
                "Invalid number of partition states, got: {}, expected: {}",
                partition_states.len(),
                self.num_partitions()
            )));
        }

        let operators = partition_states
            .into_iter()
            .map(|partition_state| OperatorWithState {
                physical: physical.clone(),
                operator_state: operator_state.clone(),
                partition_state,
                profile_data: OperatorProfileData::default(),
            });

        for (operator, partition_pipeline) in operators.zip(self.partitions.iter_mut()) {
            partition_pipeline.operators.push(operator)
        }

        Ok(())
    }
}

impl Explainable for ExecutablePipeline {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(format!("Pipeline {}", self.pipeline_id.0))
    }
}

/// A partition pipeline holds a sequence of operators along with the state for
/// a single partition.
///
/// This is the smallest unit of work as it relates to the scheduler.
#[derive(Debug)]
pub struct ExecutablePartitionPipeline {
    /// Information about the pipeline.
    ///
    /// Should only be used for generating profiling data.
    info: PartitionPipelineInfo,

    /// State of this pipeline.
    state: PipelinePartitionState,

    /// All operators part of this pipeline.
    ///
    /// Data batches flow from left to right.
    ///
    /// The left-most operator will only be pulled from, while the right most
    /// will only pushed to.
    operators: Vec<OperatorWithState>,

    /// Index to begin pulling from.
    ///
    /// Initially this is 0 (the pipeline source), but as operators become
    /// exhausted, this will be incremented to avoid pulling from an exhausted
    /// operator.
    pull_start_idx: usize,
}

impl ExecutablePartitionPipeline {
    fn new(pipeline: PipelineId, partition: usize) -> Self {
        ExecutablePartitionPipeline {
            info: PartitionPipelineInfo {
                pipeline,
                partition,
            },
            state: PipelinePartitionState::PullFrom { operator_idx: 0 },
            operators: Vec::new(),
            pull_start_idx: 0,
        }
    }

    /// Get the pipeline id for this partition pipeline.
    pub fn pipeline_id(&self) -> PipelineId {
        self.info.pipeline
    }

    /// Get the partition number for this partition pipeline.
    pub fn partition(&self) -> usize {
        self.info.partition
    }

    /// Get the current state of the pipeline.
    pub fn state(&self) -> &PipelinePartitionState {
        &self.state
    }

    pub fn operators(&self) -> &[OperatorWithState] {
        &self.operators
    }
}

/// Information about a partition pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PartitionPipelineInfo {
    pub pipeline: PipelineId,
    pub partition: usize,
}

#[derive(Debug)]
pub struct OperatorWithState {
    /// The underlying physical operator.
    physical: Arc<dyn ExecutableOperator>,

    /// The state that's shared across all partitions for this operator.
    operator_state: Arc<OperatorState>,

    /// The state for this operator that's exclusive to this partition.
    partition_state: PartitionState,

    /// Profile data for this operator.
    profile_data: OperatorProfileData,
}

impl OperatorWithState {
    pub fn physical_operator(&self) -> &dyn ExecutableOperator {
        self.physical.as_ref()
    }

    pub fn profile_data(&self) -> &OperatorProfileData {
        &self.profile_data
    }
}

#[derive(Clone)]
pub enum PipelinePartitionState {
    /// Need to pull from an operator.
    PullFrom { operator_idx: usize },

    /// Need to push to an operator.
    PushTo { batch: Batch, operator_idx: usize },

    /// Need to finalize a push to an operator.
    FinalizePush { operator_idx: usize },

    /// Pipeline is completed.
    Completed,
}

impl fmt::Debug for PipelinePartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PullFrom { operator_idx } => f
                .debug_struct("PullFrom")
                .field("operator_idx", operator_idx)
                .finish(),
            Self::PushTo { operator_idx, .. } => f
                .debug_struct("PushTo")
                .field("operator_idx", operator_idx)
                .finish(),
            Self::FinalizePush { operator_idx } => f
                .debug_struct("Finalize")
                .field("operator_idx", operator_idx)
                .finish(),
            Self::Completed => f.debug_struct("Completed").finish(),
        }
    }
}

impl ExecutablePartitionPipeline {
    /// Try to execute as much of the pipeline for this partition as possible.
    ///
    /// Loop through all operators, pushing data as far as we can until we get
    /// to a pending state, or we've completed the pipeline.
    ///
    /// When we reach a pending state (either pending pull or pending push), the
    /// state will be updated such that the next call to `poll_execute` will
    /// pick up where it left off.
    ///
    /// Once a batch has been pushed to the 'sink' operator (the last operator),
    /// the pull state gets reset such that this will begin pulling from the
    /// first non-exhausted operator.
    ///
    /// When an operator is exhausted (no more batches to pull), `finalize_push`
    /// is called on the _next_ operator, and we begin pulling from the _next_
    /// operator until it's exhausted.
    ///
    /// `PollPush::Break` is a special case in all of this. When we receive a
    /// break, it indicates that the operator should not receive any more input.
    /// We set the state to skip pulling from all previous operators even if
    /// they've not been exhausted. An example operator that would emit a Break
    /// is LIMIT.
    pub fn poll_execute<I>(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>>
    where
        I: RuntimeInstant,
    {
        trace!(
            pipeline_id = %self.info.pipeline.0,
            partition = %self.info.partition,
            "executing partition pipeline",
        );

        let state = &mut self.state;

        loop {
            match state {
                PipelinePartitionState::PullFrom { operator_idx } => {
                    let operator = self
                        .operators
                        .get_mut(*operator_idx)
                        .expect("operator to exist");

                    let timer = Timer::<I>::start();
                    let poll_pull = operator.physical.poll_pull(
                        cx,
                        &mut operator.partition_state,
                        &operator.operator_state,
                    );
                    let elapsed = timer.stop();
                    operator.profile_data.elapsed += elapsed;

                    match poll_pull {
                        Ok(PollPull::Batch(batch)) => {
                            // We got a batch, increment operator index to push
                            // it into the next operator.
                            operator.profile_data.rows_emitted += batch.num_rows();
                            *state = PipelinePartitionState::PushTo {
                                batch,
                                operator_idx: *operator_idx + 1,
                            };
                            continue;
                        }
                        Ok(PollPull::Pending) => {
                            return Poll::Pending;
                        }
                        Ok(PollPull::Exhausted) => {
                            // This operator is exhausted, we're never going to
                            // pull from it again.
                            self.pull_start_idx += 1;

                            // Finalize the next operator to indicate that it
                            // will no longer be receiving batch inputs.
                            *state = PipelinePartitionState::FinalizePush {
                                operator_idx: self.pull_start_idx,
                            };
                        }
                        Err(e) => {
                            // We received an error. Currently no way to
                            // recover, so just mark this as completed and
                            // assume the error gets bubbled up.
                            *state = PipelinePartitionState::Completed;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                PipelinePartitionState::FinalizePush { operator_idx } => {
                    let next_operator = self
                        .operators
                        .get_mut(*operator_idx)
                        .expect("next operator to exist");

                    let timer = Timer::<I>::start();
                    let poll_finalize = next_operator.physical.poll_finalize_push(
                        cx,
                        &mut next_operator.partition_state,
                        &next_operator.operator_state,
                    );
                    let elapsed = timer.stop();
                    next_operator.profile_data.elapsed += elapsed;

                    match poll_finalize {
                        Ok(PollFinalize::Finalized) => {
                            if self.pull_start_idx == self.operators.len() - 1 {
                                // This partition pipeline has been completely exhausted, and
                                // we've just finalized the "sink" operator. We're done.
                                *state = PipelinePartitionState::Completed;
                                continue;
                            }

                            // Otherwise we should now begin pulling from the
                            // next non-exhausted operator.
                            *state = PipelinePartitionState::PullFrom {
                                operator_idx: self.pull_start_idx,
                            };
                        }
                        Ok(PollFinalize::Pending) => return Poll::Pending,
                        Err(e) => {
                            // Erroring on finalize is not recoverable.
                            *state = PipelinePartitionState::Completed;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                PipelinePartitionState::PushTo {
                    batch,
                    operator_idx,
                } => {
                    // To satisfy ownership. State will be updated anyways.
                    let batch = std::mem::replace(batch, Batch::empty());

                    let operator = self
                        .operators
                        .get_mut(*operator_idx)
                        .expect("operator to exist");

                    operator.profile_data.rows_read += batch.num_rows();

                    let timer = Timer::<I>::start();
                    let poll_push = operator.physical.poll_push(
                        cx,
                        &mut operator.partition_state,
                        &operator.operator_state,
                        batch,
                    );
                    let elapsed = timer.stop();
                    operator.profile_data.elapsed += elapsed;

                    match poll_push {
                        Ok(PollPush::Pushed) => {
                            // We successfully pushed to the operator.
                            //
                            // If we pushed to last operator (the 'sink'), we
                            // should reset the pull process to begin the
                            // executing on the next batch.
                            if *operator_idx == self.operators.len() - 1 {
                                // Next iteration will pull from the first
                                // non-exhausted operator.
                                *state = PipelinePartitionState::PullFrom {
                                    operator_idx: self.pull_start_idx,
                                };
                            } else {
                                // Otherwise we should just pull from the
                                // operator we just pushed to.
                                *state = PipelinePartitionState::PullFrom {
                                    operator_idx: *operator_idx,
                                };
                            }
                            continue;
                        }
                        Ok(PollPush::Pending(batch)) => {
                            // Operator not ready to accept input.
                            //
                            // Waker has been registered, and this pipeline will
                            // get called again once the operator can take more
                            // input. In the mean time, the batch will just be
                            // hanging out on this pipeline's state.
                            *state = PipelinePartitionState::PushTo {
                                batch,
                                operator_idx: *operator_idx,
                            };
                            return Poll::Pending;
                        }
                        Ok(PollPush::Break) => {
                            // Operator has received everything it needs. Set
                            // the pipeline to start pulling from the operator,
                            // even if the operator we're currently pull from
                            // has not been exhausted.
                            //
                            // An example use of the Break is the LIMIT
                            // operator. It needs a way to signal that it needs
                            // no more batches.
                            self.pull_start_idx = *operator_idx;
                            *state = PipelinePartitionState::PullFrom {
                                operator_idx: *operator_idx,
                            };
                            continue;
                        }
                        Ok(PollPush::NeedsMore) => {
                            // Operator accepted input, but needs more input
                            // before it will produce output.
                            //
                            // Reset the state to pull from the start of the
                            // pipline to produce more batches.
                            assert_ne!(0, *operator_idx);
                            *state = PipelinePartitionState::PullFrom {
                                operator_idx: self.pull_start_idx,
                            };
                            continue;
                        }
                        Err(e) => {
                            // Errors currently unrecoverable.
                            *state = PipelinePartitionState::Completed;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                PipelinePartitionState::Completed => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

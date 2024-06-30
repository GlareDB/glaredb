use crate::logical::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::operators::{OperatorState, PartitionState, PhysicalOperator, PollPull, PollPush};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::{
    fmt,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tracing::trace;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(pub usize);

/// A pipeline represents execution across a sequence of operators.
///
/// Pipelines are made of multiple partition pipelines, where all partition
/// pipelines are doing the same work across the same operators, just in a
/// different partition.
#[derive(Debug)]
pub struct Pipeline {
    /// ID of this pipeline. Unique to the query graph.
    ///
    /// Informational only.
    #[allow(dead_code)]
    pub(crate) pipeline_id: PipelineId,

    /// Parition pipelines that make up this pipeline.
    pub(crate) partitions: Vec<PartitionPipeline>,
}

impl Pipeline {
    pub(crate) fn new(pipeline_id: PipelineId, num_partitions: usize) -> Self {
        assert_ne!(0, num_partitions);
        let partitions = (0..num_partitions)
            .map(|partition| PartitionPipeline::new(pipeline_id, partition))
            .collect();
        Pipeline {
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

    pub fn into_partition_pipeline_iter(self) -> impl Iterator<Item = PartitionPipeline> {
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
        physical: Arc<dyn PhysicalOperator>,
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
            });

        for (operator, partition_pipeline) in operators.zip(self.partitions.iter_mut()) {
            partition_pipeline.operators.push(operator)
        }

        Ok(())
    }

    /// Return an iterator over all operators in the pipeline.
    ///
    /// Operators are ordered from the the "source" operator (operator at index
    /// 0) to the "sink" operator (operator at the last index).
    pub(crate) fn iter_operators(&self) -> impl Iterator<Item = &dyn PhysicalOperator> {
        let p0 = self
            .partitions
            .first()
            .expect("pipeline to have at least one partition");
        p0.operators.iter().map(|o| o.physical.as_ref())
    }
}

impl Explainable for Pipeline {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(format!("Pipeline {}", self.pipeline_id.0))
    }
}

/// A partition pipeline holds a sequence of operators along with the state for
/// a single partition.
///
/// This is the smallest unit of work as it relates to the scheduler.
#[derive(Debug)]
pub struct PartitionPipeline {
    /// Information about the pipeline.
    ///
    /// Should only be used for debugging/logging.
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

    /// Execution timings.
    timings: PartitionPipelineTimings,
}

impl PartitionPipeline {
    fn new(pipeline: PipelineId, partition: usize) -> Self {
        PartitionPipeline {
            info: PartitionPipelineInfo {
                pipeline,
                partition,
            },
            state: PipelinePartitionState::PullFrom { operator_idx: 0 },
            operators: Vec::new(),
            pull_start_idx: 0,
            timings: PartitionPipelineTimings::default(),
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
    pub(crate) fn state(&self) -> &PipelinePartitionState {
        &self.state
    }

    pub(crate) fn timings(&self) -> &PartitionPipelineTimings {
        &self.timings
    }

    /// Return an iterator over all the physcial operators in this partition
    /// pipeline.
    pub(crate) fn iter_operators(&self) -> impl Iterator<Item = &Arc<dyn PhysicalOperator>> {
        self.operators.iter().map(|op| &op.physical)
    }
}

/// Information about a partition pipeline.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PartitionPipelineInfo {
    pipeline: PipelineId,
    partition: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionPipelineTimings {
    /// Instant at which the pipeline started execution. Set on the first call
    /// the `poll_execute`.
    pub start: Option<Instant>,

    /// Instant at which the pipeline completed. Only set when the pipeline
    /// completes without error.
    pub completed: Option<Instant>,
}

#[derive(Debug)]
pub(crate) struct OperatorWithState {
    /// The underlying physical operator.
    physical: Arc<dyn PhysicalOperator>,

    /// The state that's shared across all partitions for this operator.
    operator_state: Arc<OperatorState>,

    /// The state for this operator that's exclusive to this partition.
    partition_state: PartitionState,
}

#[derive(Clone)]
pub(crate) enum PipelinePartitionState {
    /// Need to pull from an operator.
    PullFrom { operator_idx: usize },

    /// Need to push to an operator.
    PushTo { batch: Batch, operator_idx: usize },

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
            Self::Completed => f.debug_struct("Completed").finish(),
        }
    }
}

impl PartitionPipeline {
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
    pub fn poll_execute(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>> {
        trace!(
            pipeline_id = %self.info.pipeline.0,
            partition = %self.info.partition,
            "executing partition pipeline",
        );

        if self.timings.start.is_none() {
            self.timings.start = Some(Instant::now());
        }

        let state = &mut self.state;

        loop {
            match state {
                PipelinePartitionState::PullFrom { operator_idx } => {
                    let operator = self
                        .operators
                        .get_mut(*operator_idx)
                        .expect("operator to exist");
                    let poll_pull = operator.physical.poll_pull(
                        cx,
                        &mut operator.partition_state,
                        &operator.operator_state,
                    );
                    match poll_pull {
                        Ok(PollPull::Batch(batch)) => {
                            // We got a batch, increment operator index to push
                            // it into the next operator.
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
                            let next_operator = self
                                .operators
                                .get_mut(self.pull_start_idx)
                                .expect("next operator to exist");
                            let result = next_operator.physical.finalize_push(
                                &mut next_operator.partition_state,
                                &next_operator.operator_state,
                            );
                            if result.is_err() {
                                // Erroring on finalize is not recoverable.
                                *state = PipelinePartitionState::Completed;
                                return Poll::Ready(Some(result));
                            }

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
                        Err(e) => {
                            // We received an error. Currently no way to
                            // recover, so just mark this as completed and
                            // assume the error gets bubbled up.
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
                    let poll_push = operator.physical.poll_push(
                        cx,
                        &mut operator.partition_state,
                        &operator.operator_state,
                        batch,
                    );

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
                    self.timings.completed = Some(Instant::now());
                    return Poll::Ready(None);
                }
            }
        }
    }
}

pub mod operation;

use std::fmt::Debug;
use std::task::Context;

use operation::{PartitionSink, SinkOperation};
use parking_lot::Mutex;
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::scalar::BorrowedScalarValue;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct SinkPartitionState {
    /// Sink for this partition.
    sink: Box<dyn PartitionSink>,
    /// Number of rows pushed so far for this partition.
    current_row_count: usize,
    /// State of the partition.
    inner: SinkPartitionStateInner,
}

/// Determines what this partition is currently doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkPartitionStateInner {
    Pushing,
    Finalizing,
    Finished {
        /// Global row count for all partitions, should be returned on the last
        /// poll.
        ///
        /// This is Some for only a single partition.
        global_row_count: Option<usize>,
    },
}

#[derive(Debug)]
pub struct SinkOperatorState {
    inner: Mutex<SinkOperatorStateInner>,
}

#[derive(Debug)]
struct SinkOperatorStateInner {
    /// Row count from all partitions.
    ///
    /// Updated as partitions finalize.
    global_row_count: usize,
    /// Number of partitions still writing to the sink.
    ///
    /// Initialize to total number of partition for this operator.
    ///
    /// The last partition to complete should write out the global row count.
    partitions_remaining: usize,
}

/// An operator that writes batches to a partition sink.
///
/// Once all inputs have completed, and single partition will return an output
/// batch containing the total number of rows pushed to all partition sinks. All
/// other partitions will return a batch with zero rows.
///
/// Insert, CopyTo, CreateTable all use this. CreateTable uses this to enable
/// CTAS semantics easily.
#[derive(Debug)]
pub struct PhysicalSink<S: SinkOperation> {
    pub(crate) sink: S,
}

impl<S: SinkOperation> PhysicalSink<S> {
    pub fn new(sink: S) -> Self {
        PhysicalSink { sink }
    }
}

impl<S: SinkOperation> ExecutableOperator for PhysicalSink<S> {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let op_state = OperatorState::Sink(SinkOperatorState {
            inner: Mutex::new(SinkOperatorStateInner {
                global_row_count: 0,
                partitions_remaining: partitions,
            }),
        });

        let sinks = self.sink.create_partition_sinks(context, partitions)?;

        let part_states = sinks
            .into_iter()
            .map(|sink| {
                PartitionState::Sink(SinkPartitionState {
                    sink,
                    current_row_count: 0,
                    inner: SinkPartitionStateInner::Pushing,
                })
            })
            .collect();

        Ok(UnaryInputStates {
            operator_state: op_state,
            partition_states: part_states,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Sink(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.inner {
            SinkPartitionStateInner::Pushing => {
                let input = inout.input.required("input batch required")?;

                match state.sink.poll_push(cx, input)? {
                    operation::PollPush::Pushed => {
                        // Only updated after completing push to avoid double
                        // counting on pending.
                        state.current_row_count += input.num_rows();
                        Ok(PollExecute::NeedsMore)
                    }
                    operation::PollPush::Pending => Ok(PollExecute::Pending),
                }
            }
            SinkPartitionStateInner::Finished { global_row_count } => match global_row_count {
                Some(count) => {
                    let output = inout.output.required("output batch required")?;
                    output.reset_for_write()?;
                    output.arrays[0].set_value(0, &BorrowedScalarValue::Int64(count as i64))?;
                    output.set_num_rows(1)?;

                    Ok(PollExecute::Exhausted)
                }
                None => Err(RayexecError::new(
                    "Attempted to drain partition sink from incorrect partition",
                )),
            },
            SinkPartitionStateInner::Finalizing => Err(RayexecError::new(
                "Attempted to execute sink when currently finalizing",
            )),
        }
    }

    fn poll_finalize(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::Sink(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        match state.inner {
            SinkPartitionStateInner::Pushing => state.inner = SinkPartitionStateInner::Finalizing,
            SinkPartitionStateInner::Finalizing => {
                // Already polled previously, still need to complete finalize.
                ()
            }
            SinkPartitionStateInner::Finished { .. } => {
                return Err(RayexecError::new(
                    "Attempt to finalize already finished partition",
                ))
            }
        }

        match state.sink.poll_finalize(cx)? {
            PollFinalize::Finalized => {
                let mut op_state = match operator_state {
                    OperatorState::Sink(state) => state.inner.lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                op_state.global_row_count += state.current_row_count;
                op_state.partitions_remaining -= 1;

                if op_state.partitions_remaining == 0 {
                    // We're the last partition to complete, we'll return the
                    // global row count.
                    state.inner = SinkPartitionStateInner::Finished {
                        global_row_count: Some(op_state.global_row_count),
                    };
                    Ok(PollFinalize::NeedsDrain)
                } else {
                    // Other partitions still pushing, can't return anything now.
                    state.inner = SinkPartitionStateInner::Finished {
                        global_row_count: None,
                    };
                    Ok(PollFinalize::Finalized)
                }
            }
            PollFinalize::NeedsDrain => {
                Err(RayexecError::new("Sink unexpectedly return NeedsDrain"))
            }
            PollFinalize::Pending => Ok(PollFinalize::Pending),
        }
    }
}

impl<S: SinkOperation> Explainable for PhysicalSink<S> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        self.sink.explain_entry(conf)
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::database_context::test_database_context;
    use crate::testutil::operator::{CollectingSinkOperation, OperatorWrapper};

    #[test]
    fn sink_single_partition() {
        let mut wrapper = OperatorWrapper::new(PhysicalSink {
            sink: CollectingSinkOperation,
        });
        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let row_counts = [4, 5, 6];
        for row_count in row_counts {
            let poll = wrapper
                .poll_execute(
                    &mut states.partition_states[0],
                    &states.operator_state,
                    ExecuteInOutState {
                        input: Some(&mut Batch::empty_with_num_rows(row_count)),
                        output: None,
                    },
                )
                .unwrap();
            assert_eq!(PollExecute::NeedsMore, poll);
        }

        let poll = wrapper
            .poll_finalize(&mut states.partition_states[0], &states.operator_state)
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let mut output = Batch::new([DataType::Int64], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::from_arrays([Array::try_from_iter([15_i64]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn sink_multiple_partitions() {
        let mut wrapper = OperatorWrapper::new(PhysicalSink {
            sink: CollectingSinkOperation,
        });
        let mut states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 4)
            .unwrap();

        // (partition, row_count) pairs
        let per_part_row_counts = [(0, 24), (1, 14), (0, 100), (3, 8)];
        for (part, row_count) in per_part_row_counts {
            let poll = wrapper
                .poll_execute(
                    &mut states.partition_states[part],
                    &states.operator_state,
                    ExecuteInOutState {
                        input: Some(&mut Batch::empty_with_num_rows(row_count)),
                        output: None,
                    },
                )
                .unwrap();
            assert_eq!(PollExecute::NeedsMore, poll);
        }

        // Finalize all partitions but last.
        for part in 0..3 {
            let poll = wrapper
                .poll_finalize(&mut states.partition_states[part], &states.operator_state)
                .unwrap();
            assert_eq!(PollFinalize::Finalized, poll);
        }

        // Finalize last.
        let poll = wrapper
            .poll_finalize(&mut states.partition_states[3], &states.operator_state)
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let mut output = Batch::new([DataType::Int64], 1024).unwrap();

        // Poll with partition that finalized with NeedsDrain (3).
        let poll = wrapper
            .poll_execute(
                &mut states.partition_states[3],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = Batch::from_arrays([Array::try_from_iter([146_i64]).unwrap()]).unwrap();

        assert_batches_eq(&expected, &output);
    }
}

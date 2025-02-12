use std::task::{Context, Waker};

use parking_lot::Mutex;
use rayexec_error::{OptionExt, Result};

use super::{
    BinaryInputStates,
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

/// Partition state for the input that also produces output.
#[derive(Debug)]
pub struct UnionPullingPartitionState {
    /// If we're draining from the buffering side of the operator.
    draining: bool,
    /// Index of this partition, used to index into the global state.
    partition_idx: usize,
}

/// Partition state for the input that buffers its inputs into the global state.
///
/// Partitions with this state treat the union operator as a sink.
#[derive(Debug)]
pub struct UnionBufferingPartitionState {
    /// Index of this partition.
    partition_idx: usize,
}

#[derive(Debug)]
pub struct UnionOperatorState {
    buffers: Vec<Mutex<BufferedPartitionBatch>>,
}

#[derive(Debug)]
struct BufferedPartitionBatch {
    /// The batch buffer for moving data from one side to another.
    batch: Batch,
    /// If the batch is ready to copy out.
    is_ready_for_pull: bool,
    /// If the buffering side is finished and no more batches will be available.
    is_finished: bool,
    /// Waker for the buffering side if we can't yet overwrite the batch.
    buffering_waker: Option<Waker>,
    /// Waker for the pulling side.
    ///
    /// Woken when we push a batch from the buffering side.
    pulling_waker: Option<Waker>,
}

/// Unions two input operations.
#[derive(Debug)]
pub struct PhysicalUnion {
    pub(crate) output_types: Vec<DataType>,
}

impl PhysicalUnion {
    pub fn new(output_types: impl IntoIterator<Item = DataType>) -> Self {
        PhysicalUnion {
            output_types: output_types.into_iter().collect(),
        }
    }
}

impl ExecutableOperator for PhysicalUnion {
    type States = BinaryInputStates;

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<BinaryInputStates> {
        let buffering: Vec<_> = (0..partitions)
            .map(|idx| {
                PartitionState::UnionBuffering(UnionBufferingPartitionState { partition_idx: idx })
            })
            .collect();

        let pulling: Vec<_> = (0..partitions)
            .map(|idx| {
                PartitionState::UnionPulling(UnionPullingPartitionState {
                    partition_idx: idx,
                    draining: false,
                })
            })
            .collect();

        let op_state = OperatorState::Union(UnionOperatorState {
            buffers: (0..partitions)
                .map(|_| {
                    Ok(Mutex::new(BufferedPartitionBatch {
                        batch: Batch::try_new(self.output_types.clone(), batch_size)?,
                        is_ready_for_pull: false,
                        is_finished: false,
                        buffering_waker: None,
                        pulling_waker: None,
                    }))
                })
                .collect::<Result<Vec<_>>>()?,
        });

        Ok(BinaryInputStates {
            operator_state: op_state,
            sink_states: buffering,
            inout_states: pulling,
        })
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        match partition_state {
            PartitionState::UnionPulling(state) => {
                if !state.draining {
                    // Just need to pass through this batch.
                    let input = inout.input.required("input batch required")?;
                    let output = inout.output.required("output batch required")?;

                    output.try_clone_from_other(input)?;

                    return Ok(PollExecute::Ready);
                }

                // Otherwise need to check the operator state.
                let mut buf_state = match operator_state {
                    OperatorState::Union(op_state) => op_state.buffers[state.partition_idx].lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                let output = inout.output.required("output batch required")?;

                if buf_state.is_ready_for_pull {
                    // We have batch ready, copy it out and wake up buffering
                    // side for more batches.
                    output.try_clone_from_other(&mut buf_state.batch)?;
                    buf_state.is_ready_for_pull = false;

                    if let Some(waker) = buf_state.buffering_waker.take() {
                        waker.wake();
                    }

                    return Ok(PollExecute::HasMore);
                }

                if buf_state.is_finished {
                    // No more batches for us.
                    output.set_num_rows(0)?;

                    return Ok(PollExecute::Exhausted);
                }

                // Otherwise need to wait for more batches from the buffering
                // side.
                buf_state.pulling_waker = Some(cx.waker().clone());
                if let Some(waker) = buf_state.buffering_waker.take() {
                    waker.wake();
                }

                Ok(PollExecute::Pending)
            }
            PartitionState::UnionBuffering(state) => {
                let mut buf_state = match operator_state {
                    OperatorState::Union(op_state) => op_state.buffers[state.partition_idx].lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                if buf_state.is_ready_for_pull {
                    // Can't yet overwrite batch, come back later.
                    buf_state.buffering_waker = Some(cx.waker().clone());
                    if let Some(waker) = buf_state.pulling_waker.take() {
                        waker.wake();
                    }

                    return Ok(PollExecute::Pending);
                }

                // Otherwise copy our input batch in and let pulling side know.
                let input = inout.input.required("input batch required")?;
                buf_state.batch.try_clone_from_other(input)?;
                buf_state.is_ready_for_pull = true;

                if let Some(waker) = buf_state.pulling_waker.take() {
                    waker.wake();
                }

                Ok(PollExecute::NeedsMore)
            }
            other => panic!("invalid partition state: {other:?}"),
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        match partition_state {
            PartitionState::UnionPulling(state) => {
                assert!(!state.draining);
                state.draining = true;
                Ok(PollFinalize::NeedsDrain)
            }
            PartitionState::UnionBuffering(state) => {
                let mut buf_state = match operator_state {
                    OperatorState::Union(op_state) => op_state.buffers[state.partition_idx].lock(),
                    other => panic!("invalid operator state: {other:?}"),
                };

                buf_state.is_finished = true;

                Ok(PollFinalize::Finalized)
            }
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
        unimplemented!()
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
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
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn union_simple() {
        let mut operator = OperatorWrapper::new(PhysicalUnion {
            output_types: vec![DataType::Int32],
        });
        let mut states = operator
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();

        let mut top_input =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();

        let mut out = Batch::try_new([DataType::Int32], 1024).unwrap();

        let poll = operator
            .poll_execute(
                &mut states.inout_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut top_input),
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([1, 2, 3, 4]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &out);

        let poll = operator
            .poll_finalize(&mut states.inout_states[0], &states.operator_state)
            .unwrap();
        assert_eq!(PollFinalize::NeedsDrain, poll);

        let mut bottom_input =
            Batch::try_from_arrays([Array::try_from_iter([5, 6, 7, 8]).unwrap()]).unwrap();

        let poll = operator
            .poll_execute(
                &mut states.sink_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut bottom_input),
                    output: None,
                },
            )
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        // Pulling side should now be able to get the batch.
        let poll = operator
            .poll_execute(
                &mut states.inout_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::HasMore, poll);

        let expected =
            Batch::try_from_arrays([Array::try_from_iter([5, 6, 7, 8]).unwrap()]).unwrap();
        assert_batches_eq(&expected, &out);

        // Finalize buffering side.
        let poll = operator
            .poll_finalize(&mut states.sink_states[0], &states.operator_state)
            .unwrap();
        assert_eq!(PollFinalize::Finalized, poll);

        // Pulling side should exhaust now.
        let poll = operator
            .poll_execute(
                &mut states.inout_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: None,
                    output: Some(&mut out),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);
        assert_eq!(0, out.num_rows());
    }
}

use std::sync::Arc;
use std::task::{Context, Waker};

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    ExecutionStates,
    InputOutputStates,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
    PollPull,
    PollPush,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::proto::DatabaseProtoConv;

#[derive(Debug)]
pub struct LimitPartitionState {
    /// Remaining offset before we can actually start sending rows.
    remaining_offset: usize,
    /// Remaining number of rows before we stop sending batches.
    ///
    /// Initialized to the operator `limit`.
    remaining_count: usize,
    /// If inputs are finished.
    finished: bool,
}

/// Operator for LIMIT and OFFSET clauses.
///
/// The provided `limit` and `offset` values work on a per-partition basis. A
/// global limit/offset should be done by using a single partition.
#[derive(Debug)]
pub struct PhysicalLimit {
    /// Number of rows to limit to.
    pub(crate) limit: usize,
    /// Offset to start limiting from.
    pub(crate) offset: Option<usize>,
}

impl ExecutableOperator for PhysicalLimit {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let states = (0..partitions)
            .map(|_| {
                PartitionState::Limit(LimitPartitionState {
                    remaining_count: self.limit,
                    remaining_offset: self.offset.unwrap_or(0),
                    finished: false,
                })
            })
            .collect();

        Ok(PartitionAndOperatorStates::Branchless {
            operator_state: OperatorState::None,
            partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        if state.finished {
            return Ok(PollExecute::Exhausted);
        }

        let input = inout.input.required("input batch required")?;
        let output = inout.output.required("output batch required")?;

        if state.remaining_offset > 0 {
            // Offset greater than the number of rows in this batch. Discard the
            // batch, and keep asking for more input.
            if state.remaining_offset >= input.num_rows() {
                state.remaining_offset -= input.num_rows();
                return Ok(PollExecute::NeedsMore);
            }

            // Otherwise we have to slice the batch at the offset point.
            let count = std::cmp::min(
                input.num_rows() - state.remaining_offset,
                state.remaining_count,
            );

            let selection: Vec<_> =
                (state.remaining_offset..(state.remaining_offset + count)).collect();

            output.try_clone_from(input)?;
            output.select(&selection)?;

            state.remaining_offset = 0;
            state.remaining_count -= output.num_rows();

            if state.remaining_count == 0 {
                state.finished = true;
                Ok(PollExecute::Break)
            } else {
                Ok(PollExecute::Ready)
            }
        } else if state.remaining_count < input.num_rows() {
            // Remaining offset is 0, and input batch is has more rows than we
            // need, just slice to the right size.
            output.try_clone_from(input)?;
            output.set_num_rows(state.remaining_count)?;
            state.remaining_count = 0;
            state.finished = true;

            Ok(PollExecute::Break)
        } else {
            // Remaing offset is 0, and input batch has more rows than our
            // limit, so just use the batch as-is.
            output.try_clone_from(input)?;
            state.remaining_count -= output.num_rows();

            Ok(PollExecute::Ready)
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

        state.finished = true;

        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Limit").with_value("limit", self.limit);
        if let Some(offset) = self.offset {
            ent = ent.with_value("offset", offset);
        }
        ent
    }
}

impl DatabaseProtoConv for PhysicalLimit {
    type ProtoType = rayexec_proto::generated::execution::PhysicalLimit;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            limit: self.limit as u64,
            offset: self.offset.map(|o| o as u64),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            limit: proto.limit as usize,
            offset: proto.offset.map(|o| o as usize),
        })
    }
}

#[cfg(test)]
mod tests {
    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};

    fn single_partition_limit(
        limit: usize,
        offset: Option<usize>,
    ) -> (
        OperatorWrapper<PhysicalLimit>,
        OperatorState,
        PartitionState,
    ) {
        let wrapper = OperatorWrapper::new(PhysicalLimit { limit, offset });
        let states = wrapper
            .operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();
        let (op_state, mut part_states) = states.branchless_into_states().unwrap();
        let part_state = part_states.pop().unwrap();

        (wrapper, op_state, part_state)
    }

    #[test]
    fn limit_no_offset_simple() {
        let (wrapper, op_state, mut part_state) = single_partition_limit(5, None);

        let mut input = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e", "f"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5, 6]).unwrap(),
        ])
        .unwrap();
        let mut output = Batch::try_new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Break, poll);

        let expected = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_no_offset_multiple_batches() {
        let (wrapper, op_state, mut part_state) = single_partition_limit(8, None);

        let mut input1 = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5]).unwrap(),
        ])
        .unwrap();
        let mut output = Batch::try_new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input1),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected1 = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected1, &output);

        let mut input2 = Batch::try_from_arrays([
            Array::try_from_iter(["f", "g", "h", "i", "j"]).unwrap(),
            Array::try_from_iter([6, 7, 8, 9, 10]).unwrap(),
        ])
        .unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input2),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Break, poll);

        let expected2 = Batch::try_from_arrays([
            Array::try_from_iter(["f", "g", "h"]).unwrap(),
            Array::try_from_iter([6, 7, 8]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn limit_with_offset_single_batch() {
        let (wrapper, op_state, mut part_state) = single_partition_limit(2, Some(1));

        let mut input = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e", "f"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5, 6]).unwrap(),
        ])
        .unwrap();
        let mut output = Batch::try_new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Break, poll);

        let expected = Batch::try_from_arrays([
            Array::try_from_iter(["b", "c"]).unwrap(),
            Array::try_from_iter([2, 3]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_with_offset_discard_first_batch() {
        let (wrapper, op_state, mut part_state) = single_partition_limit(2, Some(6));

        let mut input = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b", "c", "d", "e"]).unwrap(),
            Array::try_from_iter([1, 2, 3, 4, 5]).unwrap(),
        ])
        .unwrap();
        let mut output = Batch::try_new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let mut input2 = Batch::try_from_arrays([
            Array::try_from_iter(["f", "g", "h", "i", "j"]).unwrap(),
            Array::try_from_iter([6, 7, 8, 9, 10]).unwrap(),
        ])
        .unwrap();

        let poll = wrapper
            .poll_execute(
                &mut part_state,
                &op_state,
                ExecuteInOutState {
                    input: Some(&mut input2),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(PollExecute::Break, poll);

        let expected = Batch::try_from_arrays([
            Array::try_from_iter(["g", "h"]).unwrap(),
            Array::try_from_iter([7, 8]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected, &output);
    }
}

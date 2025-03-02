use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOut,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::array::selection::Selection;
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

impl PhysicalLimit {
    pub fn new(limit: usize, offset: Option<usize>) -> Self {
        PhysicalLimit { limit, offset }
    }
}

impl ExecutableOperator for PhysicalLimit {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        _batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let states = (0..partitions)
            .map(|_| {
                PartitionState::Limit(LimitPartitionState {
                    remaining_count: self.limit,
                    remaining_offset: self.offset.unwrap_or(0),
                })
            })
            .collect();

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states: states,
        })
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOut,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Limit(state) => state,
            other => panic!("invalid partition state: {other:?}"),
        };

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

            output.clone_from_other(input)?;
            output.select(Selection::linear(state.remaining_offset, count))?;

            state.remaining_offset = 0;
            state.remaining_count -= output.num_rows();

            if state.remaining_count == 0 {
                Ok(PollExecute::Exhausted)
            } else {
                Ok(PollExecute::Ready)
            }
        } else if state.remaining_count < input.num_rows() {
            // Remaining offset is 0, and input batch is has more rows than we
            // need, just slice to the right size.
            output.clone_from_other(input)?;
            output.set_num_rows(state.remaining_count)?;
            state.remaining_count = 0;

            Ok(PollExecute::Exhausted)
        } else {
            // Remaing offset is 0, and input batch has more rows than our
            // limit, so just use the batch as-is.
            output.clone_from_other(input)?;
            state.remaining_count -= output.num_rows();

            Ok(PollExecute::Ready)
        }
    }

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
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
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};
    use crate::testutil::operator::OperatorWrapper2;

    #[test]
    fn limit_no_offset_simple() {
        let mut wrapper = OperatorWrapper2::new(PhysicalLimit::new(5, None));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut input = generate_batch!(["a", "b", "c", "d", "e", "f"], [1, 2, 3, 4, 5, 6]);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_no_offset_multiple_batches() {
        let mut wrapper = OperatorWrapper2::new(PhysicalLimit::new(8, None));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut input = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::Ready, poll);

        let expected1 = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5]);
        assert_batches_eq(&expected1, &output);

        let mut input2 = generate_batch!(["f", "g", "h", "i", "j"], [6, 7, 8, 9, 10]);
        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input2, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);

        let expected2 = generate_batch!(["f", "g", "h"], [6, 7, 8],);
        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn limit_with_offset_single_batch() {
        let mut wrapper = OperatorWrapper2::new(PhysicalLimit::new(2, Some(1)));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut input = generate_batch!(["a", "b", "c", "d", "e", "f"], [1, 2, 3, 4, 5, 6],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["b", "c"], [2, 3],);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_with_offset_discard_first_batch() {
        let mut wrapper = OperatorWrapper2::new(PhysicalLimit::new(2, Some(6)));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut input = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(PollExecute::NeedsMore, poll);

        let mut input2 = generate_batch!(["f", "g", "h", "i", "j"], [6, 7, 8, 9, 10],);

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input2, &mut output);
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["g", "h"], [7, 8],);
        assert_batches_eq(&expected, &output);
    }
}

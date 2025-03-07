use std::task::Context;

use parking_lot::Mutex;
use rayexec_error::Result;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug)]
pub struct LimitOperatorState {
    inner: Mutex<StateInner>,
}

#[derive(Debug)]
struct StateInner {
    /// Remaining offset before we can actually start sending rows.
    remaining_offset: usize,
    /// Remaining number of rows before we stop sending batches.
    ///
    /// Initialized to the operator `limit`.
    remaining_count: usize,
}

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
    pub(crate) datatypes: Vec<DataType>,
}

impl PhysicalLimit {
    pub fn new(
        limit: usize,
        offset: Option<usize>,
        datatypes: impl IntoIterator<Item = DataType>,
    ) -> Self {
        PhysicalLimit {
            limit,
            offset,
            datatypes: datatypes.into_iter().collect(),
        }
    }
}

impl BaseOperator for PhysicalLimit {
    type OperatorState = LimitOperatorState;

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(LimitOperatorState {
            inner: Mutex::new(StateInner {
                remaining_count: self.limit,
                remaining_offset: self.offset.unwrap_or(0),
            }),
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl ExecuteOperator for PhysicalLimit {
    type PartitionExecuteState = ();

    fn create_partition_execute_states(
        &self,
        operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        Ok(vec![(); partitions])
    }

    fn poll_execute(
        &self,
        cx: &mut Context,
        operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        let mut state = operator_state.inner.lock();

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

            let offset = state.remaining_offset;

            state.remaining_offset = 0;
            state.remaining_count -= count;

            let exhausted = state.remaining_count == 0;

            std::mem::drop(state);

            output.clone_from_other(input)?;
            output.select(Selection::linear(offset, count))?;

            if exhausted {
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

    fn poll_finalize_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        _state: &mut Self::PartitionExecuteState,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn limit_no_offset_simple() {
        let wrapper = OperatorWrapper::new(PhysicalLimit::new(
            5,
            None,
            [DataType::Utf8, DataType::Int32],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d", "e", "f"], [1, 2, 3, 4, 5, 6]);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5]);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_no_offset_multiple_batches() {
        let wrapper = OperatorWrapper::new(PhysicalLimit::new(
            8,
            None,
            [DataType::Utf8, DataType::Int32],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected1 = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5]);
        assert_batches_eq(&expected1, &output);

        let mut input2 = generate_batch!(["f", "g", "h", "i", "j"], [6, 7, 8, 9, 10]);
        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input2, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected2 = generate_batch!(["f", "g", "h"], [6, 7, 8],);
        assert_batches_eq(&expected2, &output);
    }

    #[test]
    fn limit_with_offset_single_batch() {
        let wrapper = OperatorWrapper::new(PhysicalLimit::new(
            2,
            Some(1),
            [DataType::Utf8, DataType::Int32],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d", "e", "f"], [1, 2, 3, 4, 5, 6],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["b", "c"], [2, 3],);
        assert_batches_eq(&expected, &output);
    }

    #[test]
    fn limit_with_offset_discard_first_batch() {
        let wrapper = OperatorWrapper::new(PhysicalLimit::new(
            2,
            Some(6),
            [DataType::Utf8, DataType::Int32],
        ));
        let props = ExecutionProperties { batch_size: 16 };
        let op_state = wrapper.operator.create_operator_state(props).unwrap();

        let mut input = generate_batch!(["a", "b", "c", "d", "e"], [1, 2, 3, 4, 5],);
        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();

        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input, &mut output)
            .unwrap();
        assert_eq!(PollExecute::NeedsMore, poll);

        let mut input2 = generate_batch!(["f", "g", "h", "i", "j"], [6, 7, 8, 9, 10],);

        let poll = wrapper
            .poll_execute(&op_state, &mut (), &mut input2, &mut output)
            .unwrap();
        assert_eq!(PollExecute::Exhausted, poll);

        let expected = generate_batch!(["g", "h"], [7, 8],);
        assert_batches_eq(&expected, &output);
    }
}

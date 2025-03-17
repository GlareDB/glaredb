use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::selection_evaluator::SelectionEvaluator;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) datatypes: Vec<DataType>,
    pub(crate) predicate: PhysicalScalarExpression,
}

#[derive(Debug)]
pub struct FilterPartitionState {
    evaluator: SelectionEvaluator,
}

impl BaseOperator for PhysicalFilter {
    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &self.datatypes
    }
}

impl ExecuteOperator for PhysicalFilter {
    type PartitionExecuteState = FilterPartitionState;

    fn create_partition_execute_states(
        &self,
        _operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        (0..partitions)
            .map(|_| {
                Ok(FilterPartitionState {
                    evaluator: SelectionEvaluator::try_new(
                        self.predicate.clone(),
                        props.batch_size,
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        _operator_state: &Self::OperatorState,
        state: &mut Self::PartitionExecuteState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        // TODO: "select_from"
        let selection = state.evaluator.select(input)?;
        output.clone_from_other(input)?;

        if selection.len() != output.num_rows() {
            // Only add selection if we're actually omitting rows.
            output.select(Selection::slice(selection))?;
        }

        Ok(PollExecute::Ready)
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

impl Explainable for PhysicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", &self.predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::generate_batch;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn filter_simple() {
        let wrapper = OperatorWrapper::new(PhysicalFilter {
            datatypes: vec![DataType::Boolean, DataType::Int32],
            predicate: PhysicalScalarExpression::Column(PhysicalColumnExpr {
                datatype: DataType::Boolean,
                idx: 0,
            }),
        });

        let props = ExecutionProperties { batch_size: 16 };
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&(), props, 1)
            .unwrap();

        let mut out = Batch::new([DataType::Boolean, DataType::Int32], 4).unwrap();
        let mut in1 = generate_batch!([true, false, true, true], [8, 9, 7, 6]);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut in1, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected = generate_batch!([true, true, true], [8, 7, 6]);
        assert_batches_eq(&expected, &out);

        let mut in2 = generate_batch!([true, false, false, false], [4, 3, 2, 1]);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut in2, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected = generate_batch!([true], [4]);
        assert_batches_eq(&expected, &out);
    }
}

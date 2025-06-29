use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::evaluator::ExpressionEvaluator;

#[derive(Debug)]
pub struct PhysicalProject {
    pub(crate) projections: Vec<PhysicalScalarExpression>,
    pub(crate) output_types: Vec<DataType>,
}

impl PhysicalProject {
    pub fn new<S>(projections: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<PhysicalScalarExpression>,
    {
        let projections: Vec<_> = projections.into_iter().map(|expr| expr.into()).collect();
        let output_types = projections.iter().map(|proj| proj.datatype()).collect();

        PhysicalProject {
            projections,
            output_types,
        }
    }
}

#[derive(Debug)]
pub struct ProjectPartitionState {
    evaluator: ExpressionEvaluator,
}

impl BaseOperator for PhysicalProject {
    const OPERATOR_NAME: &str = "Project";

    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalProject {
    type PartitionExecuteState = ProjectPartitionState;

    fn create_partition_execute_states(
        &self,
        _operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        (0..partitions)
            .map(|_| {
                Ok(ProjectPartitionState {
                    evaluator: ExpressionEvaluator::try_new(
                        self.projections.clone(),
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
        let sel = input.selection();
        state.evaluator.eval_batch(input, sel, output)?;

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

impl Explainable for PhysicalProject {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new(Self::OPERATOR_NAME, conf)
            .with_values("projections", &self.projections)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::exprs::plan_scalars;
    use crate::testutil::operator::OperatorWrapper;
    use crate::{expr, generate_batch};

    #[test]
    fn project_simple() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, [DataType::boolean(), DataType::int32()], ["c1", "c2"])
            .unwrap();

        let projections = plan_scalars(
            &list,
            [
                &expr::column((t0, 1), DataType::int32()),
                &expr::lit("lit").into(),
            ],
        );

        let props = ExecutionProperties { batch_size: 16 };
        let wrapper = OperatorWrapper::new(PhysicalProject::new(projections));
        wrapper.operator.create_operator_state(props).unwrap();
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&(), props, 1)
            .unwrap();

        let mut out = Batch::new([DataType::int32(), DataType::utf8()], 4).unwrap();
        let mut in1 = generate_batch!([true, false, true, true], [8, 9, 7, 6]);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut in1, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected = generate_batch!([8, 9, 7, 6], ["lit", "lit", "lit", "lit"]);
        assert_batches_eq(&expected, &out);

        let mut in2 = generate_batch!([true, false, true, true], [Some(4), Some(5), None, Some(7)]);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut in2, &mut out)
            .unwrap();
        assert_eq!(PollExecute::Ready, poll);

        let expected = generate_batch!(
            [Some(4), Some(5), None, Some(7)],
            ["lit", "lit", "lit", "lit"]
        );
        assert_batches_eq(&expected, &out);
    }
}

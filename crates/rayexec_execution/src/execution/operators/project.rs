use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionState,
    PollExecute,
    PollFinalize,
    UnaryInputStates,
};
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;

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

impl ExecutableOperator for PhysicalProject {
    type States = UnaryInputStates;

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let partition_states = (0..partitions)
            .map(|_| {
                Ok(PartitionState::Project(ProjectPartitionState {
                    evaluator: ExpressionEvaluator::try_new(self.projections.clone(), batch_size)?,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(UnaryInputStates {
            operator_state: OperatorState::None,
            partition_states,
        })
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }

    fn poll_execute(
        &self,
        _cx: &mut Context,
        partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
        inout: ExecuteInOutState,
    ) -> Result<PollExecute> {
        let state = match partition_state {
            PartitionState::Project(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        let input = inout.input.required("batch input")?;
        let output = inout.output.required("batch output")?;

        let sel = input.selection();
        state.evaluator.eval_batch(input, sel, output)?;

        Ok(PollExecute::Ready)
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

impl Explainable for PhysicalProject {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Project").with_values("projections", &self.projections)
    }
}

#[cfg(test)]
mod tests {

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
    use crate::testutil::arrays::assert_batches_eq;
    use crate::testutil::database_context::test_database_context;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn project_simple() {
        let projections = vec![
            PhysicalScalarExpression::Column(PhysicalColumnExpr {
                datatype: DataType::Int32,
                idx: 1,
            }),
            PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                literal: "lit".into(),
            }),
        ];

        let mut operator = PhysicalProject::new(projections);
        let mut states = operator
            .create_states(&test_database_context(), 4, 1)
            .unwrap();

        let wrapper = OperatorWrapper::new(operator);

        let mut out = Batch::new([DataType::Int32, DataType::Utf8], 4).unwrap();

        let mut in1 = Batch::from_arrays([
            Array::try_from_iter([true, false, true, true]).unwrap(),
            Array::try_from_iter([8, 9, 7, 6]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut in1),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected1 = Batch::from_arrays([
            Array::try_from_iter([8, 9, 7, 6]).unwrap(),
            Array::try_from_iter(["lit", "lit", "lit", "lit"]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected1, &out);

        let mut in2 = Batch::from_arrays([
            Array::try_from_iter([true, false, true, true]).unwrap(),
            Array::try_from_iter([Some(4), Some(5), None, Some(7)]).unwrap(),
        ])
        .unwrap();

        wrapper
            .poll_execute(
                &mut states.partition_states[0],
                &states.operator_state,
                ExecuteInOutState {
                    input: Some(&mut in2),
                    output: Some(&mut out),
                },
            )
            .unwrap();

        let expected2 = Batch::from_arrays([
            Array::try_from_iter([Some(4), Some(5), None, Some(7)]).unwrap(),
            Array::try_from_iter(["lit", "lit", "lit", "lit"]).unwrap(),
        ])
        .unwrap();
        assert_batches_eq(&expected2, &out);
    }
}

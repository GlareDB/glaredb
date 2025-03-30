use std::task::Context;

use glaredb_error::Result;

use super::{BaseOperator, ExecuteOperator, ExecutionProperties, PollExecute, PollFinalize};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::evaluator::ExpressionEvaluator;

#[derive(Debug)]
pub struct ValuesPartitionState {
    /// Index for the "row" of expressions we're currently working on.
    row_idx: usize,
    /// Output buffer for evaluating expressions.
    out_buf: Batch,
}

#[derive(Debug)]
pub struct PhysicalValues {
    pub(crate) expressions: Vec<Vec<PhysicalScalarExpression>>,
    pub(crate) output_types: Vec<DataType>,
}

impl PhysicalValues {
    pub fn new(expressions: Vec<Vec<PhysicalScalarExpression>>) -> Self {
        let datatypes = match expressions.first() {
            Some(first) => first.iter().map(|expr| expr.datatype()).collect(),
            None => {
                return PhysicalValues {
                    expressions: Vec::new(),
                    output_types: Vec::new(),
                };
            }
        };

        PhysicalValues {
            expressions,
            output_types: datatypes,
        }
    }
}

impl BaseOperator for PhysicalValues {
    const OPERATOR_NAME: &str = "Values";

    type OperatorState = ();

    fn create_operator_state(&self, _props: ExecutionProperties) -> Result<Self::OperatorState> {
        Ok(())
    }

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }
}

impl ExecuteOperator for PhysicalValues {
    type PartitionExecuteState = ValuesPartitionState;

    fn create_partition_execute_states(
        &self,
        _operator_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionExecuteState>> {
        (0..partitions)
            .map(|_| {
                let out_buf = Batch::new(self.output_types.clone(), props.batch_size)?;
                Ok(ValuesPartitionState {
                    row_idx: 0,
                    out_buf,
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
        output.set_num_rows(0)?;

        if input.num_rows() == 0 {
            return Ok(PollExecute::Exhausted);
        }

        let capacity = output.write_capacity()?;
        loop {
            let out_rem = capacity - output.num_rows();
            if out_rem < input.num_rows() {
                // Not enough room in the output batch, come back later for the
                // remaining expressions.
                return Ok(PollExecute::HasMore);
            }

            // TODO: Don't create this here, store it on the partition state.
            let mut evaluator = ExpressionEvaluator::try_new(
                self.expressions[state.row_idx].clone(),
                input.num_rows(),
            )?;

            state.out_buf.reset_for_write()?;

            evaluator.eval_batch(input, input.selection(), &mut state.out_buf)?;
            output.append(&state.out_buf)?;

            // Move to next set of expressions.
            state.row_idx += 1;

            if state.row_idx >= self.expressions.len() {
                // Executed all expressions for this batch, need new input
                state.row_idx = 0;
                return Ok(PollExecute::Ready);
            }
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

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new(Self::OPERATOR_NAME)
            .with_value("num_rows", self.expressions.len())
            .with_values("datatypes", &self.output_types)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::scalar::ScalarValue;
    use crate::expr;
    use crate::logical::binder::table_list::TableList;
    use crate::testutil::arrays::{assert_batches_eq, generate_batch};
    use crate::testutil::exprs::plan_scalar;
    use crate::testutil::operator::OperatorWrapper;

    #[test]
    fn values_literal() {
        // `VALUES ('a', 2), ('b', 3)`
        let list = TableList::empty();
        let expr_rows = vec![
            vec![
                plan_scalar(&list, expr::lit("a")),
                plan_scalar(&list, expr::lit(2)),
            ],
            vec![
                plan_scalar(&list, expr::lit("b")),
                plan_scalar(&list, expr::lit(3)),
            ],
        ];

        let wrapper = OperatorWrapper::new(PhysicalValues::new(expr_rows));
        let props = ExecutionProperties { batch_size: 16 };
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&(), props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();
        let mut input = Batch::empty_with_num_rows(1);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(poll, PollExecute::Ready);

        let expected1 = generate_batch!(["a", "b"], [2, 3]);
        assert_batches_eq(&expected1, &output);
    }

    #[test]
    fn values_literal_with_null() {
        // `VALUES (4), (NULL)`
        let list = TableList::empty();
        let expr_rows = vec![
            vec![plan_scalar(&list, expr::lit(4))],
            vec![plan_scalar(&list, expr::lit(ScalarValue::Null))],
        ];

        let wrapper = OperatorWrapper::new(PhysicalValues::new(expr_rows));
        let props = ExecutionProperties { batch_size: 16 };
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&(), props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Int32], 1024).unwrap();
        let mut input = Batch::empty_with_num_rows(1);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(poll, PollExecute::Ready);

        let expected1 = generate_batch!([Some(4), None]);
        assert_batches_eq(&expected1, &output);

        let mut input = Batch::empty_with_num_rows(0);
        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(poll, PollExecute::Exhausted);
        assert_eq!(0, output.num_rows);
    }

    #[test]
    fn values_literal_multiple_polls() {
        // Test that we properly handle multiple polls if all expressions don't
        // fit into a single output batch. While we use constant expressions
        // here, this covers the case where we have correlated or input column
        // to the VALUES clause.
        //
        // This sets the output capacity to 1024, and the input num rows to 513.
        // Each poll can only handle a single row of expressions.

        // `VALUES ('a', 2), ('b', 3)`
        let list = TableList::empty();
        let expr_rows = vec![
            vec![
                plan_scalar(&list, expr::lit("a")),
                plan_scalar(&list, expr::lit(2)),
            ],
            vec![
                plan_scalar(&list, expr::lit("b")),
                plan_scalar(&list, expr::lit(3)),
            ],
        ];

        let wrapper = OperatorWrapper::new(PhysicalValues::new(expr_rows));
        let props = ExecutionProperties { batch_size: 1024 };
        let mut states = wrapper
            .operator
            .create_partition_execute_states(&(), props, 1)
            .unwrap();

        let mut output = Batch::new([DataType::Utf8, DataType::Int32], 1024).unwrap();
        let mut input = Batch::empty_with_num_rows(513);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(poll, PollExecute::HasMore);

        let expected1 = generate_batch!(
            std::iter::repeat("a").take(513),
            std::iter::repeat(2).take(513)
        );
        assert_batches_eq(&expected1, &output);

        let poll = wrapper
            .poll_execute(&(), &mut states[0], &mut input, &mut output)
            .unwrap();
        assert_eq!(poll, PollExecute::Ready);

        let expected2 = generate_batch!(
            std::iter::repeat("b").take(513),
            std::iter::repeat(3).take(513)
        );
        assert_batches_eq(&expected2, &output);
    }
}

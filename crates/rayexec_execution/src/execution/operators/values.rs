use std::task::Context;

use rayexec_error::{OptionExt, Result};

use super::{
    ExecutableOperator,
    ExecuteInOutState,
    OperatorState,
    PartitionAndOperatorStates,
    PartitionState,
    PollExecute,
    PollFinalize,
};
use crate::arrays::batch::Batch;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;

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
}

impl ExecutableOperator for PhysicalValues {
    fn create_states(
        &self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<PartitionAndOperatorStates> {
        let states = (0..partitions)
            .map(|_| {
                let out_buf = match self.expressions.first() {
                    Some(first) => {
                        let datatypes = first.iter().map(|expr| expr.datatype());
                        Batch::try_new(datatypes, batch_size)?
                    }
                    None => Batch::empty(),
                };

                Ok(PartitionState::Values(ValuesPartitionState {
                    row_idx: 0,
                    out_buf,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

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
            PartitionState::Values(state) => state,
            other => panic!("invalid state: {other:?}"),
        };

        let input = inout.input.required("input batch required")?;
        let output = inout.output.required("input batch required")?;

        output.set_num_rows(0)?;

        loop {
            let out_rem = output.capacity - output.num_rows();
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

    fn poll_finalize(
        &self,
        _cx: &mut Context,
        _partition_state: &mut PartitionState,
        _operator_state: &OperatorState,
    ) -> Result<PollFinalize> {
        Ok(PollFinalize::Finalized)
    }
}

impl Explainable for PhysicalValues {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Values")
    }
}

impl DatabaseProtoConv for PhysicalValues {
    type ProtoType = rayexec_proto::generated::execution::PhysicalValues;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // use rayexec_proto::generated::array::IpcStreamBatch;

        // // TODO: Should empty values even be allowed? Is it allowed?
        // let schema = match self.batches.first() {
        //     Some(batch) => Schema::new(
        //         batch
        //             .columns()
        //             .iter()
        //             .map(|c| Field::new("", c.datatype().clone(), true)),
        //     ),
        //     None => {
        //         return Ok(Self::ProtoType {
        //             batches: Some(IpcStreamBatch { ipc: Vec::new() }),
        //         })
        //     }
        // };

        // let buf = Vec::new();
        // let mut writer = StreamWriter::try_new(buf, &schema, IpcConfig {})?;

        // for batch in &self.batches {
        //     writer.write_batch(batch)?
        // }

        // let buf = writer.into_writer();

        // Ok(Self::ProtoType {
        //     batches: Some(IpcStreamBatch { ipc: buf }),
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // let ipc = proto.batches.required("batches")?.ipc;

        // let mut reader = StreamReader::try_new(Cursor::new(ipc), IpcConfig {})?;

        // let mut batches = Vec::new();
        // while let Some(batch) = reader.try_next_batch()? {
        //     batches.push(batch);
        // }

        // Ok(Self { batches })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_batches_eq;
    use crate::execution::operators::testutil::{test_database_context, OperatorWrapper};
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;

    #[test]
    fn values_literal() {
        // `VALUES ('a', 2), ('b', 3)`
        let exprs = vec![
            vec![
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: "a".into(),
                }),
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr { literal: 2.into() }),
            ],
            vec![
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: "b".into(),
                }),
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr { literal: 3.into() }),
            ],
        ];

        let operator = PhysicalValues { expressions: exprs };
        let states = operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();
        let (operator_state, mut partition_states) = states.branchless_into_states().unwrap();

        let wrapper = OperatorWrapper::new(operator);

        let mut output = Batch::try_from_arrays([
            Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 1024).unwrap(),
            Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 1024).unwrap(),
        ])
        .unwrap();

        let mut input = Batch::empty_with_num_rows(1);

        let poll = wrapper
            .poll_execute(
                &mut partition_states[0],
                &operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(poll, PollExecute::Ready);

        let expected1 = Batch::try_from_arrays([
            Array::try_from_iter(["a", "b"]).unwrap(),
            Array::try_from_iter([2, 3]).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected1, &output);
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
        let exprs = vec![
            vec![
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: "a".into(),
                }),
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr { literal: 2.into() }),
            ],
            vec![
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: "b".into(),
                }),
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr { literal: 3.into() }),
            ],
        ];

        let operator = PhysicalValues { expressions: exprs };
        let states = operator
            .create_states(&test_database_context(), 1024, 1)
            .unwrap();
        let (operator_state, mut partition_states) = states.branchless_into_states().unwrap();

        let wrapper = OperatorWrapper::new(operator);

        let mut output = Batch::try_from_arrays([
            Array::try_new(&Arc::new(NopBufferManager), DataType::Utf8, 1024).unwrap(),
            Array::try_new(&Arc::new(NopBufferManager), DataType::Int32, 1024).unwrap(),
        ])
        .unwrap();

        let mut input = Batch::empty_with_num_rows(513);

        let poll = wrapper
            .poll_execute(
                &mut partition_states[0],
                &operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(poll, PollExecute::HasMore);

        let expected1 = Batch::try_from_arrays([
            Array::try_from_iter(std::iter::repeat("a").take(513)).unwrap(),
            Array::try_from_iter(std::iter::repeat(2).take(513)).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected1, &output);

        let poll = wrapper
            .poll_execute(
                &mut partition_states[0],
                &operator_state,
                ExecuteInOutState {
                    input: Some(&mut input),
                    output: Some(&mut output),
                },
            )
            .unwrap();
        assert_eq!(poll, PollExecute::Ready);

        let expected1 = Batch::try_from_arrays([
            Array::try_from_iter(std::iter::repeat("b").take(513)).unwrap(),
            Array::try_from_iter(std::iter::repeat(3).take(513)).unwrap(),
        ])
        .unwrap();

        assert_batches_eq(&expected1, &output);
    }
}

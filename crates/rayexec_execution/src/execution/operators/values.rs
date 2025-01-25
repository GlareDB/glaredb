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
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
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
                }
            }
        };

        PhysicalValues {
            expressions,
            output_types: datatypes,
        }
    }
}

impl ExecutableOperator for PhysicalValues {
    type States = UnaryInputStates;

    fn output_types(&self) -> &[DataType] {
        &self.output_types
    }

    fn create_states(
        &mut self,
        _context: &DatabaseContext,
        batch_size: usize,
        partitions: usize,
    ) -> Result<UnaryInputStates> {
        let states = (0..partitions)
            .map(|_| {
                let out_buf = Batch::try_new(self.output_types.clone(), batch_size)?;
                Ok(PartitionState::Values(ValuesPartitionState {
                    row_idx: 0,
                    out_buf,
                }))
            })
            .collect::<Result<Vec<_>>>()?;

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

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::Array;
    use crate::arrays::batch::Batch;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::{assert_batches_eq, generate_batch};
    use crate::execution::operators::testutil::{plan_scalars, OperatorWrapper};
    use crate::expr;

    #[test]
    fn values_literal() {
        // `VALUES ('a', 2), ('b', 3)`
        let expr_rows = vec![
            plan_scalars(&[expr::lit("a"), expr::lit(2)], &[]),
            plan_scalars(&[expr::lit("b"), expr::lit(3)], &[]),
        ];

        let mut wrapper = OperatorWrapper::new(PhysicalValues::new(expr_rows));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut output = Batch::try_from_arrays([
            Array::try_new(&NopBufferManager, DataType::Utf8, 1024).unwrap(),
            Array::try_new(&NopBufferManager, DataType::Int32, 1024).unwrap(),
        ])
        .unwrap();

        let mut input = Batch::empty_with_num_rows(1);

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(poll, PollExecute::Ready);

        let expected1 = generate_batch!(["a", "b"], [2, 3]);
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
        let expr_rows = vec![
            plan_scalars(&[expr::lit("a"), expr::lit(2)], &[]),
            plan_scalars(&[expr::lit("b"), expr::lit(3)], &[]),
        ];

        let mut wrapper = OperatorWrapper::new(PhysicalValues::new(expr_rows));
        let mut states = wrapper.create_unary_states(1024, 1);

        let mut output = Batch::try_from_arrays([
            Array::try_new(&NopBufferManager, DataType::Utf8, 1024).unwrap(),
            Array::try_new(&NopBufferManager, DataType::Int32, 1024).unwrap(),
        ])
        .unwrap();

        let mut input = Batch::empty_with_num_rows(513);

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(poll, PollExecute::HasMore);

        let expected1 = generate_batch!(
            std::iter::repeat("a").take(513),
            std::iter::repeat(2).take(513)
        );
        assert_batches_eq(&expected1, &output);

        let poll = wrapper.unary_execute_inout(&mut states, 0, &mut input, &mut output);
        assert_eq!(poll, PollExecute::Ready);

        let expected2 = generate_batch!(
            std::iter::repeat("b").take(513),
            std::iter::repeat(3).take(513)
        );
        assert_batches_eq(&expected2, &output);
    }
}

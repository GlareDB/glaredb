use std::borrow::Cow;
use std::fmt;

use fmtutil::IntoDisplayableSlice;
use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::batch::BatchOld;
use rayexec_error::Result;

use super::evaluator::{ExpressionEvaluator, ExpressionState};
use super::PhysicalScalarExpression;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::executor::OutBuffer;
use crate::arrays::flat_array::FlatSelection;
use crate::database::DatabaseContext;
use crate::functions::scalar::PlannedScalarFunction;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalScalarFunctionExpr {
    pub function: PlannedScalarFunction,
    pub inputs: Vec<PhysicalScalarExpression>,
}

impl PhysicalScalarFunctionExpr {
    pub fn eval2<'a>(&self, batch: &'a BatchOld) -> Result<Cow<'a, ArrayOld>> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.eval2(batch))
            .collect::<Result<Vec<_>>>()?;

        let refs: Vec<_> = inputs.iter().map(|a| a.as_ref()).collect(); // Can I not?
        let mut out = self.function.function_impl.execute_old(&refs)?;

        // If function is provided no input, it's expected to return an
        // array of length 1. We extend the array here so that it's the
        // same size as the rest.
        //
        // TODO: Could just extend the selection vector too.
        if refs.is_empty() {
            let scalar = out.logical_value(0)?;
            out = scalar.as_array(batch.num_rows())?;
        }

        Ok(Cow::Owned(out))
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: FlatSelection,
        output: &mut Array,
    ) -> Result<()> {
        // Eval inputs, writing them to our buffer state.
        for (idx, arg_input) in self.inputs.iter().enumerate() {
            let arg_state = &mut state.inputs[idx];
            let output = state.buffer.get_array_mut(idx)?;

            ExpressionEvaluator::eval_expression(arg_input, input, arg_state, sel, output)?;
        }

        state.buffer.set_num_rows(sel.len())?;

        // Eval function with now filled batch.
        let out = OutBuffer {
            buffer: output.data.try_as_mut()?,
            validity: &mut output.validity,
        };

        self.function.function_impl.execute(&state.buffer, out)?;

        Ok(())
    }
}

impl fmt::Display for PhysicalScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.function.function.name(),
            self.inputs.display_as_list()
        )
    }
}

impl DatabaseProtoConv for PhysicalScalarFunctionExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalScalarFunctionExpr;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     function: Some(self.function.to_proto_ctx(context)?),
        //     inputs: self
        //         .inputs
        //         .iter()
        //         .map(|input| input.to_proto_ctx(context))
        //         .collect::<Result<Vec<_>>>()?,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     function: DatabaseProtoConv::from_proto_ctx(
        //         proto.function.required("function")?,
        //         context,
        //     )?,
        //     inputs: proto
        //         .inputs
        //         .into_iter()
        //         .map(|input| DatabaseProtoConv::from_proto_ctx(input, context))
        //         .collect::<Result<Vec<_>>>()?,
        // })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::datatype::DataTypeOld;

    use super::*;
    use crate::arrays::buffer::physical_type::PhysicalI32;
    use crate::arrays::buffer::Int32BufferBuilder;
    use crate::arrays::buffer_manager::NopBufferManager;
    use crate::arrays::datatype::DataType;
    use crate::expr::column_expr::ColumnExpr;
    use crate::expr::physical::column_expr::PhysicalColumnExpr;
    use crate::expr::Expression;
    use crate::functions::scalar::builtin::temp_add_2::TempAdd2;
    use crate::functions::scalar::ScalarFunction;
    use crate::logical::binder::table_list::TableList;

    fn make_temp_add_2() -> PlannedScalarFunction {
        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(None, vec![DataTypeOld::Int32], vec!["col".to_string()])
            .unwrap();

        TempAdd2
            .plan(
                &table_list,
                vec![Expression::Column(ColumnExpr {
                    table_scope: table_ref,
                    column: 0,
                })],
            )
            .unwrap()
    }

    #[test]
    fn eval_simple() {
        let mut batch = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32BufferBuilder::from_iter([4, 5, 6]).unwrap(),
            )],
            true,
        )
        .unwrap();

        let expr = PhysicalScalarFunctionExpr {
            function: make_temp_add_2(),
            inputs: vec![PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 0 })],
        };

        let mut state = ExpressionState {
            buffer: Batch::new(&NopBufferManager, [DataType::Int32], 4096).unwrap(),
            inputs: vec![ExpressionState::empty()],
        };

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();

        expr.eval(&mut batch, &mut state, FlatSelection::linear(3), &mut out)
            .unwrap();

        let out_slice = out.data().try_as_slice::<PhysicalI32>().unwrap();
        assert_eq!(&[6, 7, 8], out_slice);
    }
}

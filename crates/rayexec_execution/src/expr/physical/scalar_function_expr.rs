use std::fmt;

use fmtutil::IntoDisplayableSlice;
use rayexec_error::Result;

use super::{ExpressionState, PhysicalScalarExpression};
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch_exp::Batch;
use crate::database::DatabaseContext;
use crate::expr::physical::evaluator::ExpressionEvaluator;
use crate::functions::scalar::PlannedScalarFunction;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalScalarFunctionExpr {
    pub function: PlannedScalarFunction,
    pub inputs: Vec<PhysicalScalarExpression>,
}

impl PhysicalScalarFunctionExpr {
    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        // Eval children.
        for (child_idx, array) in state.buffer.arrays_mut().iter_mut().enumerate() {
            let expr = &self.inputs[child_idx];
            let child_state = &mut state.inputs[child_idx];
            ExpressionEvaluator::eval_expression(expr, input, child_state, sel, array)?;
        }

        // Eval function with child outputs.
        state.buffer.set_num_rows(sel.len())?;
        self.function.function_impl.execute(&state.buffer, output)?;

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

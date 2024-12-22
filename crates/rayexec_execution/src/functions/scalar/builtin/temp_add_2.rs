use rayexec_bullet::datatype::{DataTypeId, DataTypeOld};
use rayexec_error::Result;

use super::ScalarFunction;
use crate::arrays::batch::Batch;
use crate::arrays::buffer::physical_type::PhysicalI32;
use crate::arrays::executor::scalar::unary::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempAdd2;

impl FunctionInfo for TempAdd2 {
    fn name(&self) -> &'static str {
        "temp_add_2"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Int32],
            variadic_arg: None,
            return_type: DataTypeId::Int32,
            doc: None,
        }]
    }
}

impl ScalarFunction for TempAdd2 {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        let datatype = inputs[0].datatype(table_list)?;
        if datatype != DataTypeOld::Int32 {
            return Err(invalid_input_types_error(self, &[datatype]));
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataTypeOld::Int32,
            inputs,
            function_impl: Box::new(TempAdd2Impl),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempAdd2Impl;

impl ScalarFunctionImpl for TempAdd2Impl {
    fn execute(&self, batch: &Batch, out: OutBuffer) -> Result<()> {
        let input = batch.get_array(0)?;

        UnaryExecutor::execute::<PhysicalI32, PhysicalI32, _>(
            input,
            batch.generate_selection(),
            out,
            |&v, buf| buf.put(&(v + 2)),
        )
    }
}

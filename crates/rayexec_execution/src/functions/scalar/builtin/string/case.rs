use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::{RayexecError, Result};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lower;

impl FunctionInfo for Lower {
    fn name(&self) -> &'static str {
        "lower"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Lower {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Utf8 => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Utf8,
                inputs,
                function_impl: Box::new(LowerImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LowerImpl;

impl ScalarFunctionImpl for LowerImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        case_convert_execute(input, str::to_lowercase)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Upper;

impl FunctionInfo for Upper {
    fn name(&self) -> &'static str {
        "upper"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Upper {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Utf8 => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Utf8,
                inputs,
                function_impl: Box::new(UpperImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpperImpl;

impl ScalarFunctionImpl for UpperImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        case_convert_execute(input, str::to_uppercase)
    }
}

fn case_convert_execute<F>(input: &Array, case_fn: F) -> Result<Array>
where
    F: Fn(&str) -> String,
{
    let cap = match input.array_data() {
        ArrayData::Binary(bin) => bin.binary_data_size_bytes(),
        _ => return Err(RayexecError::new("Unexpected array data type")),
    };

    let builder = ArrayBuilder {
        datatype: DataType::Utf8,
        buffer: GermanVarlenBuffer::<str>::with_len_and_data_capacity(input.logical_len(), cap),
    };

    UnaryExecutor::execute::<PhysicalUtf8, _, _>(input, builder, |v, buf| {
        // TODO: Non-allocating variant.
        buf.put(&case_fn(v))
    })
}

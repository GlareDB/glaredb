use rayexec_error::Result;
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
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
            doc: Some(&Documentation {
                category: Category::String,
                description: "Convert the string to lowercase.",
                arguments: &["string"],
                example: Some(Example {
                    example: "lower('ABC')",
                    output: "abc",
                }),
            }),
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];
        case_convert_execute(input, sel, str::to_lowercase, output)
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
            doc: Some(&Documentation {
                category: Category::String,
                description: "Convert the string to uppercase.",
                arguments: &["string"],
                example: Some(Example {
                    example: "lower('abc')",
                    output: "ABC",
                }),
            }),
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];
        case_convert_execute(input, sel, str::to_uppercase, output)
    }
}

// TODO: Reusable string buffer.
fn case_convert_execute<F>(
    input: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    case_fn: F,
    output: &mut Array,
) -> Result<()>
where
    F: Fn(&str) -> String,
{
    UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
        input,
        sel,
        OutBuffer::from_array(output)?,
        |v, buf| buf.put(&case_fn(v)),
    )
}

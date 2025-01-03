use rayexec_error::Result;

use crate::arrays::array::exp::Array;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::physical_type::{PhysicalI32, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::arrays::executor_exp::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ascii;

impl FunctionInfo for Ascii {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Int32,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Get the ascii code of the first character of the argument.",
                arguments: &["string"],
                example: Some(Example {
                    example: "ascii('h')",
                    output: "104",
                }),
            }),
        }]
    }
}

impl ScalarFunction for Ascii {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;

        match inputs[0].datatype(table_list)? {
            DataType::Utf8 => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Int32,
                inputs,
                function_impl: Box::new(AsciiImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsciiImpl;

impl ScalarFunctionImpl for AsciiImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalI32, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| {
                let v = v.chars().next().map(|c| c as i32).unwrap_or(0);
                buf.put(&v)
            },
        )
    }
}

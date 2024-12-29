use std::fmt::Debug;

use rayexec_error::Result;

use crate::arrays::array::Array2;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use crate::arrays::executor::physical_type::{PhysicalI64_2, PhysicalUtf8_2};
use crate::arrays::executor::scalar::BinaryExecutor2;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repeat;

impl FunctionInfo for Repeat {
    fn name(&self) -> &'static str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Int64],
            variadic_arg: None,
            return_type: DataTypeId::Utf8,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Repeat a string some number of times.",
                arguments: &["string", "count"],
                example: Some(Example {
                    example: "repeat('abc', 3)",
                    output: "abcabcabc",
                }),
            }),
        }]
    }
}

impl ScalarFunction for Repeat {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 2)?;
        match (
            inputs[0].datatype(table_list)?,
            inputs[1].datatype(table_list)?,
        ) {
            (DataType::Utf8, DataType::Int64) => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Utf8,
                inputs,
                function_impl: Box::new(RepeatUtf8Impl),
            }),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RepeatUtf8Impl;

impl ScalarFunctionImpl for RepeatUtf8Impl {
    fn execute2(&self, inputs: &[&Array2]) -> Result<Array2> {
        let strings = inputs[0];
        let nums = inputs[1];

        // TODO: Capacity

        let mut string_buf = String::new();

        BinaryExecutor2::execute::<PhysicalUtf8_2, PhysicalI64_2, _, _>(
            strings,
            nums,
            ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::with_len(strings.logical_len()),
            },
            |s, num, buf| {
                string_buf.clear();
                for _ in 0..num {
                    string_buf.push_str(s);
                }
                buf.put(string_buf.as_str())
            },
        )
    }
}

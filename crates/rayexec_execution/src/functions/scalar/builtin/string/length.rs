use rayexec_error::Result;

use crate::arrays::array::physical_type::{PhysicalBinary, PhysicalI64, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction2, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Length;

impl FunctionInfo for Length {
    fn name(&self) -> &'static str {
        "length"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["char_length", "character_length"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Int64,
            doc: Some(&Documentation {
                category: Category::String,
                description: "Get the number of characters in a string.",
                arguments: &["string"],
                example: Some(Example {
                    example: "length('tschüß')",
                    output: "6",
                }),
            }),
        }]
    }
}

impl ScalarFunction2 for Length {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction2> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Utf8 => Ok(PlannedScalarFunction2 {
                function: Box::new(*self),
                return_type: DataType::Int64,
                inputs,
                function_impl: Box::new(StrLengthImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrLengthImpl;

impl ScalarFunctionImpl for StrLengthImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalI64, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |s, buf| {
                let len = s.chars().count() as i64;
                buf.put(&len)
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteLength;

impl FunctionInfo for ByteLength {
    fn name(&self) -> &'static str {
        "byte_length"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["octet_length"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Int64,
                doc: Some(&Documentation {
                    category: Category::String,
                    description: "Get the number of bytes in a string.",
                    arguments: &["string"],
                    example: Some(Example {
                        example: "byte_length('tschüß')",
                        output: "6",
                    }),
                }),
            },
            Signature {
                positional_args: &[DataTypeId::Binary],
                variadic_arg: None,
                return_type: DataTypeId::Int64,
                doc: Some(&Documentation {
                    category: Category::String,
                    description: "Get the number of bytes in a binary blob.",
                    arguments: &["blob"],
                    example: None,
                }),
            },
        ]
    }
}

impl ScalarFunction2 for ByteLength {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction2> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Utf8 | DataType::Binary => Ok(PlannedScalarFunction2 {
                function: Box::new(*self),
                return_type: DataType::Int64,
                inputs,
                function_impl: Box::new(ByteLengthImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteLengthImpl;

impl ScalarFunctionImpl for ByteLengthImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        // Binary applicable to both str and [u8].
        UnaryExecutor::execute::<PhysicalBinary, PhysicalI64, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| buf.put(&(v.len() as i64)),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitLength;

impl FunctionInfo for BitLength {
    fn name(&self) -> &'static str {
        "bit_length"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                positional_args: &[DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Int64,
                doc: Some(&Documentation {
                    category: Category::String,
                    description: "Get the number of bits in a string.",
                    arguments: &["string"],
                    example: Some(Example {
                        example: "bit_length('tschüß')",
                        output: "64",
                    }),
                }),
            },
            Signature {
                positional_args: &[DataTypeId::Binary],
                variadic_arg: None,
                return_type: DataTypeId::Int64,
                doc: Some(&Documentation {
                    category: Category::String,
                    description: "Get the number of bits in a binary blob.",
                    arguments: &["blob"],
                    example: None,
                }),
            },
        ]
    }
}

impl ScalarFunction2 for BitLength {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction2> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Utf8 | DataType::Binary => Ok(PlannedScalarFunction2 {
                function: Box::new(*self),
                return_type: DataType::Int64,
                inputs,
                function_impl: Box::new(BitLengthImpl),
            }),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitLengthImpl;

impl ScalarFunctionImpl for BitLengthImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];

        // Binary applicable to both str and [u8].
        UnaryExecutor::execute::<PhysicalBinary, PhysicalI64, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |v, buf| {
                let bit_len = v.len() * 8;
                buf.put(&(bit_len as i64))
            },
        )
    }
}

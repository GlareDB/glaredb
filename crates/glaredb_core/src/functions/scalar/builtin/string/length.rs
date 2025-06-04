use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBinary, PhysicalI64, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_LENGTH: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("length"),
    aliases: &[FnName::default("char_length"), FnName::default("character_length")],
    doc: &[&Documentation {
        category: Category::String,
        description: "Get the number of characters in a string.",
        arguments: &["string"],
        example: Some(Example {
            example: "length('tschüß')",
            output: "6",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8], DataTypeId::Int64),
        &StringLength,
    )],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringLength;

impl ScalarFunction for StringLength {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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

pub const FUNCTION_SET_BYTE_LENGTH: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("byte_length"),
    aliases: &[FnName::default("octet_length")],
    doc: &[&Documentation {
        category: Category::String,
        description: "Get the number of bytes in a string or blob.",
        arguments: &["string"],
        example: Some(Example {
            example: "byte_length('tschüß')",
            output: "6",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Int64),
            &ByteLength,
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Binary], DataTypeId::Int64),
            &ByteLength,
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteLength;

impl ScalarFunction for ByteLength {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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

pub const FUNCTION_SET_BIT_LENGTH: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("bit_length"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::String,
        description: "Get the number of bits in a string or blob.",
        arguments: &["string"],
        example: Some(Example {
            example: "bit_length('tschüß')",
            output: "64",
        }),
    }],
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Int64),
            &BitLength,
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Binary], DataTypeId::Int64),
            &BitLength,
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitLength;

impl ScalarFunction for BitLength {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
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

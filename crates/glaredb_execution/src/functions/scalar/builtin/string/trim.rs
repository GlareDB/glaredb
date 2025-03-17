use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub trait StringTrimOp: Sync + Send + Debug + Clone + Copy + PartialEq + Eq + 'static {
    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str;
}

pub const FUNCTION_SET_BTRIM: ScalarFunctionSet = ScalarFunctionSet {
    name: "btrim",
    aliases: &["trim"],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Trim matching characters from both sides of the string.",
        arguments: &["string", "characters"],
        example: Some(Example {
            example: "trim('->hello<', '<>-')",
            output: "hello",
        }),
    }),
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Utf8),
            &BothTrimOp,
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &BothTrimOp,
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BothTrimOp;

impl StringTrimOp for BothTrimOp {
    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_matches(|c| pattern.contains(c))
    }
}

impl ScalarFunction for BothTrimOp {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        trim::<Self>(input.selection(), input.arrays(), output)
    }
}

pub const FUNCTION_SET_LTRIM: ScalarFunctionSet = ScalarFunctionSet {
    name: "ltrim",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Trim matching characters from the left side of the string.",
        arguments: &["string", "characters"],
        example: Some(Example {
            example: "ltrim('->hello<', '<>-')",
            output: "hello<",
        }),
    }),
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Utf8),
            &LeftTrimOp,
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &LeftTrimOp,
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftTrimOp;

impl StringTrimOp for LeftTrimOp {
    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_start_matches(|c| pattern.contains(c))
    }
}

impl ScalarFunction for LeftTrimOp {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        trim::<Self>(input.selection(), input.arrays(), output)
    }
}

pub const FUNCTION_SET_RTRIM: ScalarFunctionSet = ScalarFunctionSet {
    name: "rtrim",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::String,
        description: "Trim whitespace from the right side of the string.",
        arguments: &["string"],
        example: Some(Example {
            example: "rtrim('  hello ')",
            output: "  hello",
        }),
    }),
    functions: &[
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Utf8),
            &RightTrimOp,
        ),
        RawScalarFunction::new(
            &Signature::new(&[DataTypeId::Utf8], DataTypeId::Utf8),
            &RightTrimOp,
        ),
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightTrimOp;

impl StringTrimOp for RightTrimOp {
    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_end_matches(|c| pattern.contains(c))
    }
}

impl ScalarFunction for RightTrimOp {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Ok(BindState {
            state: (),
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        trim::<Self>(input.selection(), input.arrays(), output)
    }
}

fn trim<F>(sel: Selection, inputs: &[Array], output: &mut Array) -> Result<()>
where
    F: StringTrimOp,
{
    match inputs.len() {
        1 => UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _>(
            &inputs[0],
            sel,
            OutBuffer::from_array(output)?,
            |s, buf| {
                let trimmed = F::trim_func(s, " ");
                buf.put(trimmed);
            },
        ),
        2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _>(
            &inputs[0],
            sel,
            &inputs[1],
            sel,
            OutBuffer::from_array(output)?,
            |s, pattern, buf| {
                let trimmed = F::trim_func(s, pattern);
                buf.put(trimmed);
            },
        ),
        other => panic!("invalid number of inputs: {other}"),
    }
}

use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args_one_of,
    FunctionInfo,
    Signature,
};

pub type LeftTrim = Trim<LeftTrimOp>;
pub type RightTrim = Trim<RightTrimOp>;
pub type BTrim = Trim<BothTrimOp>;

pub trait StringTrimOp: Sync + Send + Debug + Clone + Copy + PartialEq + Eq + 'static {
    const NAME: &'static str;
    const ALIASES: &'static [&'static str];

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BothTrimOp;

impl StringTrimOp for BothTrimOp {
    const NAME: &'static str = "btrim";
    const ALIASES: &'static [&'static str] = &["trim"];

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_matches(|c| pattern.contains(c))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftTrimOp;

impl StringTrimOp for LeftTrimOp {
    const NAME: &'static str = "ltrim";
    const ALIASES: &'static [&'static str] = &[];

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_start_matches(|c| pattern.contains(c))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightTrimOp;

impl StringTrimOp for RightTrimOp {
    const NAME: &'static str = "rtrim";
    const ALIASES: &'static [&'static str] = &[];

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_end_matches(|c| pattern.contains(c))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim<F: StringTrimOp> {
    _op: PhantomData<F>,
}

impl<F: StringTrimOp> Trim<F> {
    pub const fn new() -> Self {
        Trim { _op: PhantomData }
    }
}

impl<F: StringTrimOp> Default for Trim<F> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F: StringTrimOp> FunctionInfo for Trim<F> {
    fn name(&self) -> &'static str {
        F::NAME
    }

    fn aliases(&self) -> &'static [&'static str] {
        F::ALIASES
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Utf8,
            },
        ]
    }
}

impl<F: StringTrimOp> ScalarFunction for Trim<F> {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(TrimImpl::<F> { _op: PhantomData }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args_one_of(self, inputs, [1, 2])?;

        match inputs.len() {
            1 => match &inputs[0] {
                DataType::Utf8 => Ok(Box::new(TrimImpl::<F> { _op: PhantomData })),
                a => Err(invalid_input_types_error(self, &[a])),
            },
            2 => match (&inputs[0], &inputs[1]) {
                (DataType::Utf8, DataType::Utf8) => {
                    Ok(Box::new(TrimImpl::<F> { _op: PhantomData }))
                }
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            },
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrimImpl<F: StringTrimOp> {
    _op: PhantomData<F>,
}

impl<F: StringTrimOp> PlannedScalarFunction for TrimImpl<F> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Trim::<F> { _op: PhantomData }
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        match inputs.len() {
            1 => UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
                let trimmed = F::trim_func(s, " ");
                buf.put(trimmed)
            }),
            2 => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[1],
                builder,
                |s, pattern, buf| {
                    let trimmed = F::trim_func(s, pattern);
                    buf.put(trimmed)
                },
            ),
            other => unreachable!("num inputs checked, got {other}"),
        }
    }
}

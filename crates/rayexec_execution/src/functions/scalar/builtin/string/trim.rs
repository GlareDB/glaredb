use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::Result;

use crate::expr::Expression;
use crate::functions::scalar::{
    PlannedScalarFunction2,
    PlannedScalarFuntion,
    ScalarFunction,
    ScalarFunctionImpl,
};
use crate::functions::{
    invalid_input_types_error,
    plan_check_num_args_one_of,
    FunctionInfo,
    Signature,
};
use crate::logical::binder::table_list::TableList;

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
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        plan_check_num_args_one_of(self, &inputs, [1, 2])?;

        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        match datatypes.len() {
            1 => match &datatypes[0] {
                DataType::Utf8 => Ok(PlannedScalarFuntion {
                    function: Box::new(*self),
                    return_type: DataType::Utf8,
                    inputs,
                    function_impl: Box::new(TrimWhitespaceImpl::<F>::new()),
                }),
                a => Err(invalid_input_types_error(self, &[a])),
            },
            2 => match (&datatypes[0], &datatypes[1]) {
                (DataType::Utf8, DataType::Utf8) => Ok(PlannedScalarFuntion {
                    function: Box::new(*self),
                    return_type: DataType::Utf8,
                    inputs,
                    function_impl: Box::new(TrimPatternImpl::<F>::new()),
                }),
                (a, b) => Err(invalid_input_types_error(self, &[a, b])),
            },
            _ => Err(invalid_input_types_error(self, &datatypes)),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TrimWhitespaceImpl<F: StringTrimOp> {
    _op: PhantomData<F>,
}

impl<F: StringTrimOp> TrimWhitespaceImpl<F> {
    fn new() -> Self {
        TrimWhitespaceImpl { _op: PhantomData }
    }
}

impl<F: StringTrimOp> ScalarFunctionImpl for TrimWhitespaceImpl<F> {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
            let trimmed = F::trim_func(s, " ");
            buf.put(trimmed)
        })
    }
}

#[derive(Debug, Clone)]
pub struct TrimPatternImpl<F: StringTrimOp> {
    _op: PhantomData<F>,
}

impl<F: StringTrimOp> TrimPatternImpl<F> {
    fn new() -> Self {
        TrimPatternImpl { _op: PhantomData }
    }
}

impl<F: StringTrimOp> ScalarFunctionImpl for TrimPatternImpl<F> {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
            inputs[0],
            inputs[1],
            builder,
            |s, pattern, buf| {
                let trimmed = F::trim_func(s, pattern);
                buf.put(trimmed)
            },
        )
    }
}

use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_error::Result;

use crate::arrays::array::physical_type::PhysicalUtf8;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::arrays::executor::OutBuffer;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
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

    const DOC_ONE_ARG: &'static Documentation;
    const DOC_TWO_ARGS: &'static Documentation;

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BothTrimOp;

impl StringTrimOp for BothTrimOp {
    const NAME: &'static str = "btrim";
    const ALIASES: &'static [&'static str] = &["trim"];

    const DOC_ONE_ARG: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim whitespace from both sides of the string.",
        arguments: &["string"],
        example: Some(Example {
            example: "trim('  hello ')",
            output: "hello",
        }),
    };

    const DOC_TWO_ARGS: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim matching characters from both sides of the string.",
        arguments: &["string", "characters"],
        example: Some(Example {
            example: "trim('->hello<', '<>-')",
            output: "hello",
        }),
    };

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_matches(|c| pattern.contains(c))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftTrimOp;

impl StringTrimOp for LeftTrimOp {
    const NAME: &'static str = "ltrim";
    const ALIASES: &'static [&'static str] = &[];

    const DOC_ONE_ARG: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim whitespace from the left side of the string.",
        arguments: &["string"],
        example: Some(Example {
            example: "ltrim('  hello ')",
            output: "hello ",
        }),
    };

    const DOC_TWO_ARGS: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim matching characters from the left side of the string.",
        arguments: &["string", "characters"],
        example: Some(Example {
            example: "ltrim('->hello<', '<>-')",
            output: "hello<",
        }),
    };

    fn trim_func<'a>(input: &'a str, pattern: &str) -> &'a str {
        input.trim_start_matches(|c| pattern.contains(c))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RightTrimOp;

impl StringTrimOp for RightTrimOp {
    const NAME: &'static str = "rtrim";
    const ALIASES: &'static [&'static str] = &[];

    const DOC_ONE_ARG: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim whitespace from the right side of the string.",
        arguments: &["string"],
        example: Some(Example {
            example: "rtrim('  hello ')",
            output: "  hello",
        }),
    };

    const DOC_TWO_ARGS: &'static Documentation = &Documentation {
        category: Category::String,
        description: "Trim matching characters from the right side of the string.",
        arguments: &["string", "characters"],
        example: Some(Example {
            example: "rtrim('->hello<', '<>-')",
            output: "->hello",
        }),
    };

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
                positional_args: &[DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(F::DOC_ONE_ARG),
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
                variadic_arg: None,
                return_type: DataTypeId::Utf8,
                doc: Some(F::DOC_TWO_ARGS),
            },
        ]
    }
}

impl<F: StringTrimOp> ScalarFunction for Trim<F> {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args_one_of(self, &inputs, [1, 2])?;

        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        match datatypes.len() {
            1 => match &datatypes[0] {
                DataType::Utf8 => Ok(PlannedScalarFunction {
                    function: Box::new(*self),
                    return_type: DataType::Utf8,
                    inputs,
                    function_impl: Box::new(TrimWhitespaceImpl::<F>::new()),
                }),
                a => Err(invalid_input_types_error(self, &[a])),
            },
            2 => match (&datatypes[0], &datatypes[1]) {
                (DataType::Utf8, DataType::Utf8) => Ok(PlannedScalarFunction {
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        UnaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
            &input.arrays()[0],
            sel,
            OutBuffer::from_array(output)?,
            |s, buf| {
                let trimmed = F::trim_func(s, " ");
                buf.put(trimmed);
            },
        )
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _, _>(
            &input.arrays()[0],
            sel,
            &input.arrays()[1],
            sel,
            OutBuffer::from_array(output)?,
            |s, pattern, buf| {
                let trimmed = F::trim_func(s, pattern);
                buf.put(trimmed);
            },
        )
    }
}

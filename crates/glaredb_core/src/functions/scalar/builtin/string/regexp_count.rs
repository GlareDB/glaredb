use glaredb_error::{Result, ResultExt};
use regex::Regex;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalI64, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::BinaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_REGEXP_COUNT: ScalarFunctionSet = ScalarFunctionSet {
    name: "regexp_count",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Regexp,
        description: "Count the number of non-overlapping occurrences of a regular expression pattern in a string.",
        arguments: &["string", "regexp"],
        example: Some(Example {
            example: "regexp_count('abacad', 'a|b')",
            output: "4",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Int64,
        ),
        &RegexpCount,
    )],
};

#[derive(Debug)]
pub struct RegexpCountState {
    pattern: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegexpCount;

impl ScalarFunction for RegexpCount {
    type State = RegexpCountState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;
            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        Ok(BindState {
            state: RegexpCountState {
                pattern,
            },
            return_type: DataType::Int64,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match state.pattern.as_ref() {
            Some(pattern) => {
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, _>(
                    &input.arrays()[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, _pattern, buf| {
                        let count = pattern.find_iter(s).count() as i64;
                        buf.put(&count);
                    },
                )
            }
            None => {
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, _>(
                    &input.arrays()[0],
                    sel,
                    &input.arrays()[1],
                    sel,
                    OutBuffer::from_array(output)?,
                    |s, pattern, buf| {
                        let pattern = match Regex::new(pattern) {
                            Ok(pattern) => pattern,
                            Err(_) => {
                                return;
                            }
                        };

                        let count = pattern.find_iter(s).count() as i64;
                        buf.put(&count);
                    },
                )
            }
        }
    }
}

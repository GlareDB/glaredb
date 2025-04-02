use glaredb_error::{Result, ResultExt};
use regex::Regex;

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{PhysicalBool, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::{BinaryExecutor, UnaryExecutor};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_REGEXP_LIKE: ScalarFunctionSet = ScalarFunctionSet {
    name: "regexp_like",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Regexp,
        description: "Returns true if the string matches the regular expression pattern.",
        arguments: &["string", "pattern"],
        example: Some(Example {
            example: "regexp_like('cat dog house', 'dog')",
            output: "true",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Boolean),
        &RegexpLike,
    )],
};

#[derive(Debug)]
pub struct RegexpLikeState {
    constant: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegexpLike;

impl ScalarFunction for RegexpLike {
    type State = RegexpLikeState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let constant = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        Ok(BindState {
            state: RegexpLikeState { constant },
            return_type: DataType::Boolean,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let strings = &input.arrays()[0];
        let patterns = &input.arrays()[1];

        match state.constant.as_ref() {
            Some(regex) => UnaryExecutor::execute::<PhysicalUtf8, PhysicalBool, _>(
                strings,
                sel,
                OutBuffer::from_array(output)?,
                |s, buf| {
                    let b = regex.is_match(s);
                    buf.put(&b);
                },
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalBool, _>(
                strings,
                sel,
                patterns,
                sel,
                OutBuffer::from_array(output)?,
                |s, pattern, buf| match Regex::new(pattern) {
                    Ok(regex) => {
                        let b = regex.is_match(s);
                        buf.put(&b);
                    }
                    Err(_) => {
                        // TODO: Handle error
                        buf.put(&false);
                    }
                },
            ),
        }
    }
}

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

pub const FUNCTION_SET_REGEXP_INSTR: ScalarFunctionSet = ScalarFunctionSet {
    name: "regexp_instr",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Regexp,
        description: "Returns the starting position of the first match of a regular expression pattern in a string.",
        arguments: &["string", "pattern"],
        example: Some(Example {
            example: "regexp_instr('abcdef', 'cd')",
            output: "3",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Int64),
        &RegexpInstr,
    )],
};

#[derive(Debug)]
pub struct RegexpInstrState {
    pattern: Option<Regex>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegexpInstr;

impl ScalarFunction for RegexpInstr {
    type State = RegexpInstrState;

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
            state: RegexpInstrState { pattern },
            return_type: DataType::int64(),
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match state.pattern.as_ref() {
            Some(pattern) => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, _>(
                &input.arrays()[0],
                sel,
                &input.arrays()[1],
                sel,
                OutBuffer::from_array(output)?,
                |s, _pattern, buf| {
                    let position = pattern.find(s).map_or(0, |m| m.start() + 1) as i64;
                    buf.put(&position);
                },
            ),
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalI64, _>(
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

                    let position = pattern.find(s).map_or(0, |m| m.start() + 1) as i64;
                    buf.put(&position);
                },
            ),
        }
    }
}

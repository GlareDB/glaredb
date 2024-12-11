use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, TernaryExecutor, UnaryExecutor};
use rayexec_error::{Result, ResultExt};
use regex::Regex;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegexpReplace;

impl FunctionInfo for RegexpReplace {
    fn name(&self) -> &'static str {
        "regexp_replace"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for RegexpReplace {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 3)?;
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        for datatype in &datatypes {
            if datatype != &DataType::Utf8 {
                return Err(invalid_input_types_error(self, &datatypes));
            }
        }

        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(table_list, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;
            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        let replacement = if inputs[2].is_const_foldable() {
            let replacement = ConstFold::rewrite(table_list, inputs[2].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(replacement)
        } else {
            None
        };

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(RegexpReplaceImpl {
                pattern,
                replacement,
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct RegexpReplaceImpl {
    pub pattern: Option<Regex>,
    pub replacement: Option<String>,
}

impl ScalarFunctionImpl for RegexpReplaceImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::<str>::with_len(inputs[0].logical_len()),
        };

        match (self.pattern.as_ref(), self.replacement.as_ref()) {
            (Some(pattern), Some(replacement)) => {
                UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
                    // TODO: Flags to more many.
                    let out = pattern.replace(s, replacement);
                    buf.put(out.as_ref());
                })
            }
            (Some(pattern), None) => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[2],
                builder,
                |s, replacement, buf| {
                    let out = pattern.replace(s, replacement);
                    buf.put(out.as_ref());
                },
            ),
            (None, Some(replacement)) => {
                BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                    inputs[0],
                    inputs[1],
                    builder,
                    |s, pattern, buf| {
                        let pattern = match Regex::new(pattern) {
                            Ok(pattern) => pattern,
                            Err(_) => {
                                // TODO: Do something.
                                return;
                            }
                        };

                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
            (None, None) => {
                TernaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, PhysicalUtf8, _, _>(
                    inputs[0],
                    inputs[1],
                    inputs[2],
                    builder,
                    |s, pattern, replacement, buf| {
                        let pattern = match Regex::new(pattern) {
                            Ok(pattern) => pattern,
                            Err(_) => {
                                // TODO: Do something.
                                return;
                            }
                        };

                        let out = pattern.replace(s, replacement);
                        buf.put(out.as_ref());
                    },
                )
            }
        }
    }
}

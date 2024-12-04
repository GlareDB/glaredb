use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, TernaryExecutor, UnaryExecutor};
use rayexec_error::{Result, ResultExt};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::util_types;
use regex::Regex;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
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
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let mut decoder = PackedDecoder::new(state);

        let pattern: util_types::OptionalString = decoder.decode_next()?;
        let replacement: util_types::OptionalString = decoder.decode_next()?;

        let pattern = pattern
            .value
            .as_ref()
            .map(|s| Regex::new(s))
            .transpose()
            .context("Failed to rebuild regex")?;

        Ok(Box::new(RegexpReplaceImpl {
            pattern,
            replacement: replacement.value,
        }))
    }

    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 3)?;
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        for datatype in &datatypes {
            if datatype != &DataType::Utf8 {
                return Err(invalid_input_types_error(self, &datatypes));
            }
        }

        let pattern = if inputs[1].is_const_foldable() {
            let pattern = ConstFold::rewrite(bind_context, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;
            let pattern = Regex::new(&pattern).context("Failed to build regexp pattern")?;

            Some(pattern)
        } else {
            None
        };

        let replacement = if inputs[2].is_const_foldable() {
            let replacement = ConstFold::rewrite(bind_context, inputs[2].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(replacement)
        } else {
            None
        };

        Ok(Box::new(RegexpReplaceImpl {
            pattern,
            replacement,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct RegexpReplaceImpl {
    pub pattern: Option<Regex>,
    pub replacement: Option<String>,
}

impl PlannedScalarFunction for RegexpReplaceImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &RegexpReplace
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut encoder = PackedEncoder::new(state);

        let pattern = self.pattern.as_ref().map(|c| c.to_string());
        let replacement = self.replacement.clone();

        encoder.encode_next(&util_types::OptionalString { value: pattern })?;
        encoder.encode_next(&util_types::OptionalString { value: replacement })?;

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

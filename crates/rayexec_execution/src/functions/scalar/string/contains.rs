use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::{BinaryExecutor, UnaryExecutor};
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::util_types;

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Contains;

impl FunctionInfo for Contains {
    fn name(&self) -> &'static str {
        "contains"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::LargeUtf8, DataTypeId::LargeUtf8],
                variadic: None,
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl ScalarFunction for Contains {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let constant: util_types::OptionalString = PackedDecoder::new(state).decode_next()?;
        Ok(Box::new(StringContainsImpl {
            constant: constant.value,
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
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        match (&datatypes[0], &datatypes[1]) {
            (DataType::Utf8, DataType::Utf8) => (),
            (DataType::LargeUtf8, DataType::LargeUtf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        let constant = if inputs[1].is_const_foldable() {
            let search_string = ConstFold::rewrite(bind_context, inputs[1].clone())?
                .try_into_scalar()?
                .try_into_string()?;

            Some(search_string)
        } else {
            None
        };

        Ok(Box::new(StringContainsImpl { constant }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringContainsImpl {
    pub constant: Option<String>,
}

impl PlannedScalarFunction for StringContainsImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Contains
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&util_types::OptionalString {
            value: self.constant.clone(),
        })
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(inputs[0].logical_len()),
        };

        match self.constant.as_ref() {
            Some(constant) => {
                UnaryExecutor::execute::<PhysicalUtf8, _, _>(inputs[0], builder, |s, buf| {
                    buf.put(&s.contains(constant))
                })
            }
            None => BinaryExecutor::execute::<PhysicalUtf8, PhysicalUtf8, _, _>(
                inputs[0],
                inputs[1],
                builder,
                |s, c, buf| buf.put(&s.contains(c)),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_contains() {
        let contains = StringContainsImpl {
            constant: Some("const".to_string()),
        };

        let mut buf = Vec::new();
        contains.encode_state(&mut buf).unwrap();

        let got = Contains.decode_state(&buf).unwrap();
        assert_eq!("contains", got.scalar_function().name());
    }
}

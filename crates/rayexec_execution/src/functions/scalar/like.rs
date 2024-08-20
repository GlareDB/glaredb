use std::sync::Arc;

use rayexec_bullet::{
    array::Array,
    datatype::{DataType, DataTypeId},
    field::TypeSchema,
};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_proto::{
    packed::{PackedDecoder, PackedEncoder},
    util_types,
};
use serde::{Deserialize, Serialize};

use crate::{
    functions::{
        invalid_input_types_error, plan_check_num_args,
        scalar::macros::{primitive_binary_execute_bool, primitive_unary_execute_bool},
        FunctionInfo, Signature,
    },
    logical::{consteval::ConstEval, expr::LogicalExpression},
};

use super::{comparison::EqImpl, PlannedScalarFunction, ScalarFunction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Like;

impl FunctionInfo for Like {
    fn name(&self) -> &'static str {
        "like"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // like(input, pattern)
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

impl ScalarFunction for Like {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let mut packed = PackedDecoder::new(state);
        let variant: String = packed.decode_next()?;
        match variant.as_str() {
            "starts_with" => {
                let constant: util_types::OptionalString = packed.decode_next()?;
                Ok(Box::new(StartsWithImpl {
                    constant: constant.value,
                }))
            }
            "ends_with" => {
                let constant: util_types::OptionalString = packed.decode_next()?;
                Ok(Box::new(EndsWithImpl {
                    constant: constant.value,
                }))
            }
            "contains" => {
                let constant: util_types::OptionalString = packed.decode_next()?;
                Ok(Box::new(ContainsImpl {
                    constant: constant.value,
                }))
            }
            other => Err(RayexecError::new(format!(
                "Unknown variant for like: {other}"
            ))),
        }
    }

    fn plan_from_expressions(
        &self,
        inputs: &[&LogicalExpression],
        operator_schema: &TypeSchema,
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(operator_schema, &[]))
            .collect::<Result<Vec<_>>>()?;

        // TODO: 3rd arg for optional escape char
        plan_check_num_args(self, &datatypes, 2)?;

        match (&datatypes[0], &datatypes[1]) {
            (DataType::Utf8, DataType::Utf8) => (),
            (DataType::LargeUtf8, DataType::LargeUtf8) => (),
            (a, b) => return Err(invalid_input_types_error(self, &[a, b])),
        }

        if inputs[1].is_constant() {
            let pattern = ConstEval::default()
                .fold(inputs[1].clone())?
                .try_unwrap_constant()?
                .try_into_string()?;

            let escape_char = b'\\'; // TODO: Possible to get from the user at some point.

            // Iterators for '%' and '_'. These lets us check the pattern string
            // for simple patterns, allowing us to skip regex if it's not
            // needed.
            //
            // The percents iterator is fused because we may call it again after
            // receiving a None.
            let mut percents = pattern.char_indices().filter(|(_, c)| *c == '%').fuse();
            let mut underscores = pattern.char_indices().filter(|(_, c)| *c == '_');

            match (percents.next(), percents.next(), underscores.next()) {
                // '%search'
                (Some((0, _)), None, None) => {
                    let pattern = pattern.trim_matches('%').to_string();
                    Ok(Box::new(EndsWithImpl {
                        constant: Some(pattern),
                    }))
                }
                // 'search%'
                (Some((n, _)), None, None)
                    if n == pattern.len() - 1 && pattern.as_bytes()[n - 1] != escape_char =>
                {
                    let pattern = pattern.trim_matches('%').to_string();
                    Ok(Box::new(StartsWithImpl {
                        constant: Some(pattern),
                    }))
                }
                // '%search%'
                (Some((0, _)), Some((n, _)), None)
                    if n == pattern.len() - 1 && pattern.as_bytes()[n - 1] != escape_char =>
                {
                    let pattern = pattern.trim_matches('%').to_string();
                    Ok(Box::new(ContainsImpl {
                        constant: Some(pattern),
                    }))
                }
                // 'search'
                // aka just equals
                (None, None, None) => Ok(Box::new(EqImpl)),
                other => {
                    // TODO: Regex
                    not_implemented!("string search {other:?}")
                }
            }
        } else {
            // TODO: Non-constant variants
            not_implemented!("non-constant string search")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LikeImpl {
    StartsWith(StartsWithImpl),
    EndsWith(EndsWithImpl),
    Contains(ContainsImpl),
    Regex(),
}

impl PlannedScalarFunction for LikeImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Like
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        match self {
            Self::StartsWith(v) => {
                packed.encode_next(&"starts_with".to_string())?;
                packed.encode_next(&util_types::OptionalString {
                    value: v.constant.clone(),
                })?
            }
            Self::EndsWith(v) => {
                packed.encode_next(&"ends_with".to_string())?;
                packed.encode_next(&util_types::OptionalString {
                    value: v.constant.clone(),
                })?
            }
            Self::Contains(v) => {
                packed.encode_next(&"contains".to_string())?;
                packed.encode_next(&util_types::OptionalString {
                    value: v.constant.clone(),
                })?
            }
            Self::Regex() => {
                not_implemented!("regex")
            }
        }

        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        match self {
            Self::StartsWith(f) => f.execute(inputs),
            Self::EndsWith(f) => f.execute(inputs),
            Self::Contains(f) => f.execute(inputs),
            Self::Regex() => not_implemented!("like regex exec"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartsWith;

impl FunctionInfo for StartsWith {
    fn name(&self) -> &'static str {
        "starts_with"
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

impl ScalarFunction for StartsWith {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let constant: util_types::OptionalString = PackedDecoder::new(state).decode_next()?;
        Ok(Box::new(StartsWithImpl {
            constant: constant.value,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Utf8) | (DataType::LargeUtf8, DataType::LargeUtf8) => {
                Ok(Box::new(StartsWithImpl { constant: None }))
            }
            _ => Err(invalid_input_types_error(self, inputs)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartsWithImpl {
    constant: Option<String>,
}

impl PlannedScalarFunction for StartsWithImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &StartsWith
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&util_types::OptionalString {
            value: self.constant.clone(),
        })
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        match self.constant.as_ref() {
            Some(constant) => Ok(match inputs[0].as_ref() {
                Array::Utf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.starts_with(constant))
                }
                Array::LargeUtf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.starts_with(constant))
                }
                other => panic!("unexpected array type: {}", other.datatype()),
            }),
            None => Ok(match (inputs[0].as_ref(), inputs[1].as_ref()) {
                (Array::Utf8(a), Array::Utf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.starts_with(b))
                }
                (Array::LargeUtf8(a), Array::LargeUtf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.starts_with(b))
                }
                _ => return Err(RayexecError::new("invalid types")),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndsWith;

impl FunctionInfo for EndsWith {
    fn name(&self) -> &'static str {
        "ends_with"
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

impl ScalarFunction for EndsWith {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let constant: util_types::OptionalString = PackedDecoder::new(state).decode_next()?;
        Ok(Box::new(EndsWithImpl {
            constant: constant.value,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Utf8) | (DataType::LargeUtf8, DataType::LargeUtf8) => {
                Ok(Box::new(EndsWithImpl { constant: None }))
            }
            _ => Err(invalid_input_types_error(self, inputs)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndsWithImpl {
    constant: Option<String>,
}

impl PlannedScalarFunction for EndsWithImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &EndsWith
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&util_types::OptionalString {
            value: self.constant.clone(),
        })
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        match self.constant.as_ref() {
            Some(constant) => Ok(match inputs[0].as_ref() {
                Array::Utf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.ends_with(constant))
                }
                Array::LargeUtf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.ends_with(constant))
                }
                other => panic!("unexpected array type: {}", other.datatype()),
            }),
            None => Ok(match (inputs[0].as_ref(), inputs[1].as_ref()) {
                (Array::Utf8(a), Array::Utf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.ends_with(b))
                }
                (Array::LargeUtf8(a), Array::LargeUtf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.ends_with(b))
                }
                _ => return Err(RayexecError::new("invalid types")),
            }),
        }
    }
}

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
        Ok(Box::new(ContainsImpl {
            constant: constant.value,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Utf8) | (DataType::LargeUtf8, DataType::LargeUtf8) => {
                Ok(Box::new(ContainsImpl { constant: None }))
            }
            _ => Err(invalid_input_types_error(self, inputs)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainsImpl {
    constant: Option<String>,
}

impl PlannedScalarFunction for ContainsImpl {
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

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        match self.constant.as_ref() {
            Some(constant) => Ok(match inputs[0].as_ref() {
                Array::Utf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.contains(constant))
                }
                Array::LargeUtf8(arr) => {
                    primitive_unary_execute_bool!(arr, |s| s.contains(constant))
                }
                other => panic!("unexpected array type: {}", other.datatype()),
            }),
            None => Ok(match (inputs[0].as_ref(), inputs[1].as_ref()) {
                (Array::Utf8(a), Array::Utf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.contains(b))
                }
                (Array::LargeUtf8(a), Array::LargeUtf8(b)) => {
                    primitive_binary_execute_bool!(a, b, |a, b| a.contains(b))
                }
                _ => return Err(RayexecError::new("invalid types")),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_contains() {
        let contains = ContainsImpl {
            constant: Some("const".to_string()),
        };

        let mut buf = Vec::new();
        contains.encode_state(&mut buf).unwrap();

        let got = Contains.decode_state(&buf).unwrap();
        assert_eq!("contains", got.scalar_function().name());
    }
}
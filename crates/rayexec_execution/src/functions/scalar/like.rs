use std::sync::Arc;

use rayexec_bullet::{
    array::Array,
    datatype::{DataType, DataTypeId},
    field::TypeSchema,
};
use rayexec_error::Result;

use crate::{
    functions::{
        invalid_input_types_error, plan_check_num_args,
        scalar::macros::primitive_unary_execute_bool, FunctionInfo, Signature,
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
                return_type: DataTypeId::Boolean,
            },
            Signature {
                input: &[DataTypeId::LargeUtf8, DataTypeId::LargeUtf8],
                return_type: DataTypeId::Boolean,
            },
        ]
    }
}

impl ScalarFunction for Like {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        // TODO: Non-const like functions
        unimplemented!()
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
                    Ok(Box::new(EndsWithImpl { pattern }))
                }
                // 'search%'
                (Some((n, _)), None, None)
                    if n == pattern.len() - 1 && pattern.as_bytes()[n - 1] != escape_char =>
                {
                    let pattern = pattern.trim_matches('%').to_string();
                    Ok(Box::new(StartsWithImpl { pattern }))
                }
                // '%search%'
                (Some((0, _)), Some((n, _)), None)
                    if n == pattern.len() - 1 && pattern.as_bytes()[n - 1] != escape_char =>
                {
                    let pattern = pattern.trim_matches('%').to_string();
                    Ok(Box::new(ContainsImpl { pattern }))
                }
                // 'search'
                // aka just equals
                (None, None, None) => Ok(Box::new(EqImpl)),
                other => {
                    // TODO: Regex
                    unimplemented!("{other:?}")
                }
            }
        } else {
            // TODO: Non-constant variants
            unimplemented!()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartsWithImpl {
    pattern: String,
}

impl PlannedScalarFunction for StartsWithImpl {
    fn name(&self) -> &'static str {
        "starts_with_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let arr = inputs[0].as_ref();
        Ok(match arr {
            Array::Utf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.starts_with(&self.pattern))
            }
            Array::LargeUtf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.starts_with(&self.pattern))
            }
            other => panic!("unexpected array type: {}", other.datatype()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndsWithImpl {
    pattern: String,
}

impl PlannedScalarFunction for EndsWithImpl {
    fn name(&self) -> &'static str {
        "ends_with_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let arr = inputs[0].as_ref();
        Ok(match arr {
            Array::Utf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.ends_with(&self.pattern))
            }
            Array::LargeUtf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.ends_with(&self.pattern))
            }
            other => panic!("unexpected array type: {}", other.datatype()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainsImpl {
    pattern: String,
}

impl PlannedScalarFunction for ContainsImpl {
    fn name(&self) -> &'static str {
        "contains_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Arc<Array>]) -> Result<Array> {
        let arr = inputs[0].as_ref();
        Ok(match arr {
            Array::Utf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.contains(&self.pattern))
            }
            Array::LargeUtf8(arr) => {
                primitive_unary_execute_bool!(arr, |s| s.contains(&self.pattern))
            }
            other => panic!("unexpected array type: {}", other.datatype()),
        })
    }
}

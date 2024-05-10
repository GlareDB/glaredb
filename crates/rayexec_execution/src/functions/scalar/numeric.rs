use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, ScalarFn,
    SpecializedScalarFunction,
};
use crate::functions::{InputTypes, ReturnType, Signature};
use rayexec_bullet::array::{BooleanArrayBuilder, PrimitiveArrayBuilder};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

/// Macro for generating a specialized unary function that accepts a primitive
/// array of some variant, and produces a primitive array of some variant.
///
/// Operation should be a lambda accepting one input, and producing one output
/// of the expected type.
macro_rules! generate_specialized_unary_numeric {
    ($name:ident, $input_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
                    let array = arrays[0];
                    Ok(match array.as_ref() {
                        Array::$input_variant(array) => {
                            let mut builder = PrimitiveArrayBuilder::with_capacity(array.len());
                            UnaryExecutor::execute(array, $operation, &mut builder)?;
                            Array::$output_variant(builder.into_typed_array())
                        }
                        other => panic!("unexpected array type: {other:?}"),
                    })
                }

                inner
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNan;

impl GenericScalarFunction for IsNan {
    fn name(&self) -> &str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Boolean),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Boolean),
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(IsNanFloat32)),
            DataType::Float64 => Ok(Box::new(IsNanFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNanFloat32;

impl SpecializedScalarFunction for IsNanFloat32 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f32_impl(arrays: &[&Arc<Array>]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array.as_ref() {
                Array::Float32(array) => {
                    let mut builder = BooleanArrayBuilder::new();
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut builder)?;
                    Array::Boolean(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f32_impl
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNanFloat64;

impl SpecializedScalarFunction for IsNanFloat64 {
    fn function_impl(&self) -> ScalarFn {
        fn is_nan_f64_impl(arrays: &[&Arc<Array>]) -> Result<Array> {
            let array = arrays[0];
            Ok(match array.as_ref() {
                Array::Float64(array) => {
                    let mut builder = BooleanArrayBuilder::new();
                    UnaryExecutor::execute(array, |f| f.is_nan(), &mut builder)?;
                    Array::Boolean(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        is_nan_f64_impl
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ceil;

impl GenericScalarFunction for Ceil {
    fn name(&self) -> &str {
        "ceil"
    }

    fn aliases(&self) -> &[&str] {
        &["ceiling"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Float32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(CeilFloat32)),
            DataType::Float64 => Ok(Box::new(CeilFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(CeilFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(CeilFloat64, Float64, Float64, |f| f.ceil());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Floor;

impl GenericScalarFunction for Floor {
    fn name(&self) -> &str {
        "floor"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float32]),
                return_type: ReturnType::Static(DataType::Float32),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Float32 => Ok(Box::new(FloorFloat32)),
            DataType::Float64 => Ok(Box::new(FloorFloat64)),
            other => Err(specialize_invalid_input_type(self, &[other])),
        }
    }
}

generate_specialized_unary_numeric!(FloorFloat32, Float32, Float32, |f| f.ceil());
generate_specialized_unary_numeric!(FloorFloat64, Float64, Float64, |f| f.ceil());

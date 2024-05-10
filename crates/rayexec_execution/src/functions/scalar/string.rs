use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, ScalarFn,
    SpecializedScalarFunction,
};
use crate::functions::{InputTypes, ReturnType, Signature};
use rayexec_bullet::array::VarlenArrayBuilder;
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repeat;

impl GenericScalarFunction for Repeat {
    fn name(&self) -> &str {
        "repeat"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Utf8, DataType::Int64]),
                return_type: ReturnType::Static(DataType::Utf8),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::LargeUtf8, DataType::Int64]),
                return_type: ReturnType::Static(DataType::LargeUtf8),
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Utf8, DataType::Int64) => Ok(Box::new(RepeatUtf8)),
            (DataType::LargeUtf8, DataType::Int64) => Ok(Box::new(RepeatLargeUtf8)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepeatUtf8;

impl SpecializedScalarFunction for RepeatUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_utf8(arrays: &[&Arc<Array>]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings.as_ref(), nums.as_ref()) {
                (Array::Utf8(strings), Array::Int64(nums)) => {
                    let mut builder = VarlenArrayBuilder::new();
                    BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut builder,
                    )?;
                    Array::Utf8(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_utf8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepeatLargeUtf8;

impl SpecializedScalarFunction for RepeatLargeUtf8 {
    fn function_impl(&self) -> ScalarFn {
        fn repeat_large_utf8(arrays: &[&Arc<Array>]) -> Result<Array> {
            let strings = arrays[0];
            let nums = arrays[1];
            Ok(match (strings.as_ref(), nums.as_ref()) {
                (Array::LargeUtf8(strings), Array::Int64(nums)) => {
                    let mut builder = VarlenArrayBuilder::new();
                    BinaryExecutor::execute(
                        strings,
                        nums,
                        |s, count| s.repeat(count as usize),
                        &mut builder,
                    )?;
                    Array::LargeUtf8(builder.into_typed_array())
                }
                other => panic!("unexpected array type: {other:?}"),
            })
        }

        repeat_large_utf8
    }
}

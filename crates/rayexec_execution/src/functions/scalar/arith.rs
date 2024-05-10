use crate::functions::{InputTypes, ReturnType, Signature};

use super::{
    specialize_check_num_args, specialize_invalid_input_type, GenericScalarFunction, ScalarFn,
    SpecializedScalarFunction,
};
use rayexec_bullet::array::PrimitiveArrayBuilder;
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

/// Signatures for primitive arith operations (+, -, /, *, %)
const PRIMITIVE_ARITH_SIGNATURES: &'static [Signature] = &[
    Signature {
        input: InputTypes::Exact(&[DataType::Float32, DataType::Float32]),
        return_type: ReturnType::Static(DataType::Float32),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::Float64, DataType::Float64]),
        return_type: ReturnType::Static(DataType::Float64),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::Int8, DataType::Int8]),
        return_type: ReturnType::Static(DataType::Int8),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::Int16, DataType::Int16]),
        return_type: ReturnType::Static(DataType::Int16),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::Int32, DataType::Int32]),
        return_type: ReturnType::Static(DataType::Int32),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::Int64, DataType::Int64]),
        return_type: ReturnType::Static(DataType::Int64),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::UInt8, DataType::UInt8]),
        return_type: ReturnType::Static(DataType::UInt8),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::UInt16, DataType::UInt16]),
        return_type: ReturnType::Static(DataType::UInt16),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::UInt32, DataType::UInt32]),
        return_type: ReturnType::Static(DataType::UInt32),
    },
    Signature {
        input: InputTypes::Exact(&[DataType::UInt64, DataType::UInt64]),
        return_type: ReturnType::Static(DataType::UInt64),
    },
];

/// Macro for generating a specialized binary function that accepts two
/// primitive arrays, and produces a single primitive array.
///
/// The operation should accept two inputs, producing a single output of the
/// expected type.
macro_rules! generate_specialized_binary_numeric {
    ($name:ident, $first_variant:ident, $second_variant:ident, $output_variant:ident, $operation:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl SpecializedScalarFunction for $name {
            fn function_impl(&self) -> ScalarFn {
                fn inner(arrays: &[&Arc<Array>]) -> Result<Array> {
                    let first = arrays[0];
                    let second = arrays[1];
                    Ok(match (first.as_ref(), second.as_ref()) {
                        (Array::$first_variant(first), Array::$second_variant(second)) => {
                            let mut builder = PrimitiveArrayBuilder::with_capacity(first.len());
                            BinaryExecutor::execute(first, second, $operation, &mut builder)?;
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
pub struct Add;

impl GenericScalarFunction for Add {
    fn name(&self) -> &str {
        "+"
    }

    fn aliases(&self) -> &[&str] {
        &["add"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(AddFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(AddFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(AddInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(AddInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(AddInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(AddInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(AddUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(AddUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(AddUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(AddUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(AddFloat32, Float32, Float32, Float32, |a, b| a + b);
generate_specialized_binary_numeric!(AddFloat64, Float64, Float64, Float64, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt8, Int8, Int8, Int8, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt16, Int16, Int16, Int16, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt32, Int32, Int32, Int32, |a, b| a + b);
generate_specialized_binary_numeric!(AddInt64, Int64, Int64, Int64, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt8, UInt8, UInt8, UInt8, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt16, UInt16, UInt16, UInt16, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt32, UInt32, UInt32, UInt32, |a, b| a + b);
generate_specialized_binary_numeric!(AddUInt64, UInt64, UInt64, UInt64, |a, b| a + b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub;

impl GenericScalarFunction for Sub {
    fn name(&self) -> &str {
        "-"
    }

    fn aliases(&self) -> &[&str] {
        &["sub"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(SubFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(SubFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(SubInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(SubInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(SubInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(SubInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(SubUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(SubUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(SubUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(SubUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(SubFloat32, Float32, Float32, Float32, |a, b| a - b);
generate_specialized_binary_numeric!(SubFloat64, Float64, Float64, Float64, |a, b| a - b);
generate_specialized_binary_numeric!(SubInt8, Int8, Int8, Int8, |a, b| a - b);
generate_specialized_binary_numeric!(SubInt16, Int16, Int16, Int16, |a, b| a - b);
generate_specialized_binary_numeric!(SubInt32, Int32, Int32, Int32, |a, b| a - b);
generate_specialized_binary_numeric!(SubInt64, Int64, Int64, Int64, |a, b| a - b);
generate_specialized_binary_numeric!(SubUInt8, UInt8, UInt8, UInt8, |a, b| a - b);
generate_specialized_binary_numeric!(SubUInt16, UInt16, UInt16, UInt16, |a, b| a - b);
generate_specialized_binary_numeric!(SubUInt32, UInt32, UInt32, UInt32, |a, b| a - b);
generate_specialized_binary_numeric!(SubUInt64, UInt64, UInt64, UInt64, |a, b| a - b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Div;

impl GenericScalarFunction for Div {
    fn name(&self) -> &str {
        "/"
    }

    fn aliases(&self) -> &[&str] {
        &["div"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(DivFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(DivFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(DivInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(DivInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(DivInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(DivInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(DivUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(DivUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(DivUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(DivUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(DivFloat32, Float32, Float32, Float32, |a, b| a / b);
generate_specialized_binary_numeric!(DivFloat64, Float64, Float64, Float64, |a, b| a / b);
generate_specialized_binary_numeric!(DivInt8, Int8, Int8, Int8, |a, b| a / b);
generate_specialized_binary_numeric!(DivInt16, Int16, Int16, Int16, |a, b| a / b);
generate_specialized_binary_numeric!(DivInt32, Int32, Int32, Int32, |a, b| a / b);
generate_specialized_binary_numeric!(DivInt64, Int64, Int64, Int64, |a, b| a / b);
generate_specialized_binary_numeric!(DivUInt8, UInt8, UInt8, UInt8, |a, b| a / b);
generate_specialized_binary_numeric!(DivUInt16, UInt16, UInt16, UInt16, |a, b| a / b);
generate_specialized_binary_numeric!(DivUInt32, UInt32, UInt32, UInt32, |a, b| a / b);
generate_specialized_binary_numeric!(DivUInt64, UInt64, UInt64, UInt64, |a, b| a / b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mul;

impl GenericScalarFunction for Mul {
    fn name(&self) -> &str {
        "*"
    }

    fn aliases(&self) -> &[&str] {
        &["mul"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(MulFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(MulFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(MulInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(MulInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(MulInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(MulInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(MulUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(MulUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(MulUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(MulUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(MulFloat32, Float32, Float32, Float32, |a, b| a * b);
generate_specialized_binary_numeric!(MulFloat64, Float64, Float64, Float64, |a, b| a * b);
generate_specialized_binary_numeric!(MulInt8, Int8, Int8, Int8, |a, b| a * b);
generate_specialized_binary_numeric!(MulInt16, Int16, Int16, Int16, |a, b| a * b);
generate_specialized_binary_numeric!(MulInt32, Int32, Int32, Int32, |a, b| a * b);
generate_specialized_binary_numeric!(MulInt64, Int64, Int64, Int64, |a, b| a * b);
generate_specialized_binary_numeric!(MulUInt8, UInt8, UInt8, UInt8, |a, b| a * b);
generate_specialized_binary_numeric!(MulUInt16, UInt16, UInt16, UInt16, |a, b| a * b);
generate_specialized_binary_numeric!(MulUInt32, UInt32, UInt32, UInt32, |a, b| a * b);
generate_specialized_binary_numeric!(MulUInt64, UInt64, UInt64, UInt64, |a, b| a * b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rem;

impl GenericScalarFunction for Rem {
    fn name(&self) -> &str {
        "%"
    }

    fn aliases(&self) -> &[&str] {
        &["rem", "mod"]
    }

    fn signatures(&self) -> &[Signature] {
        PRIMITIVE_ARITH_SIGNATURES
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float32, DataType::Float32) => Ok(Box::new(RemFloat32)),
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RemFloat64)),
            (DataType::Int8, DataType::Int8) => Ok(Box::new(RemInt8)),
            (DataType::Int16, DataType::Int16) => Ok(Box::new(RemInt16)),
            (DataType::Int32, DataType::Int32) => Ok(Box::new(RemInt32)),
            (DataType::Int64, DataType::Int64) => Ok(Box::new(RemInt64)),
            (DataType::UInt8, DataType::UInt8) => Ok(Box::new(RemUInt8)),
            (DataType::UInt16, DataType::UInt16) => Ok(Box::new(RemUInt16)),
            (DataType::UInt32, DataType::UInt32) => Ok(Box::new(RemUInt32)),
            (DataType::UInt64, DataType::UInt64) => Ok(Box::new(RemUInt64)),
            (a, b) => Err(specialize_invalid_input_type(self, &[a, b])),
        }
    }
}

generate_specialized_binary_numeric!(RemFloat32, Float32, Float32, Float32, |a, b| a % b);
generate_specialized_binary_numeric!(RemFloat64, Float64, Float64, Float64, |a, b| a % b);
generate_specialized_binary_numeric!(RemInt8, Int8, Int8, Int8, |a, b| a % b);
generate_specialized_binary_numeric!(RemInt16, Int16, Int16, Int16, |a, b| a % b);
generate_specialized_binary_numeric!(RemInt32, Int32, Int32, Int32, |a, b| a % b);
generate_specialized_binary_numeric!(RemInt64, Int64, Int64, Int64, |a, b| a % b);
generate_specialized_binary_numeric!(RemUInt8, UInt8, UInt8, UInt8, |a, b| a % b);
generate_specialized_binary_numeric!(RemUInt16, UInt16, UInt16, UInt16, |a, b| a % b);
generate_specialized_binary_numeric!(RemUInt32, UInt32, UInt32, UInt32, |a, b| a % b);
generate_specialized_binary_numeric!(RemUInt64, UInt64, UInt64, UInt64, |a, b| a % b);

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Int32Array;

    use super::*;

    #[test]
    fn add_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));

        let specialized = Add.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([5, 7, 9]));

        assert_eq!(expected, out);
    }

    #[test]
    fn sub_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Sub.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([3, 3, 3]));

        assert_eq!(expected, out);
    }

    #[test]
    fn div_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Div.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([4, 2, 2]));

        assert_eq!(expected, out);
    }

    #[test]
    fn rem_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Rem.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([0, 1, 0]));

        assert_eq!(expected, out);
    }

    #[test]
    fn mul_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([4, 5, 6])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));

        let specialized = Mul.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = (specialized.function_impl())(&[&a, &b]).unwrap();
        let expected = Array::Int32(Int32Array::from_iter([4, 10, 18]));

        assert_eq!(expected, out);
    }
}

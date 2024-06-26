use super::{GenericScalarFunction, SpecializedScalarFunction};
use crate::functions::scalar::macros::cmp_binary_execute;
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};
use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

// TODOs:
//
// - Normalize scales for decimals for comparisons (will be needed elsewhere too).
// - Normalize intervals for comparisons

const COMPARISON_SIGNATURES: &[Signature] = &[
    Signature {
        input: &[DataTypeId::Boolean, DataTypeId::Boolean],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int8, DataTypeId::Int8],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int16, DataTypeId::Int16],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int32, DataTypeId::Int32],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Int64, DataTypeId::Int64],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt8, DataTypeId::UInt8],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt16, DataTypeId::UInt16],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt32, DataTypeId::UInt32],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::UInt64, DataTypeId::UInt64],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float32, DataTypeId::Float32],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Float64, DataTypeId::Float64],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal64, DataTypeId::Decimal64],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Decimal128, DataTypeId::Decimal128],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::TimestampSeconds, DataTypeId::TimestampSeconds],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[
            DataTypeId::TimestampMilliseconds,
            DataTypeId::TimestampMilliseconds,
        ],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[
            DataTypeId::TimestampMicroseconds,
            DataTypeId::TimestampMicroseconds,
        ],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[
            DataTypeId::TimestampNanoseconds,
            DataTypeId::TimestampNanoseconds,
        ],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Date32, DataTypeId::Date32],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Utf8, DataTypeId::Utf8],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::LargeUtf8, DataTypeId::LargeUtf8],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::Binary, DataTypeId::Binary],
        return_type: DataTypeId::Boolean,
    },
    Signature {
        input: &[DataTypeId::LargeBinary, DataTypeId::LargeBinary],
        return_type: DataTypeId::Boolean,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eq;

impl FunctionInfo for Eq {
    fn name(&self) -> &'static str {
        "="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Eq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(EqSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EqSpecialized;

impl SpecializedScalarFunction for EqSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a == b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a == b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a == b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neq;

impl FunctionInfo for Neq {
    fn name(&self) -> &'static str {
        "<>"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["!="]
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Neq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(NeqSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeqSpecialized;

impl SpecializedScalarFunction for NeqSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a != b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a != b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a != b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lt;

impl FunctionInfo for Lt {
    fn name(&self) -> &'static str {
        "<"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Lt {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtSpecialized;

impl SpecializedScalarFunction for LtSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| !a & b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a < b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a < b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a < b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEq;

impl FunctionInfo for LtEq {
    fn name(&self) -> &'static str {
        "<="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for LtEq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(LtEqSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LtEqSpecialized;

impl SpecializedScalarFunction for LtEqSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a <= b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a <= b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a <= b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gt;

impl FunctionInfo for Gt {
    fn name(&self) -> &'static str {
        ">"
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for Gt {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtSpecialized;

impl SpecializedScalarFunction for GtSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| a & !b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a > b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a > b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a > b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEq;

impl FunctionInfo for GtEq {
    fn name(&self) -> &'static str {
        ">="
    }

    fn signatures(&self) -> &[Signature] {
        COMPARISON_SIGNATURES
    }
}

impl GenericScalarFunction for GtEq {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        specialize_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Int8, DataType::Int8)
            | (DataType::Int16, DataType::Int16)
            | (DataType::Int32, DataType::Int32)
            | (DataType::Int64, DataType::Int64)
            | (DataType::UInt8, DataType::UInt8)
            | (DataType::UInt16, DataType::UInt16)
            | (DataType::UInt32, DataType::UInt32)
            | (DataType::UInt64, DataType::UInt64)
            | (DataType::Float32, DataType::Float32)
            | (DataType::Float64, DataType::Float64)
            | (DataType::Decimal64(_), DataType::Decimal64(_))
            | (DataType::Decimal128(_), DataType::Decimal128(_))
            | (DataType::TimestampSeconds, DataType::TimestampSeconds)
            | (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds)
            | (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds)
            | (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds)
            | (DataType::Date32, DataType::Date32)
            | (DataType::Date64, DataType::Date64)
            | (DataType::Utf8, DataType::Utf8)
            | (DataType::LargeUtf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::Binary)
            | (DataType::LargeBinary, DataType::LargeBinary) => Ok(Box::new(GtEqSpecialized)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtEqSpecialized;

impl SpecializedScalarFunction for GtEqSpecialized {
    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Int8(first), Array::Int8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Int16(first), Array::Int16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Int32(first), Array::Int32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Int64(first), Array::Int64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::UInt8(first), Array::UInt8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::UInt16(first), Array::UInt16(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::UInt32(first), Array::UInt32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::UInt64(first), Array::UInt64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Float32(first), Array::Float32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Float64(first), Array::Float64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Decimal64(first), Array::Decimal64(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a >= b)
            }
            (Array::Decimal128(first), Array::Decimal128(second)) => {
                // TODO: Scale check
                cmp_binary_execute!(first.get_primitive(), second.get_primitive(), |a, b| a >= b)
            }
            (Array::TimestampSeconds(first), Array::TimestampSeconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::TimestampMilliseconds(first), Array::TimestampMilliseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::TimestampMicroseconds(first), Array::TimestampMicroseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::TimestampNanoseconds(first), Array::TimestampNanoseconds(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Date32(first), Array::Date32(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Date64(first), Array::Date64(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Utf8(first), Array::Utf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::LargeUtf8(first), Array::LargeUtf8(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::Binary(first), Array::Binary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            (Array::LargeBinary(first), Array::LargeBinary(second)) => {
                cmp_binary_execute!(first, second, |a, b| a >= b)
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{BooleanArray, Int32Array};

    use super::*;

    #[test]
    fn eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Eq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn neq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Neq.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Lt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn lt_eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = LtEq
            .specialize(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, true, true]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = Gt.specialize(&[DataType::Int32, DataType::Int32]).unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn gt_eq_i32() {
        let a = Arc::new(Array::Int32(Int32Array::from_iter([1, 2, 3])));
        let b = Arc::new(Array::Int32(Int32Array::from_iter([2, 2, 6])));

        let specialized = GtEq
            .specialize(&[DataType::Int32, DataType::Int32])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([false, true, false]));

        assert_eq!(expected, out);
    }
}

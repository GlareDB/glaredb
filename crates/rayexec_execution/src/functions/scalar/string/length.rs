use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalBinary, PhysicalUtf8};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Length;

impl FunctionInfo for Length {
    fn name(&self) -> &'static str {
        "length"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["char_length", "character_length"]
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Int64,
        }]
    }
}

impl ScalarFunction for Length {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(StrLengthImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 => Ok(Box::new(StrLengthImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrLengthImpl;

impl PlannedScalarFunction for StrLengthImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Length
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let builder = ArrayBuilder {
            datatype: DataType::Int64,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };

        UnaryExecutor::execute::<PhysicalUtf8, _, _>(input, builder, |v, buf| {
            let len = v.chars().count() as i64;
            buf.put(&len)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteLength;

impl FunctionInfo for ByteLength {
    fn name(&self) -> &'static str {
        "byte_length"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["octet_length"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::Binary],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
        ]
    }
}

impl ScalarFunction for ByteLength {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(ByteLengthImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 | DataType::Binary => Ok(Box::new(ByteLengthImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteLengthImpl;

impl PlannedScalarFunction for ByteLengthImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ByteLength
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let builder = ArrayBuilder {
            datatype: DataType::Int64,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };

        // Binary applicable to both str and [u8].
        UnaryExecutor::execute::<PhysicalBinary, _, _>(input, builder, |v, buf| {
            buf.put(&(v.len() as i64))
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitLength;

impl FunctionInfo for BitLength {
    fn name(&self) -> &'static str {
        "bit_length"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
            Signature {
                input: &[DataTypeId::Binary],
                variadic: None,
                return_type: DataTypeId::Int64,
            },
        ]
    }
}

impl ScalarFunction for BitLength {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(BitLengthImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 | DataType::Binary => Ok(Box::new(BitLengthImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitLengthImpl;

impl PlannedScalarFunction for BitLengthImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &BitLength
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let builder = ArrayBuilder {
            datatype: DataType::Int64,
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };

        // Binary applicable to both str and [u8].
        UnaryExecutor::execute::<PhysicalBinary, _, _>(input, builder, |v, buf| {
            let bit_len = v.len() * 8;
            buf.put(&(bit_len as i64))
        })
    }
}

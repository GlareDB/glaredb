use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ascii;

impl FunctionInfo for Ascii {
    fn name(&self) -> &'static str {
        "ascii"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Int32,
        }]
    }
}

impl ScalarFunction for Ascii {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(AsciiImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 => Ok(Box::new(AsciiImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsciiImpl;

impl PlannedScalarFunction2 for AsciiImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Ascii
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int32
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        let builder = ArrayBuilder {
            datatype: DataType::Int32,
            buffer: PrimitiveBuffer::with_len(inputs[0].logical_len()),
        };

        UnaryExecutor::execute::<PhysicalUtf8, _, _>(input, builder, |v, buf| {
            let v = v.chars().next().map(|c| c as i32).unwrap_or(0);
            buf.put(&v)
        })
    }
}

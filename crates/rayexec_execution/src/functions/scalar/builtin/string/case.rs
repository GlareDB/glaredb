use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::{RayexecError, Result};

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lower;

impl FunctionInfo for Lower {
    fn name(&self) -> &'static str {
        "lower"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Lower {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(LowerImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 => Ok(Box::new(LowerImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LowerImpl;

impl PlannedScalarFunction for LowerImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Lower
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        case_convert_execute(input, str::to_lowercase)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Upper;

impl FunctionInfo for Upper {
    fn name(&self) -> &'static str {
        "upper"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl ScalarFunction for Upper {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(UpperImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Utf8 => Ok(Box::new(UpperImpl)),
            a => Err(invalid_input_types_error(self, &[a])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpperImpl;

impl PlannedScalarFunction for UpperImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Upper
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        case_convert_execute(input, str::to_uppercase)
    }
}

fn case_convert_execute<F>(input: &Array, case_fn: F) -> Result<Array>
where
    F: Fn(&str) -> String,
{
    let cap = match input.array_data() {
        ArrayData::Binary(bin) => bin.binary_data_size_bytes(),
        _ => return Err(RayexecError::new("Unexpected array data type")),
    };

    let builder = ArrayBuilder {
        datatype: DataType::Utf8,
        buffer: GermanVarlenBuffer::<str>::with_len_and_data_capacity(input.logical_len(), cap),
    };

    UnaryExecutor::execute::<PhysicalUtf8, _, _>(input, builder, |v, buf| {
        // TODO: Non-allocating variant.
        buf.put(&case_fn(v))
    })
}

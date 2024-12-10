use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::storage::PrimitiveStorage;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::functions::scalar::{FunctionVolatility, PlannedScalarFunction2, ScalarFunction};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Random;

impl FunctionInfo for Random {
    fn name(&self) -> &'static str {
        "random"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl ScalarFunction for Random {
    fn volatility(&self) -> FunctionVolatility {
        FunctionVolatility::Volatile
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(RandomImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 0)?;
        Ok(Box::new(RandomImpl))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RandomImpl;

impl PlannedScalarFunction2 for RandomImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Random
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn execute(&self, _inputs: &[&Array]) -> Result<Array> {
        let val = rand::random::<f64>();
        Ok(Array::new_with_array_data(
            DataType::Float64,
            PrimitiveStorage::from(vec![val]),
        ))
    }
}

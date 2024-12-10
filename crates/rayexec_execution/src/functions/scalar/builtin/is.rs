use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalAny, PhysicalBool};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::Result;

use crate::functions::scalar::{PlannedScalarFunction2, ScalarFunction};
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNull;

impl FunctionInfo for IsNull {
    fn name(&self) -> &'static str {
        "is_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNull {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckNullImpl::<true>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckNullImpl::<true>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotNull;

impl FunctionInfo for IsNotNull {
    fn name(&self) -> &'static str {
        "is_not_null"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNotNull {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckNullImpl::<false>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckNullImpl::<false>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckNullImpl<const IS_NULL: bool>;

impl<const IS_NULL: bool> PlannedScalarFunction2 for CheckNullImpl<IS_NULL> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        if IS_NULL {
            &IsNull
        } else {
            &IsNotNull
        }
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let (initial, updated) = if IS_NULL {
            // Executor will only execute on non-null inputs, so we can assume
            // everything is null first then selectively set false for things
            // that the executor executes.
            (true, false)
        } else {
            (false, true)
        };

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len_and_default_value(input.logical_len(), initial),
        };
        let array = UnaryExecutor::execute::<PhysicalAny, _, _>(input, builder, |_, buf| {
            buf.put(&updated)
        })?;

        // Drop validity.
        let data = array.into_array_data();
        Ok(Array::new_with_array_data(DataType::Boolean, data))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsTrue;

impl FunctionInfo for IsTrue {
    fn name(&self) -> &'static str {
        "is_true"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsTrue {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckBoolImpl::<false, true>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckBoolImpl::<false, true>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotTrue;

impl FunctionInfo for IsNotTrue {
    fn name(&self) -> &'static str {
        "is_not_true"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNotTrue {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckBoolImpl::<true, true>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckBoolImpl::<true, true>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsFalse;

impl FunctionInfo for IsFalse {
    fn name(&self) -> &'static str {
        "is_false"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsFalse {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckBoolImpl::<false, false>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckBoolImpl::<false, false>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IsNotFalse;

impl FunctionInfo for IsNotFalse {
    fn name(&self) -> &'static str {
        "is_not_false"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for IsNotFalse {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(CheckBoolImpl::<true, false>))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(CheckBoolImpl::<true, false>))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckBoolImpl<const NOT: bool, const BOOL: bool>;

impl<const NOT: bool, const BOOL: bool> PlannedScalarFunction2 for CheckBoolImpl<NOT, BOOL> {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        match (NOT, BOOL) {
            (false, true) => &IsTrue,
            (true, true) => &IsNotTrue,
            (false, false) => &IsFalse,
            (true, false) => &IsNotFalse,
        }
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];

        let initial = NOT;

        let builder = ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len_and_default_value(input.logical_len(), initial),
        };
        let array = UnaryExecutor::execute::<PhysicalBool, _, _>(input, builder, |val, buf| {
            let b = if NOT { val != BOOL } else { val == BOOL };
            buf.put(&b)
        })?;

        // Drop validity.
        let data = array.into_array_data();
        Ok(Array::new_with_array_data(DataType::Boolean, data))
    }
}

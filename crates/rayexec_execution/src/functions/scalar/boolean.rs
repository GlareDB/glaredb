use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::array::Array;
use rayexec_bullet::array::{BooleanArray, BooleanValuesBuffer};
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::scalar::BinaryExecutor;
use rayexec_error::Result;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct And;

impl FunctionInfo for And {
    fn name(&self) -> &'static str {
        "and"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean, DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for And {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean) => Ok(Box::new(AndImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AndImpl;

impl PlannedScalarFunction for AndImpl {
    fn name(&self) -> &'static str {
        "and_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                let validity = BinaryExecutor::execute(first, second, |a, b| a && b, &mut buffer)?;
                Array::Boolean(BooleanArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Or;

impl FunctionInfo for Or {
    fn name(&self) -> &'static str {
        "or"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Boolean, DataTypeId::Boolean],
            variadic: None,
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for Or {
    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Boolean, DataType::Boolean) => Ok(Box::new(OrImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrImpl;

impl PlannedScalarFunction for OrImpl {
    fn name(&self) -> &'static str {
        "or_impl"
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, arrays: &[&Arc<Array>]) -> Result<Array> {
        let first = arrays[0];
        let second = arrays[1];
        Ok(match (first.as_ref(), second.as_ref()) {
            (Array::Boolean(first), Array::Boolean(second)) => {
                let mut buffer = BooleanValuesBuffer::with_capacity(first.len());
                let validity = BinaryExecutor::execute(first, second, |a, b| a || b, &mut buffer)?;
                Array::Boolean(BooleanArray::new(buffer, validity))
            }
            other => panic!("unexpected array type: {other:?}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::BooleanArray;

    use super::*;

    #[test]
    fn and_bool() {
        let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

        let specialized = And
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, false, false]));

        assert_eq!(expected, out);
    }

    #[test]
    fn or_bool() {
        let a = Arc::new(Array::Boolean(BooleanArray::from_iter([
            true, false, false,
        ])));
        let b = Arc::new(Array::Boolean(BooleanArray::from_iter([true, true, false])));

        let specialized = Or
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();
        let expected = Array::Boolean(BooleanArray::from_iter([true, true, false]));

        assert_eq!(expected, out);
    }
}

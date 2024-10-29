use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayBuilder, BooleanBuffer};
use rayexec_bullet::executor::physical_type::PhysicalBool;
use rayexec_bullet::executor::scalar::{BinaryExecutor, TernaryExecutor, UniformExecutor};
use rayexec_bullet::storage::BooleanStorage;
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct And;

impl FunctionInfo for And {
    fn name(&self) -> &'static str {
        "and"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for And {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(AndImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Boolean {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(AndImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AndImpl;

impl PlannedScalarFunction for AndImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &And
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::new_with_array_data(
                    DataType::Boolean,
                    BooleanStorage::from(Bitmap::new_with_val(false, 1)),
                );
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];
                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a && b)),
                )
            }
            3 => {
                let a = inputs[0];
                let b = inputs[1];
                let c = inputs[2];
                TernaryExecutor::execute::<PhysicalBool, PhysicalBool, PhysicalBool, _, _>(
                    a,
                    b,
                    c,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(a.logical_len()),
                    },
                    |a, b, c, buf| buf.put(&(a && b && c)),
                )
            }
            _ => {
                let len = inputs[0].logical_len();
                UniformExecutor::execute::<PhysicalBool, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(len),
                    },
                    |bools, buf| buf.put(&(bools.iter().all(|b| *b))),
                )
            }
        }
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
            input: &[],
            variadic: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
        }]
    }
}

impl ScalarFunction for Or {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(OrImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        for input in inputs {
            if input.datatype_id() != DataTypeId::Boolean {
                return Err(invalid_input_types_error(self, inputs));
            }
        }

        Ok(Box::new(OrImpl))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrImpl;

impl PlannedScalarFunction for OrImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &Or
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        match inputs.len() {
            0 => {
                let mut array = Array::new_with_array_data(
                    DataType::Boolean,
                    BooleanStorage::from(Bitmap::new_with_val(false, 1)),
                );
                array.set_physical_validity(0, false);
                Ok(array)
            }
            1 => Ok(inputs[0].clone()),
            2 => {
                let a = inputs[0];
                let b = inputs[1];
                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, _, _>(
                    a,
                    b,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(a.logical_len()),
                    },
                    |a, b, buf| buf.put(&(a || b)),
                )
            }
            _ => {
                let len = inputs[0].logical_len();
                UniformExecutor::execute::<PhysicalBool, _, _>(
                    inputs,
                    ArrayBuilder {
                        datatype: DataType::Boolean,
                        buffer: BooleanBuffer::with_len(len),
                    },
                    |bools, buf| buf.put(&(bools.iter().any(|b| *b))),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use super::*;

    #[test]
    fn and_bool_2() {
        let a = Array::from_iter([true, false, false]);
        let b = Array::from_iter([true, true, false]);

        let specialized = And
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();

        assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }

    #[test]
    fn and_bool_3() {
        let a = Array::from_iter([true, true, true]);
        let b = Array::from_iter([false, true, true]);
        let c = Array::from_iter([true, true, false]);

        let specialized = And
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute(&[&a, &b, &c]).unwrap();

        assert_eq!(ScalarValue::from(false), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }

    #[test]
    fn or_bool_2() {
        let a = Array::from_iter([true, false, false]);
        let b = Array::from_iter([true, true, false]);

        let specialized = Or
            .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
            .unwrap();

        let out = specialized.execute(&[&a, &b]).unwrap();

        assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }
}

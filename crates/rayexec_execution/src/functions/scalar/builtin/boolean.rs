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

use crate::expr::Expression;
use crate::functions::scalar::{
    PlannedScalarFunction2,
    PlannedScalarFuntion,
    ScalarFunction,
    ScalarFunctionImpl,
};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;

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
    fn plan(
        &self,
        bind_context: &BindContext,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        if !datatypes.iter().all(|dt| dt == &DataType::Boolean) {
            return Err(invalid_input_types_error(self, &datatypes));
        }

        Ok(PlannedScalarFuntion {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(AndImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct AndImpl;

impl ScalarFunctionImpl for AndImpl {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AndImpl2;

impl PlannedScalarFunction2 for AndImpl2 {
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
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction2>> {
        Ok(Box::new(OrImpl))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction2>> {
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

impl PlannedScalarFunction2 for OrImpl {
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
    // TODO
    // use rayexec_bullet::scalar::ScalarValue;

    // use super::*;

    // #[test]
    // fn and_bool_2() {
    //     let a = Array::from_iter([true, false, false]);
    //     let b = Array::from_iter([true, true, false]);

    //     let specialized = And
    //         .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();

    //     assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
    //     assert_eq!(ScalarValue::from(false), out.logical_value(1).unwrap());
    //     assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    // }

    // #[test]
    // fn and_bool_3() {
    //     let a = Array::from_iter([true, true, true]);
    //     let b = Array::from_iter([false, true, true]);
    //     let c = Array::from_iter([true, true, false]);

    //     let specialized = And
    //         .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b, &c]).unwrap();

    //     assert_eq!(ScalarValue::from(false), out.logical_value(0).unwrap());
    //     assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
    //     assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    // }

    // #[test]
    // fn or_bool_2() {
    //     let a = Array::from_iter([true, false, false]);
    //     let b = Array::from_iter([true, true, false]);

    //     let specialized = Or
    //         .plan_from_datatypes(&[DataType::Boolean, DataType::Boolean])
    //         .unwrap();

    //     let out = specialized.execute(&[&a, &b]).unwrap();

    //     assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
    //     assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
    //     assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    // }
}

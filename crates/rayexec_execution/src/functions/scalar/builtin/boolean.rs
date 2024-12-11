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
    PlannedScalarFunction,
    PlannedScalarFunction2,
    ScalarFunction,
    ScalarFunctionImpl,
};
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

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
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        if !datatypes.iter().all(|dt| dt == &DataType::Boolean) {
            return Err(invalid_input_types_error(self, &datatypes));
        }

        Ok(PlannedScalarFunction {
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
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|input| input.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        if !datatypes.iter().all(|dt| dt == &DataType::Boolean) {
            return Err(invalid_input_types_error(self, &datatypes));
        }

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: DataType::Boolean,
            inputs,
            function_impl: Box::new(OrImpl),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrImpl;

impl ScalarFunctionImpl for OrImpl {
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
    use crate::expr;

    #[test]
    fn and_bool_2() {
        let a = Array::from_iter([true, false, false]);
        let b = Array::from_iter([true, true, false]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Boolean, DataType::Boolean],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = And
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();

        assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }

    #[test]
    fn and_bool_3() {
        let a = Array::from_iter([true, true, true]);
        let b = Array::from_iter([false, true, true]);
        let c = Array::from_iter([true, true, false]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Boolean, DataType::Boolean, DataType::Boolean],
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
            )
            .unwrap();

        let planned = And
            .plan(
                &table_list,
                vec![
                    expr::col_ref(table_ref, 0),
                    expr::col_ref(table_ref, 1),
                    expr::col_ref(table_ref, 2),
                ],
            )
            .unwrap();

        let out = planned.function_impl.execute(&[&a, &b, &c]).unwrap();

        assert_eq!(ScalarValue::from(false), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }

    #[test]
    fn or_bool_2() {
        let a = Array::from_iter([true, false, false]);
        let b = Array::from_iter([true, true, false]);

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Boolean, DataType::Boolean],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = Or
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let out = planned.function_impl.execute(&[&a, &b]).unwrap();

        assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }
}

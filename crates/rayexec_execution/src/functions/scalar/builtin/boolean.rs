use std::fmt::Debug;

use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::arrays::array::physical_type::PhysicalBool;
use crate::arrays::array::Array;
use crate::arrays::bitmap::Bitmap;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::builder::{ArrayBuilder, BooleanBuffer};
use crate::arrays::executor::scalar::{BinaryExecutor, TernaryExecutor, UniformExecutor};
use crate::arrays::storage::BooleanStorage;
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
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
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Boolean and all inputs.",
                arguments: &["var_args"],
                example: Some(Example {
                    example: "and(true, false, true)",
                    output: "false",
                }),
            }),
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
    fn execute2(&self, inputs: &[&Array]) -> Result<Array> {
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
                BinaryExecutor::execute2::<PhysicalBool, PhysicalBool, _, _>(
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
                TernaryExecutor::execute2::<PhysicalBool, PhysicalBool, PhysicalBool, _, _>(
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
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Boolean),
            return_type: DataTypeId::Boolean,
            doc: Some(&Documentation {
                category: Category::General,
                description: "Boolean or all inputs.",
                arguments: &["var_args"],
                example: Some(Example {
                    example: "or(true, false, true)",
                    output: "true",
                }),
            }),
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
    fn execute2(&self, inputs: &[&Array]) -> Result<Array> {
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
                BinaryExecutor::execute2::<PhysicalBool, PhysicalBool, _, _>(
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
    use super::*;
    use crate::arrays::scalar::ScalarValue;
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

        let out = planned.function_impl.execute2(&[&a, &b]).unwrap();

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

        let out = planned.function_impl.execute2(&[&a, &b, &c]).unwrap();

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

        let out = planned.function_impl.execute2(&[&a, &b]).unwrap();

        assert_eq!(ScalarValue::from(true), out.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(true), out.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(false), out.logical_value(2).unwrap());
    }
}

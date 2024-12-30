use std::fmt::Debug;

use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use crate::arrays::array::exp::Array;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::physical_type::PhysicalBool;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor_exp::scalar::binary::BinaryExecutor;
use crate::arrays::executor_exp::scalar::unary::UnaryExecutor;
use crate::arrays::executor_exp::scalar::uniform::UniformExecutor;
use crate::arrays::executor_exp::OutBuffer;
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Default to false?
                let vals = output
                    .data_mut()
                    .try_as_mut()?
                    .try_as_slice_mut::<PhysicalBool>()?;

                for v in vals {
                    *v = false;
                }
            }
            1 => {
                let input = &input.arrays()[0];
                UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                    input,
                    sel,
                    OutBuffer::from_array(output)?,
                    |v, buf| buf.put(v),
                )?;
            }
            2 => {
                let a = &input.arrays()[0];
                let b = &input.arrays()[1];

                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, PhysicalBool, _>(
                    a,
                    sel,
                    b,
                    sel,
                    OutBuffer::from_array(output)?,
                    |&a, &b, buf| buf.put(&(a && b)),
                )?;
            }
            _ => UniformExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                input.arrays(),
                sel,
                OutBuffer::from_array(output)?,
                |bools, buf| buf.put(&(bools.iter().all(|b| **b))),
            )?,
        }

        Ok(())
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
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();

        match input.arrays().len() {
            0 => {
                // TODO: Default to false?
                let vals = output
                    .data_mut()
                    .try_as_mut()?
                    .try_as_slice_mut::<PhysicalBool>()?;

                for v in vals {
                    *v = false;
                }
            }
            1 => {
                let input = &input.arrays()[0];
                UnaryExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                    input,
                    sel,
                    OutBuffer::from_array(output)?,
                    |v, buf| buf.put(v),
                )?;
            }
            2 => {
                let a = &input.arrays()[0];
                let b = &input.arrays()[1];

                BinaryExecutor::execute::<PhysicalBool, PhysicalBool, PhysicalBool, _>(
                    a,
                    sel,
                    b,
                    sel,
                    OutBuffer::from_array(output)?,
                    |&a, &b, buf| buf.put(&(a || b)),
                )?;
            }
            _ => UniformExecutor::execute::<PhysicalBool, PhysicalBool, _>(
                input.arrays(),
                sel,
                OutBuffer::from_array(output)?,
                |bools, buf| buf.put(&(bools.iter().any(|b| **b))),
            )?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iterutil::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::buffer::buffer_manager::NopBufferManager;
    use crate::arrays::testutil::assert_arrays_eq;
    use crate::expr;

    #[test]
    fn and_bool_2() {
        let a = Array::try_from_iter([true, false, false]).unwrap();
        let b = Array::try_from_iter([true, true, false]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

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

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([true, false, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn and_bool_3() {
        let a = Array::try_from_iter([true, true, true]).unwrap();
        let b = Array::try_from_iter([false, true, true]).unwrap();
        let c = Array::try_from_iter([true, true, false]).unwrap();
        let batch = Batch::from_arrays([a, b, c], true).unwrap();

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

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([false, true, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn or_bool_2() {
        let a = Array::try_from_iter([true, false, false]).unwrap();
        let b = Array::try_from_iter([true, true, false]).unwrap();
        let batch = Batch::from_arrays([a, b], true).unwrap();

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

        let mut out = Array::new(&NopBufferManager, DataType::Boolean, 3).unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        let expected = Array::try_from_iter([true, true, false]).unwrap();

        assert_arrays_eq(&expected, &out);
    }
}

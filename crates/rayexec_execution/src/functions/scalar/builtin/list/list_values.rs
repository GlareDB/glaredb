use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, ListTypeMeta};
use rayexec_bullet::executor::scalar::concat;
use rayexec_bullet::storage::ListStorage;
use rayexec_error::{RayexecError, Result};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFuntion, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListValues;

impl FunctionInfo for ListValues {
    fn name(&self) -> &'static str {
        "list_values"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
        }]
    }
}

impl ScalarFunction for ListValues {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFuntion> {
        let first = match inputs.first() {
            Some(expr) => expr.datatype(table_list)?,
            None => {
                let return_type = DataType::List(ListTypeMeta {
                    datatype: Box::new(DataType::Null),
                });
                return Ok(PlannedScalarFuntion {
                    function: Box::new(*self),
                    return_type: return_type.clone(),
                    inputs,
                    function_impl: Box::new(ListValuesImpl {
                        list_datatype: return_type,
                    }),
                });
            }
        };

        for input in &inputs {
            let dt = input.datatype(table_list)?;
            // TODO: We can add casts here.
            if dt != first {
                return Err(RayexecError::new(format!(
                    "Not all inputs are the same type, got {dt}, expected {first}"
                )));
            }
        }

        let return_type = DataType::List(ListTypeMeta {
            datatype: Box::new(first.clone()),
        });

        return Ok(PlannedScalarFuntion {
            function: Box::new(*self),
            return_type: return_type.clone(),
            inputs,
            function_impl: Box::new(ListValuesImpl {
                list_datatype: return_type,
            }),
        });
    }
}

#[derive(Debug, Clone)]
pub struct ListValuesImpl {
    list_datatype: DataType,
}

impl ScalarFunctionImpl for ListValuesImpl {
    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        if inputs.is_empty() {
            let inner_type = match &self.list_datatype {
                DataType::List(l) => l.datatype.as_ref(),
                other => panic!("invalid data type: {other}"),
            };

            let data = ListStorage::empty_list(Array::new_typed_null_array(inner_type.clone(), 1)?);
            return Ok(Array::new_with_array_data(self.list_datatype.clone(), data));
        }

        let out = concat(inputs)?;
        let data = ListStorage::single_list(out);

        Ok(Array::new_with_array_data(self.list_datatype.clone(), data))
    }
}

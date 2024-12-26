use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::arrays::executor::scalar::concat;
use crate::arrays::storage::ListStorage;
use rayexec_error::{RayexecError, Result};

use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
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
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
            doc: Some(&Documentation {
                category: Category::List,
                description: "Create a list fromt the given values.",
                arguments: &["var_arg"],
                example: Some(Example {
                    example: "list_values('cat', 'dog', 'mouse')",
                    output: "[cat, dog, mouse]",
                }),
            }),
        }]
    }
}

impl ScalarFunction for ListValues {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let first = match inputs.first() {
            Some(expr) => expr.datatype(table_list)?,
            None => {
                let return_type = DataType::List(ListTypeMeta {
                    datatype: Box::new(DataType::Null),
                });
                return Ok(PlannedScalarFunction {
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

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: return_type.clone(),
            inputs,
            function_impl: Box::new(ListValuesImpl {
                list_datatype: return_type,
            }),
        })
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

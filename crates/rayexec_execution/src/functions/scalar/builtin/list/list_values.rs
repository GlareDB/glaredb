use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::make_list::make_list_from_values;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
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
                    function_impl: Box::new(ListValuesImpl),
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
            function_impl: Box::new(ListValuesImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListValuesImpl;

impl ScalarFunctionImpl for ListValuesImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        make_list_from_values(input.arrays(), input.selection(), output)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::array_buffer::ListItemMetadata;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::{PhysicalList, PhysicalStorage};
    use crate::expr;

    #[test]
    fn list_values_primitive() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();
        let batch = Batch::try_from_arrays([a, b]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = ListValues
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::try_new(
            &Arc::new(NopBufferManager),
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        // TODO: Assert list equality.

        let expected_metas = &[
            ListItemMetadata { offset: 0, len: 2 },
            ListItemMetadata { offset: 2, len: 2 },
            ListItemMetadata { offset: 4, len: 2 },
        ];

        let s = PhysicalList::get_addressable(&out.data).unwrap();
        assert_eq!(expected_metas, s);
    }
}

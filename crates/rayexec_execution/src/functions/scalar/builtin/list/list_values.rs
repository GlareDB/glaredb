use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::make_list::make_list_from_values;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;

pub const FUNCTION_SET_LIST_VALUES: ScalarFunctionSet = ScalarFunctionSet {
    name: "list_values",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::List,
        description: "Create a list fromt the given values.",
        arguments: &["var_arg"],
        example: Some(Example {
            example: "list_values('cat', 'dog', 'mouse')",
            output: "[cat, dog, mouse]",
        }),
    }),
    functions: &[RawScalarFunction::new(
        Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
            doc: None,
        },
        &ListValues,
    )],
};

#[derive(Debug, Clone)]
pub struct ListValues;

impl ScalarFunction for ListValues {
    type State = ();

    fn bind(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<BindState<Self::State>> {
        let first = match inputs.first() {
            Some(expr) => expr.datatype(table_list)?,
            None => {
                // No values in the list.
                DataType::Null
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
            datatype: Box::new(first),
        });

        Ok(BindState {
            state: (),
            return_type,
            inputs,
        })
    }

    fn execute(&self, _state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        make_list_from_values(input.arrays(), input.selection(), output)
    }
}

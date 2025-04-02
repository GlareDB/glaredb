use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::make_list::make_list_from_values;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};

pub const FUNCTION_SET_LIST_VALUES: ScalarFunctionSet = ScalarFunctionSet {
    name: "list_values",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::List,
        description: "Create a list from the given values.",
        arguments: &["var_arg"],
        example: Some(Example {
            example: "list_values('cat', 'dog', 'mouse')",
            output: "[cat, dog, mouse]",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List(&DataTypeId::Any),
        },
        &ListValues,
    )],
};

#[derive(Debug, Clone, Copy)]
pub struct ListValues;

impl ScalarFunction for ListValues {
    type State = ();

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let first = match inputs.first() {
            Some(expr) => expr.datatype()?,
            None => {
                // No values in the list.
                DataType::Null
            }
        };

        for input in &inputs {
            let dt = input.datatype()?;
            // TODO: We can add casts here.
            if dt != first {
                return Err(DbError::new(format!(
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

    fn execute(_state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        make_list_from_values(input.arrays(), input.selection(), output)
    }
}

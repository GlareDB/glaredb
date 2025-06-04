use glaredb_error::{DbError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::list_extract::list_extract;
use crate::arrays::datatype::DataTypeId;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::{FnName, ScalarFunctionSet};
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_LIST_EXTRACT: ScalarFunctionSet = ScalarFunctionSet {
    name: FnName::default("list_extract"),
    aliases: &[],
    doc: &[&Documentation {
        category: Category::List,
        description: "Extract an item from the list. Used 1-based indexing.",
        arguments: &["list", "index"],
        example: Some(Example {
            example: "list_extract([4,5,6], 2)",
            output: "5",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(&[DataTypeId::List, DataTypeId::Int64], DataTypeId::Any),
        &ListExtract,
    )],
};

#[derive(Debug)]
pub struct ListExtractState {
    index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl ScalarFunction for ListExtract {
    type State = ListExtractState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let index = ConstFold::rewrite(inputs[1].clone())?
            .try_into_scalar()?
            .try_as_i64()?;

        if index <= 0 {
            return Err(DbError::new("Index cannot be less than 1"));
        }
        // Adjust from 1-based indexing.
        let index = (index - 1) as usize;

        let inner_datatype = inputs[0]
            .datatype()?
            .try_get_list_type_meta()?
            .datatype
            .as_ref()
            .clone();

        Ok(BindState {
            state: ListExtractState { index },
            return_type: inner_datatype,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];
        list_extract(input, sel, output, state.index)
    }
}

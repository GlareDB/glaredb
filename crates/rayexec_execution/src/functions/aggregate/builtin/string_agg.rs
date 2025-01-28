use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::buffer_manager::BufferManager;
use crate::arrays::array::physical_type::{AddressableMut, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::arrays::scalar::ScalarValue;
use crate::expr::Expression;
use crate::functions::aggregate::states::{
    drain,
    unary_update,
    AggregateGroupStates,
    TypedAggregateGroupStates,
};
use crate::functions::aggregate::{
    AggregateFunction,
    AggregateFunctionImpl,
    PlannedAggregateFunction,
};
use crate::functions::documentation::{Category, Documentation};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringAgg;

impl FunctionInfo for StringAgg {
    fn name(&self) -> &'static str {
        "string_agg"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic_arg: None,
            return_type: DataTypeId::Utf8,
            doc: Some(&Documentation {
                category: Category::Aggregate,
                description: "Concatenate all non-NULL input string values using a delimiter.",
                arguments: &["inputs", "delimiter"],
                example: None,
            }),
        }]
    }
}

impl AggregateFunction for StringAgg {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedAggregateFunction> {
        plan_check_num_args(self, &inputs, 2)?;

        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        match &datatypes[0] {
            DataType::Utf8 => (),
            _ => return Err(invalid_input_types_error(self, &datatypes)),
        }

        if !inputs[1].is_const_foldable() {
            return Err(RayexecError::new(
                "Second argument to STRING_AGG must be constant",
            ));
        }

        let sep = match ConstFold::rewrite(table_list, inputs[1].clone())?.try_into_scalar()? {
            ScalarValue::Null => String::new(),
            ScalarValue::Utf8(v) => v.into_owned(),
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected value for STRING_AGG: {other}"
                )))
            }
        };

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: Box::new(StringAggImpl { sep }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringAggImpl {
    pub sep: String,
}

impl AggregateFunctionImpl for StringAggImpl {
    fn new_states(&self) -> Box<dyn AggregateGroupStates> {
        let sep = self.sep.clone();
        let state_init = move || StringAggState {
            sep: sep.clone(),
            string: None,
        };

        Box::new(TypedAggregateGroupStates::new(
            state_init,
            unary_update::<PhysicalUtf8, PhysicalUtf8, _>,
            drain::<PhysicalUtf8, _, _>,
        ))
    }
}

#[derive(Debug, Default)]
pub struct StringAggState {
    /// Separate between concatenated strings.
    sep: String,
    /// String build built up.
    ///
    /// None if we haven't received any input.
    string: Option<String>,
}

impl AggregateState<&str, str> for StringAggState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.string.is_none() {
            std::mem::swap(self, other);
            return Ok(());
        }

        if other.string.is_none() {
            return Ok(());
        }

        let s = self.string.as_mut().unwrap();
        s.push_str(&self.sep);
        s.push_str(other.string.as_ref().unwrap());

        Ok(())
    }

    fn update(&mut self, input: &str) -> Result<()> {
        match &mut self.string {
            Some(s) => {
                s.push_str(&self.sep);
                s.push_str(input)
            }
            None => self.string = Some(input.to_string()),
        }
        Ok(())
    }

    fn finalize<M, B>(&mut self, output: PutBuffer<M, B>) -> Result<()>
    where
        M: AddressableMut<B, T = str>,
        B: BufferManager,
    {
        match &self.string {
            Some(s) => output.put(s),
            None => output.put_null(),
        }
        Ok(())
    }
}

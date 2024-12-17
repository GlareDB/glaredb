use std::fmt::Debug;

use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, StateFinalizer};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{RayexecError, Result};

use crate::expr::Expression;
use crate::functions::aggregate::states::{new_unary_aggregate_states, AggregateGroupStates};
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

        new_unary_aggregate_states::<PhysicalUtf8, _, _, _, _>(state_init, move |states| {
            let builder = ArrayBuilder {
                datatype: DataType::Utf8,
                buffer: GermanVarlenBuffer::<str>::with_len(states.len()),
            };
            StateFinalizer::finalize(states, builder)
        })
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

impl AggregateState<&str, String> for StringAggState {
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

    fn finalize(&mut self) -> Result<(String, bool)> {
        match self.string.take() {
            Some(s) => Ok((s, true)),
            None => Ok((String::new(), false)),
        }
    }
}

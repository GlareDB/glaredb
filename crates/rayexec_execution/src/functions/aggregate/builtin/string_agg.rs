use std::fmt::Debug;

use glaredb_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{AddressableMut, PhysicalUtf8};
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::AggregateState;
use crate::arrays::executor::PutBuffer;
use crate::arrays::scalar::BorrowedScalarValue;
use crate::expr::Expression;
use crate::functions::aggregate::simple::{BinaryAggregate, SimpleBinaryAggregate};
use crate::functions::aggregate::RawAggregateFunction;
use crate::functions::bind_state::BindState;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::AggregateFunctionSet;
use crate::functions::Signature;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

pub const FUNCTION_SET_STRING_AGG: AggregateFunctionSet = AggregateFunctionSet {
    name: "string_agg",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Aggregate,
        description: "Concatenate all non-NULL input string values using a delimiter.",
        arguments: &["inputs", "delimiter"],
        example: None,
    }),
    functions: &[RawAggregateFunction::new(
        &Signature::new(&[DataTypeId::Utf8, DataTypeId::Utf8], DataTypeId::Utf8),
        &SimpleBinaryAggregate::new(&StringAgg),
    )],
};

#[derive(Debug)]
pub struct StringAggBindState {
    sep: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringAgg;

impl BinaryAggregate for StringAgg {
    type Input1 = PhysicalUtf8;
    type Input2 = PhysicalUtf8;
    type Output = PhysicalUtf8;

    type BindState = StringAggBindState;
    type GroupState = StringAggState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::BindState>> {
        if !inputs[1].is_const_foldable() {
            return Err(RayexecError::new(
                "Second argument to STRING_AGG must be constant",
            ));
        }

        let sep = match ConstFold::rewrite(inputs[1].clone())?.try_into_scalar()? {
            BorrowedScalarValue::Null => String::new(),
            BorrowedScalarValue::Utf8(v) => v.into_owned(),
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected value for STRING_AGG: {other}"
                )))
            }
        };

        Ok(BindState {
            state: StringAggBindState { sep },
            return_type: DataType::Utf8,
            inputs,
        })
    }

    fn new_aggregate_state(_state: &Self::BindState) -> Self::GroupState {
        StringAggState::default()
    }
}

#[derive(Debug, Default)]
pub struct StringAggState {
    /// String build built up.
    ///
    /// None if we haven't received any input.
    string: Option<String>,
}

impl AggregateState<(&str, &str), str> for StringAggState {
    type BindState = StringAggBindState;

    fn merge(&mut self, state: &StringAggBindState, other: &mut Self) -> Result<()> {
        if self.string.is_none() {
            std::mem::swap(self, other);
            return Ok(());
        }

        if other.string.is_none() {
            return Ok(());
        }

        let s = self.string.as_mut().unwrap();
        s.push_str(&state.sep);
        s.push_str(other.string.as_ref().unwrap());

        Ok(())
    }

    fn update(&mut self, state: &StringAggBindState, (input, _): (&str, &str)) -> Result<()> {
        match &mut self.string {
            Some(s) => {
                s.push_str(&state.sep);
                s.push_str(input)
            }
            None => self.string = Some(input.to_string()),
        }
        Ok(())
    }

    fn finalize<M>(&mut self, _state: &StringAggBindState, output: PutBuffer<M>) -> Result<()>
    where
        M: AddressableMut<T = str>,
    {
        match &self.string {
            Some(s) => output.put(s),
            None => output.put_null(),
        }
        Ok(())
    }
}

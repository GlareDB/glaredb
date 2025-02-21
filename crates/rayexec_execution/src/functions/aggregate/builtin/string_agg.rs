use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::executor::aggregate::{AggregateState, UnaryNonNullUpdater};
use crate::arrays::executor::PutBuffer;
use crate::arrays::scalar::ScalarValue;
use crate::expr::Expression;
use crate::functions::aggregate::states::{AggregateFunctionImpl, AggregateStateLogic};
use crate::functions::aggregate::{AggregateFunction, PlannedAggregateFunction};
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

        let extra = Arc::new(sep) as Arc<_>;

        Ok(PlannedAggregateFunction {
            function: Box::new(*self),
            return_type: DataType::Utf8,
            inputs,
            function_impl: AggregateFunctionImpl::new::<StringAggImpl>(Some(extra)),
        })
    }
}

#[derive(Debug, Clone)]
pub struct StringAggImpl {
    pub sep: String,
}

impl AggregateStateLogic for StringAggImpl {
    type State = StringAggState;

    fn init_state(extra: Option<&dyn Any>) -> Self::State {
        let sep = extra.unwrap().downcast_ref::<String>().unwrap();
        StringAggState {
            sep: sep.clone(),
            string: None,
        }
    }

    fn update(
        _extra: Option<&dyn Any>,
        inputs: &[Array],
        num_rows: usize,
        states: &mut [*mut Self::State],
    ) -> Result<()> {
        UnaryNonNullUpdater::update::<PhysicalUtf8, _, _>(&inputs[0], 0..num_rows, states)
    }

    fn combine(
        _extra: Option<&dyn Any>,
        src: &mut [&mut Self::State],
        dest: &mut [&mut Self::State],
    ) -> Result<()> {
        // TODO: Reduce duplication.
        if src.len() != dest.len() {
            return Err(RayexecError::new(
                "Source and destination have different number of states",
            )
            .with_field("source", src.len())
            .with_field("dest", dest.len()));
        }

        for (src, dest) in src.iter_mut().zip(dest) {
            dest.merge(src)?;
        }

        Ok(())
    }

    fn finalize(
        _extra: Option<&dyn Any>,
        states: &mut [&mut Self::State],
        output: &mut Array,
    ) -> Result<()> {
        // TODO: Reduce duplication
        let buffer = &mut PhysicalUtf8::get_addressable_mut(&mut output.data)?;
        let validity = &mut output.validity;

        for (idx, state) in states.iter_mut().enumerate() {
            state.finalize(PutBuffer::new(idx, buffer, validity))?;
        }

        Ok(())
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

    fn finalize<M>(&mut self, output: PutBuffer<M>) -> Result<()>
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

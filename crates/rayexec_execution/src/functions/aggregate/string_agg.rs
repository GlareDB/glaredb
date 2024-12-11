use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::PhysicalUtf8;
use rayexec_bullet::scalar::ScalarValue;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};

use super::{AggregateFunction, DefaultGroupedStates, PlannedAggregateFunction};
use crate::expr::Expression;
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::functions::{invalid_input_types_error, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
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
            input: &[DataTypeId::Utf8, DataTypeId::Utf8],
            variadic: None,
            return_type: DataTypeId::Utf8,
        }]
    }
}

impl AggregateFunction for StringAgg {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        let sep: String = PackedDecoder::new(state).decode_next()?;
        Ok(Box::new(StringAggImpl { sep }))
    }

    fn plan_from_datatypes(
        &self,
        _inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn plan_from_expressions(
        &self,
        table_list: &TableList,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
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

        Ok(Box::new(StringAggImpl { sep }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringAggImpl {
    pub sep: String,
}

impl PlannedAggregateFunction for StringAggImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &StringAgg
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.sep)
    }

    fn return_type(&self) -> DataType {
        DataType::Utf8
    }

    fn new_grouped_state(&self) -> Result<Box<dyn super::GroupedStates>> {
        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [StringAggState],
        ) -> Result<()> {
            // Note second array ignored, should be a constant.
            UnaryNonNullUpdater::update::<PhysicalUtf8, _, _, _>(arrays[0], mapping, states)
        }

        let sep = self.sep.clone();

        Ok(Box::new(DefaultGroupedStates::new(
            move || StringAggState {
                sep: sep.clone(),
                string: None,
            },
            update,
            move |states| {
                let builder = ArrayBuilder {
                    datatype: DataType::Utf8,
                    buffer: GermanVarlenBuffer::<str>::with_len(states.len()),
                };
                StateFinalizer::finalize(states, builder)
            },
        )))
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

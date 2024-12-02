use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};

use super::covar::{CovarPopFinalize, CovarState};
use super::stddev::{StddevPopFinalize, VarianceState};
use super::{
    primitive_finalize,
    AggregateFunction,
    DefaultGroupedStates,
    PlannedAggregateFunction,
};
use crate::expr::Expression;
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;

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
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        unimplemented!()
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
        unimplemented!()
    }
}

#[derive(Debug, Default)]
pub struct StringAggState {
    /// String build built up.
    string: String,
}

impl AggregateState<&str, String> for StringAggState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        unimplemented!()
    }

    fn update(&mut self, input: &str) -> Result<()> {
        unimplemented!()
    }

    fn finalize(&mut self) -> Result<(String, bool)> {
        unimplemented!()
    }
}

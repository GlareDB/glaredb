use std::fmt::Debug;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalAny;
use rayexec_error::Result;

use crate::functions::aggregate::{
    primitive_finalize,
    AggregateFunction,
    ChunkGroupAddressIter,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrCount;

impl FunctionInfo for RegrCount {
    fn name(&self) -> &'static str {
        "regr_count"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Int64,
        }]
    }
}

impl AggregateFunction for RegrCount {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(RegrCountImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RegrCountImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrCountImpl;

impl PlannedAggregateFunction for RegrCountImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &RegrCount
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        let datatype = self.return_type();

        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [RegrCountState],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalAny, PhysicalAny, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(
            RegrCountState::default,
            update,
            move |states| primitive_finalize(datatype.clone(), states),
        )))
    }
}

/// State for `regr_count`.
///
/// Note that this can be used for any input type, but the sql function we
/// expose only accepts f64 (to match Postgres).
#[derive(Debug, Clone, Copy, Default)]
pub struct RegrCountState {
    count: i64,
}

impl AggregateState<((), ()), i64> for RegrCountState {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, _input: ((), ())) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(i64, bool)> {
        Ok((self.count, true))
    }
}

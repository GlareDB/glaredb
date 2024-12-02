use std::fmt::Debug;
use std::marker::PhantomData;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, BinaryNonNullUpdater};
use rayexec_bullet::executor::physical_type::PhysicalF64;
use rayexec_error::Result;

use super::{
    primitive_finalize,
    AggregateFunction,
    DefaultGroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::aggregate::ChunkGroupAddressIter;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgY;

impl FunctionInfo for RegrAvgY {
    fn name(&self) -> &'static str {
        "regr_avgy"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for RegrAvgY {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(RegrAvgYImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RegrAvgYImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegrAvgYImpl;

impl PlannedAggregateFunction for RegrAvgYImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &RegrAvgY
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn super::GroupedStates>> {
        let datatype = self.return_type();

        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [RegrAvgState<RegrAvgYImpl>],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(
            RegrAvgState::<RegrAvgYImpl>::default,
            update,
            move |states| primitive_finalize(datatype.clone(), states),
        )))
    }
}

impl RegrAvgInput for RegrAvgYImpl {
    fn input(vals: (f64, f64)) -> f64 {
        // 'y' in (y, x)
        vals.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegrAvgX;

impl FunctionInfo for RegrAvgX {
    fn name(&self) -> &'static str {
        "regr_avgx"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for RegrAvgX {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(RegrAvgXImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(RegrAvgXImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegrAvgXImpl;

impl PlannedAggregateFunction for RegrAvgXImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &RegrAvgX
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn return_type(&self) -> DataType {
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn super::GroupedStates>> {
        let datatype = self.return_type();

        fn update(
            arrays: &[&Array],
            mapping: ChunkGroupAddressIter,
            states: &mut [RegrAvgState<RegrAvgXImpl>],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(
            RegrAvgState::<RegrAvgXImpl>::default,
            update,
            move |states| primitive_finalize(datatype.clone(), states),
        )))
    }
}

impl RegrAvgInput for RegrAvgXImpl {
    fn input(vals: (f64, f64)) -> f64 {
        // 'x' in (y, x)
        vals.1
    }
}

pub trait RegrAvgInput: Sync + Send + Default + Debug + 'static {
    fn input(vals: (f64, f64)) -> f64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RegrAvgState<F>
where
    F: RegrAvgInput,
{
    sum: f64,
    count: i64,
    _input: PhantomData<F>,
}

impl<F> AggregateState<(f64, f64), f64> for RegrAvgState<F>
where
    F: RegrAvgInput,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.count += other.count;
        self.sum += other.sum;
        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        self.sum += F::input(input);
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        if self.count == 0 {
            Ok((0.0, false))
        } else {
            Ok((self.sum / self.count as f64, true))
        }
    }
}

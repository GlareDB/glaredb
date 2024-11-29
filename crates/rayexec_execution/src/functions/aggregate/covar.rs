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
pub struct CovarPop;

impl FunctionInfo for CovarPop {
    fn name(&self) -> &'static str {
        "covar_pop"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for CovarPop {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(CovarPopImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(CovarPopImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarPopImpl;

impl PlannedAggregateFunction for CovarPopImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &CovarPop
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
            states: &mut [CovarState<CovarPopFinalize>],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(update, move |states| {
            primitive_finalize(datatype.clone(), states)
        })))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarSamp;

impl FunctionInfo for CovarSamp {
    fn name(&self) -> &'static str {
        "covar_samp"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Float64, DataTypeId::Float64],
            variadic: None,
            return_type: DataTypeId::Float64,
        }]
    }
}

impl AggregateFunction for CovarSamp {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(CovarSampImpl))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 2)?;
        match (&inputs[0], &inputs[1]) {
            (DataType::Float64, DataType::Float64) => Ok(Box::new(CovarSampImpl)),
            (a, b) => Err(invalid_input_types_error(self, &[a, b])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CovarSampImpl;

impl PlannedAggregateFunction for CovarSampImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &CovarSamp
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
            states: &mut [CovarState<CovarSampFinalize>],
        ) -> Result<()> {
            BinaryNonNullUpdater::update::<PhysicalF64, PhysicalF64, _, _, _>(
                arrays[0], arrays[1], mapping, states,
            )
        }

        Ok(Box::new(DefaultGroupedStates::new(update, move |states| {
            primitive_finalize(datatype.clone(), states)
        })))
    }
}

pub trait CovarFinalize: Sync + Send + Debug + Default + 'static {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarSampFinalize;

impl CovarFinalize for CovarSampFinalize {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool) {
        match count {
            0 | 1 => (0.0, false),
            _ => (co_moment / (count - 1) as f64, true),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CovarPopFinalize;

impl CovarFinalize for CovarPopFinalize {
    fn finalize(co_moment: f64, count: i64) -> (f64, bool) {
        match count {
            0 => (0.0, false),
            _ => (co_moment / count as f64, true),
        }
    }
}

#[derive(Debug, Default)]
pub struct CovarState<F: CovarFinalize> {
    pub count: i64,
    pub meanx: f64,
    pub meany: f64,
    pub co_moment: f64,
    _finalize: PhantomData<F>,
}

impl<F> AggregateState<(f64, f64), f64> for CovarState<F>
where
    F: CovarFinalize,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if self.count == 0 {
            std::mem::swap(self, other);
            return Ok(());
        }
        if other.count == 0 {
            return Ok(());
        }

        let count = self.count + other.count;
        let meanx =
            (other.count as f64 * other.meanx + self.count as f64 * self.meanx) / count as f64;
        let meany =
            (other.count as f64 * other.meany + self.count as f64 * self.meany) / count as f64;

        let deltax = self.meanx - other.meanx;
        let deltay = self.meany - other.meany;

        self.co_moment = other.co_moment
            + self.co_moment
            + deltax * deltay * other.count as f64 * self.count as f64 / count as f64;
        self.meanx = meanx;
        self.meany = meany;
        self.count = count;

        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        // Note that 'y' comes first, covariance functions are call like `COVAR_SAMP(y, x)`.
        let x = input.1;
        let y = input.0;

        self.count += 1;
        let n = self.count as f64;

        let dx = x - self.meanx;
        let meanx = self.meanx + dx / n;

        let dy = y - self.meany;
        let meany = self.meany + dy / n;

        let co_moment = self.co_moment + dx * (y - meany);

        self.meanx = meanx;
        self.meany = meany;
        self.co_moment = co_moment;

        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        Ok(F::finalize(self.co_moment, self.count))
    }
}

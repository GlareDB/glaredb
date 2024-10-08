use std::fmt::Debug;

use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::physical_type::{
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
};
use rayexec_bullet::scalar::interval::Interval;
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use super::{
    primitive_finalize,
    unary_update,
    AggregateFunction,
    GroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::aggregate::DefaultGroupedStates;
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Min;

impl FunctionInfo for Min {
    fn name(&self) -> &'static str {
        "min"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl AggregateFunction for Min {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(MinImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal64(_)
            | DataType::Decimal128(_)
            | DataType::Timestamp(_) => Ok(Box::new(MinImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Max;

impl FunctionInfo for Max {
    fn name(&self) -> &'static str {
        "max"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl AggregateFunction for Max {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(MinImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal64(_)
            | DataType::Decimal128(_)
            | DataType::Timestamp(_) => Ok(Box::new(MaxImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction for MinImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Min
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let datatype = self.datatype.clone();
        match self.datatype.physical_type().expect("to get physical type") {
            PhysicalType::Int8 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<i8>, PhysicalI8, i8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int16 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<i16>, PhysicalI16, i16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<i32>, PhysicalI32, i32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<i64>, PhysicalI64, i64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int128 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<i128>, PhysicalI128, i128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt8 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<u8>, PhysicalU8, u8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt16 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<u16>, PhysicalU16, u16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<u32>, PhysicalU32, u32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<u64>, PhysicalU64, u64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt128 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<u128>, PhysicalU128, u128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<f32>, PhysicalF32, f32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<f64>, PhysicalF64, f64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Interval => Box::new(DefaultGroupedStates::new(
                unary_update::<MinState<Interval>, PhysicalInterval, Interval>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            other => panic!("unexpected physical type {other:?}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaxImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction for MaxImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Max
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let datatype = self.datatype.clone();
        match self.datatype.physical_type().expect("to get physical type") {
            PhysicalType::Int8 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<i8>, PhysicalI8, i8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int16 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<i16>, PhysicalI16, i16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<i32>, PhysicalI32, i32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<i64>, PhysicalI64, i64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int128 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<i128>, PhysicalI128, i128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt8 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<u8>, PhysicalU8, u8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt16 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<u16>, PhysicalU16, u16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<u32>, PhysicalU32, u32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<u64>, PhysicalU64, u64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt128 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<u128>, PhysicalU128, u128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float32 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<f32>, PhysicalF32, f32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float64 => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<f64>, PhysicalF64, f64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Interval => Box::new(DefaultGroupedStates::new(
                unary_update::<MaxState<Interval>, PhysicalInterval, Interval>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            other => panic!("unexpected physical type {other:?}"),
        }
    }
}

#[derive(Debug, Default)]
pub struct MinState<T> {
    min: T,
    valid: bool,
}

impl<T: PartialOrd + Debug + Default> AggregateState<T, T> for MinState<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            self.min = other.min;
        } else if other.valid && other.min < self.min {
            self.min = other.min;
        }

        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = input;
        } else if input < self.min {
            self.min = input
        }
        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.min, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxState<T> {
    max: T,
    valid: bool,
}

impl<T: PartialOrd + Debug + Default> AggregateState<T, T> for MaxState<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            self.max = other.max;
        } else if other.valid && other.max > self.max {
            self.max = other.max;
        }
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = input;
        } else if input > self.max {
            self.max = input
        }

        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.max, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

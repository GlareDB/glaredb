use super::{
    helpers::{
        create_single_decimal_input_grouped_state, create_single_primitive_input_grouped_state,
        create_single_timestamp_input_grouped_state,
    },
    AggregateFunction, GroupedStates, PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::{
    datatype::{DataType, DataTypeId},
    executor::aggregate::AggregateState,
    scalar::interval::Interval,
};
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
        match &self.datatype {
            DataType::Int8 => create_single_primitive_input_grouped_state!(Int8, MinState<i8>),
            DataType::Int16 => create_single_primitive_input_grouped_state!(Int16, MinState<i16>),
            DataType::Int32 => create_single_primitive_input_grouped_state!(Int32, MinState<i32>),
            DataType::Int64 => create_single_primitive_input_grouped_state!(Int64, MinState<i64>),
            DataType::UInt8 => create_single_primitive_input_grouped_state!(UInt8, MinState<u8>),
            DataType::UInt16 => create_single_primitive_input_grouped_state!(UInt16, MinState<u16>),
            DataType::UInt32 => create_single_primitive_input_grouped_state!(UInt32, MinState<u32>),
            DataType::UInt64 => create_single_primitive_input_grouped_state!(UInt64, MinState<u64>),
            DataType::Float32 => {
                create_single_primitive_input_grouped_state!(Float32, MinState<f32>)
            }
            DataType::Float64 => {
                create_single_primitive_input_grouped_state!(Float64, MinState<f64>)
            }
            DataType::Interval => {
                create_single_primitive_input_grouped_state!(Interval, MinState<Interval>)
            }
            DataType::Timestamp(meta) => {
                create_single_timestamp_input_grouped_state::<MinState<i64>>(meta.unit)
            }
            DataType::Decimal64(meta) => {
                create_single_decimal_input_grouped_state!(
                    Decimal64,
                    MinState<i64>,
                    meta.precision,
                    meta.scale
                )
            }
            DataType::Decimal128(meta) => {
                create_single_decimal_input_grouped_state!(
                    Decimal128,
                    MinState<i128>,
                    meta.precision,
                    meta.scale
                )
            }
            datatype => panic!("unexpected datatype {datatype}"),
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
        match &self.datatype {
            DataType::Int8 => create_single_primitive_input_grouped_state!(Int8, MaxState<i8>),
            DataType::Int16 => create_single_primitive_input_grouped_state!(Int16, MaxState<i16>),
            DataType::Int32 => create_single_primitive_input_grouped_state!(Int32, MaxState<i32>),
            DataType::Int64 => create_single_primitive_input_grouped_state!(Int64, MaxState<i64>),
            DataType::UInt8 => create_single_primitive_input_grouped_state!(UInt8, MaxState<u8>),
            DataType::UInt16 => create_single_primitive_input_grouped_state!(UInt16, MaxState<u16>),
            DataType::UInt32 => create_single_primitive_input_grouped_state!(UInt32, MaxState<u32>),
            DataType::UInt64 => create_single_primitive_input_grouped_state!(UInt64, MaxState<u64>),
            DataType::Float32 => {
                create_single_primitive_input_grouped_state!(Float32, MaxState<f32>)
            }
            DataType::Float64 => {
                create_single_primitive_input_grouped_state!(Float64, MaxState<f64>)
            }
            DataType::Interval => {
                create_single_primitive_input_grouped_state!(Interval, MaxState<Interval>)
            }
            DataType::Timestamp(meta) => {
                create_single_timestamp_input_grouped_state::<MaxState<i64>>(meta.unit)
            }
            DataType::Decimal64(meta) => {
                create_single_decimal_input_grouped_state!(
                    Decimal64,
                    MaxState<i64>,
                    meta.precision,
                    meta.scale
                )
            }
            DataType::Decimal128(meta) => {
                create_single_decimal_input_grouped_state!(
                    Decimal128,
                    MaxState<i128>,
                    meta.precision,
                    meta.scale
                )
            }
            datatype => panic!("unexpected datatype {datatype}"),
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

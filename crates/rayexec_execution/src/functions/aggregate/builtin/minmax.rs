use std::fmt::Debug;

use half::f16;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::{AggregateState, StateFinalizer};
use rayexec_bullet::executor::builder::{ArrayBuilder, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::{
    PhysicalBinary,
    PhysicalF16,
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

use crate::functions::aggregate::{
    primitive_finalize,
    unary_update,
    AggregateFunction,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction2,
};
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
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction2>> {
        Ok(Box::new(MinImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction2>> {
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
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::Binary
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
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction2>> {
        Ok(Box::new(MinImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction2>> {
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
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::Binary
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

impl PlannedAggregateFunction2 for MinImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Min
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        let datatype = self.datatype.clone();
        Ok(
            match self.datatype.physical_type().expect("to get physical type") {
                PhysicalType::Int8 => Box::new(DefaultGroupedStates::new(
                    MinState::<i8>::default,
                    unary_update::<MinState<i8>, PhysicalI8, i8>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int16 => Box::new(DefaultGroupedStates::new(
                    MinState::<i16>::default,
                    unary_update::<MinState<i16>, PhysicalI16, i16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int32 => Box::new(DefaultGroupedStates::new(
                    MinState::<i32>::default,
                    unary_update::<MinState<i32>, PhysicalI32, i32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int64 => Box::new(DefaultGroupedStates::new(
                    MinState::<i64>::default,
                    unary_update::<MinState<i64>, PhysicalI64, i64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int128 => Box::new(DefaultGroupedStates::new(
                    MinState::<i128>::default,
                    unary_update::<MinState<i128>, PhysicalI128, i128>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt8 => Box::new(DefaultGroupedStates::new(
                    MinState::<u8>::default,
                    unary_update::<MinState<u8>, PhysicalU8, u8>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt16 => Box::new(DefaultGroupedStates::new(
                    MinState::<u16>::default,
                    unary_update::<MinState<u16>, PhysicalU16, u16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt32 => Box::new(DefaultGroupedStates::new(
                    MinState::<u32>::default,
                    unary_update::<MinState<u32>, PhysicalU32, u32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt64 => Box::new(DefaultGroupedStates::new(
                    MinState::<u64>::default,
                    unary_update::<MinState<u64>, PhysicalU64, u64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt128 => Box::new(DefaultGroupedStates::new(
                    MinState::<u128>::default,
                    unary_update::<MinState<u128>, PhysicalU128, u128>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float16 => Box::new(DefaultGroupedStates::new(
                    MinState::<f16>::default,
                    unary_update::<MinState<f16>, PhysicalF16, f16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float32 => Box::new(DefaultGroupedStates::new(
                    MinState::<f32>::default,
                    unary_update::<MinState<f32>, PhysicalF32, f32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float64 => Box::new(DefaultGroupedStates::new(
                    MinState::<f64>::default,
                    unary_update::<MinState<f64>, PhysicalF64, f64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Interval => Box::new(DefaultGroupedStates::new(
                    MinState::<Interval>::default,
                    unary_update::<MinState<Interval>, PhysicalInterval, Interval>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Utf8 => {
                    // Safe to read as binary since we should've already
                    // validated that this is valid utf8.
                    Box::new(DefaultGroupedStates::new(
                        MinStateBinary::default,
                        unary_update::<MinStateBinary, PhysicalBinary, Vec<u8>>,
                        move |states| {
                            let builder = ArrayBuilder {
                                datatype: datatype.clone(),
                                buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                            };
                            StateFinalizer::finalize(states, builder)
                        },
                    ))
                }
                PhysicalType::Binary => Box::new(DefaultGroupedStates::new(
                    MinStateBinary::default,
                    unary_update::<MinStateBinary, PhysicalBinary, Vec<u8>>,
                    move |states| {
                        let builder = ArrayBuilder {
                            datatype: datatype.clone(),
                            buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                        };
                        StateFinalizer::finalize(states, builder)
                    },
                )),

                other => panic!("unexpected physical type {other:?}"),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaxImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction2 for MaxImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Max
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        let datatype = self.datatype.clone();
        Ok(
            match self.datatype.physical_type().expect("to get physical type") {
                PhysicalType::Int8 => Box::new(DefaultGroupedStates::new(
                    MaxState::<i8>::default,
                    unary_update::<MaxState<i8>, PhysicalI8, i8>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int16 => Box::new(DefaultGroupedStates::new(
                    MaxState::<i16>::default,
                    unary_update::<MaxState<i16>, PhysicalI16, i16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int32 => Box::new(DefaultGroupedStates::new(
                    MaxState::<i32>::default,
                    unary_update::<MaxState<i32>, PhysicalI32, i32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int64 => Box::new(DefaultGroupedStates::new(
                    MaxState::<i64>::default,
                    unary_update::<MaxState<i64>, PhysicalI64, i64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Int128 => Box::new(DefaultGroupedStates::new(
                    MaxState::<i128>::default,
                    unary_update::<MaxState<i128>, PhysicalI128, i128>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt8 => Box::new(DefaultGroupedStates::new(
                    MaxState::<u8>::default,
                    unary_update::<MaxState<u8>, PhysicalU8, u8>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt16 => Box::new(DefaultGroupedStates::new(
                    MaxState::<u16>::default,
                    unary_update::<MaxState<u16>, PhysicalU16, u16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt32 => Box::new(DefaultGroupedStates::new(
                    MaxState::<u32>::default,
                    unary_update::<MaxState<u32>, PhysicalU32, u32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt64 => Box::new(DefaultGroupedStates::new(
                    MaxState::<u64>::default,
                    unary_update::<MaxState<u64>, PhysicalU64, u64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::UInt128 => Box::new(DefaultGroupedStates::new(
                    MaxState::<u128>::default,
                    unary_update::<MaxState<u128>, PhysicalU128, u128>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float16 => Box::new(DefaultGroupedStates::new(
                    MaxState::<f16>::default,
                    unary_update::<MaxState<f16>, PhysicalF16, f16>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float32 => Box::new(DefaultGroupedStates::new(
                    MaxState::<f32>::default,
                    unary_update::<MaxState<f32>, PhysicalF32, f32>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Float64 => Box::new(DefaultGroupedStates::new(
                    MaxState::<f64>::default,
                    unary_update::<MaxState<f64>, PhysicalF64, f64>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Interval => Box::new(DefaultGroupedStates::new(
                    MaxState::<Interval>::default,
                    unary_update::<MaxState<Interval>, PhysicalInterval, Interval>,
                    move |states| primitive_finalize(datatype.clone(), states),
                )),
                PhysicalType::Utf8 => {
                    // Safe to use binary, see Min
                    Box::new(DefaultGroupedStates::new(
                        MaxStateBinary::default,
                        unary_update::<MaxStateBinary, PhysicalBinary, Vec<u8>>,
                        move |states| {
                            let builder = ArrayBuilder {
                                datatype: datatype.clone(),
                                buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                            };
                            StateFinalizer::finalize(states, builder)
                        },
                    ))
                }
                PhysicalType::Binary => Box::new(DefaultGroupedStates::new(
                    MaxStateBinary::default,
                    unary_update::<MaxStateBinary, PhysicalBinary, Vec<u8>>,
                    move |states| {
                        let builder = ArrayBuilder {
                            datatype: datatype.clone(),
                            buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                        };
                        StateFinalizer::finalize(states, builder)
                    },
                )),

                other => panic!("unexpected physical type {other:?}"),
            },
        )
    }
}

#[derive(Debug, Default)]
pub struct MinState<T> {
    min: T,
    valid: bool,
}

impl<T> AggregateState<T, T> for MinState<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
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

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.min, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MinStateBinary {
    min: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], Vec<u8>> for MinStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.min, &mut other.min);
        } else if other.valid && other.min < self.min {
            std::mem::swap(&mut self.min, &mut other.min);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.min = input.into();
        } else if input < self.min.as_slice() {
            self.min = input.into();
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<(Vec<u8>, bool)> {
        if self.valid {
            Ok((std::mem::take(&mut self.min), true))
        } else {
            Ok((Vec::new(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxState<T> {
    max: T,
    valid: bool,
}

impl<T> AggregateState<T, T> for MaxState<T>
where
    T: PartialOrd + Debug + Default + Copy,
{
    fn merge(&mut self, other: &mut Self) -> Result<()> {
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

    fn finalize(&mut self) -> Result<(T, bool)> {
        if self.valid {
            Ok((self.max, true))
        } else {
            Ok((T::default(), false))
        }
    }
}

#[derive(Debug, Default)]
pub struct MaxStateBinary {
    max: Vec<u8>,
    valid: bool,
}

impl AggregateState<&[u8], Vec<u8>> for MaxStateBinary {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        if !self.valid {
            self.valid = other.valid;
            std::mem::swap(&mut self.max, &mut other.max);
        } else if other.valid && other.max > self.max {
            std::mem::swap(&mut self.max, &mut other.max);
        }

        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if !self.valid {
            self.valid = true;
            self.max = input.into();
        } else if input > self.max.as_slice() {
            self.max = input.into();
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<(Vec<u8>, bool)> {
        if self.valid {
            Ok((std::mem::take(&mut self.max), true))
        } else {
            Ok((Vec::new(), false))
        }
    }
}

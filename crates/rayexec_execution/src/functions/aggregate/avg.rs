use num_traits::{AsPrimitive, PrimInt};
use rayexec_bullet::{
    array::{Array, Decimal128Array, Decimal64Array, PrimitiveArray},
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId, DecimalTypeMeta},
    executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater},
};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};

use super::{AggregateFunction, DefaultGroupedStates, GroupedStates, PlannedAggregateFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_error::{RayexecError, Result};
use std::vec;
use std::{fmt::Debug, ops::AddAssign};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Avg;

impl FunctionInfo for Avg {
    fn name(&self) -> &'static str {
        "avg"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Float64],
                variadic: None,
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Int64],
                variadic: None,
                return_type: DataTypeId::Float64, // TODO: Should be decimal // TODO: Should it though?
            },
            Signature {
                input: &[DataTypeId::Decimal64],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Decimal128],
                variadic: None,
                return_type: DataTypeId::Decimal128,
            },
        ]
    }
}

impl AggregateFunction for Avg {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        let mut packed = PackedDecoder::new(state);
        let variant: String = packed.decode_next()?;
        match variant.as_str() {
            "decimal_64" => {
                let precision: i32 = packed.decode_next()?;
                let scale: i32 = packed.decode_next()?;
                Ok(Box::new(AvgImpl::Decimal64(AvgDecimal64Impl {
                    precision: precision as u8,
                    scale: scale as i8,
                })))
            }
            "decimal_128" => {
                let precision: i32 = packed.decode_next()?;
                let scale: i32 = packed.decode_next()?;
                Ok(Box::new(AvgImpl::Decimal128(AvgDecimal128Impl {
                    precision: precision as u8,
                    scale: scale as i8,
                })))
            }
            "float_64" => Ok(Box::new(AvgImpl::Float64(AvgFloat64Impl))),
            "int_64" => Ok(Box::new(AvgImpl::Int64(AvgInt64Impl))),

            other => Err(RayexecError::new(format!("Invalid avg variant: {other}"))),
        }
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(AvgImpl::Int64(AvgInt64Impl))),
            DataType::Float64 => Ok(Box::new(AvgImpl::Float64(AvgFloat64Impl))),
            DataType::Decimal64(meta) => Ok(Box::new(AvgImpl::Decimal64(AvgDecimal64Impl {
                precision: meta.precision,
                scale: meta.scale,
            }))),
            DataType::Decimal128(meta) => Ok(Box::new(AvgImpl::Decimal128(AvgDecimal128Impl {
                precision: meta.precision,
                scale: meta.scale,
            }))),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AvgImpl {
    Decimal64(AvgDecimal64Impl),
    Decimal128(AvgDecimal128Impl),
    Float64(AvgFloat64Impl),
    Int64(AvgInt64Impl),
}

impl PlannedAggregateFunction for AvgImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &Avg
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        match self {
            Self::Decimal64(v) => {
                packed.encode_next(&"decimal_64".to_string())?;
                packed.encode_next(&(v.precision as i32))?;
                packed.encode_next(&(v.scale as i32))?;
            }
            Self::Decimal128(v) => {
                packed.encode_next(&"decimal_128".to_string())?;
                packed.encode_next(&(v.precision as i32))?;
                packed.encode_next(&(v.scale as i32))?;
            }
            Self::Float64(_) => {
                packed.encode_next(&"float_64".to_string())?;
            }
            Self::Int64(_) => {
                packed.encode_next(&"int_64".to_string())?;
            }
        }
        Ok(())
    }

    fn return_type(&self) -> DataType {
        match self {
            Self::Decimal64(s) => DataType::Decimal64(DecimalTypeMeta::new(s.precision, s.scale)),
            Self::Decimal128(s) => DataType::Decimal128(DecimalTypeMeta::new(s.precision, s.scale)),
            Self::Float64(_) => DataType::Float64,
            Self::Int64(_) => DataType::Int64,
        }
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        match self {
            Self::Decimal64(s) => s.new_grouped_state(),
            Self::Decimal128(s) => s.new_grouped_state(),
            Self::Float64(s) => s.new_grouped_state(),
            Self::Int64(s) => s.new_grouped_state(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgDecimal64Impl {
    precision: u8,
    scale: i8,
}

impl AvgDecimal64Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [AvgStateInt<i64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Decimal64(arr) => {
                UnaryNonNullUpdater::update(row_selection, arr.get_primitive(), mapping, states)
            }
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<AvgStateInt<i64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let precision = self.precision;
        let scale = self.scale;
        let finalize = move |states: vec::Drain<_>| match Self::finalize(states)? {
            Array::Int64(arr) => Ok(Array::Decimal64(Decimal64Array::new(precision, scale, arr))),
            other => panic!("unexpected array type: {}", other.datatype()),
        };
        Box::new(DefaultGroupedStates::new(Self::update, finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgDecimal128Impl {
    precision: u8,
    scale: i8,
}

impl AvgDecimal128Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [AvgStateInt<i128>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Decimal128(arr) => {
                UnaryNonNullUpdater::update(row_selection, arr.get_primitive(), mapping, states)
            }
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<AvgStateInt<i128>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Int128(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let precision = self.precision;
        let scale = self.scale;
        let finalize = move |states: vec::Drain<_>| match Self::finalize(states)? {
            Array::Int128(arr) => Ok(Array::Decimal128(Decimal128Array::new(
                precision, scale, arr,
            ))),
            other => panic!("unexpected array type: {}", other.datatype()),
        };
        Box::new(DefaultGroupedStates::new(Self::update, finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgFloat64Impl;

impl AvgFloat64Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [AvgStateF64<f64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Float64(arr) => UnaryNonNullUpdater::update(row_selection, arr, mapping, states),
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<AvgStateF64<f64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Float64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgInt64Impl;

impl AvgInt64Impl {
    fn update(
        row_selection: &Bitmap,
        arrays: &[&Array],
        mapping: &[usize],
        states: &mut [AvgStateF64<i64>],
    ) -> Result<()> {
        match &arrays[0] {
            Array::Int64(arr) => UnaryNonNullUpdater::update(row_selection, arr, mapping, states),
            other => panic!("unexpected array type: {other:?}"),
        }
    }

    fn finalize(states: vec::Drain<AvgStateF64<i64>>) -> Result<Array> {
        let mut buffer = Vec::with_capacity(states.len());
        let mut bitmap = Bitmap::with_capacity(states.len());
        StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
        Ok(Array::Float64(PrimitiveArray::new(buffer, Some(bitmap))))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(Self::update, Self::finalize))
    }
}

#[derive(Debug, Default)]
struct AvgStateInt<T> {
    sum: T,
    count: i64,
}

impl<T: PrimInt + AddAssign + Debug + Default> AggregateState<T, T> for AvgStateInt<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        self.count += 1;
        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
        if self.count == 0 {
            return Ok((T::default(), false));
        }
        let count = T::from(self.count).unwrap();
        Ok((self.sum / count, true))
    }
}

#[derive(Debug, Default)]
struct AvgStateF64<T> {
    sum: T,
    count: i64,
}

impl<T: AsPrimitive<f64> + AddAssign + Debug + Default> AggregateState<T, f64> for AvgStateF64<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        self.count += 1;
        Ok(())
    }

    fn finalize(self) -> Result<(f64, bool)> {
        if self.count == 0 {
            return Ok((0.0, false));
        }
        let sum: f64 = self.sum.as_();
        Ok((sum / self.count as f64, true))
    }
}

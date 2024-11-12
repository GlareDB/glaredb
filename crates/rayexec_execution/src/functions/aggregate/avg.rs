use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::AddAssign;

use num_traits::AsPrimitive;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::aggregate::AggregateState;
use rayexec_bullet::executor::builder::{ArrayBuilder, ArrayDataBuffer, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::{PhysicalF64, PhysicalI128, PhysicalI64};
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use serde::{Deserialize, Serialize};

use super::{
    primitive_finalize,
    unary_update,
    AggregateFunction,
    DefaultGroupedStates,
    GroupedStates,
    PlannedAggregateFunction,
};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};

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
                return_type: DataTypeId::Float64,
            },
            Signature {
                input: &[DataTypeId::Decimal128],
                variadic: None,
                return_type: DataTypeId::Float64,
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
        // TODO: This used to return decimal is the case of the decimal impl,
        // but I changed that return floats. We might change it back to return
        // decimals, but not sure yet.
        DataType::Float64
    }

    fn new_grouped_state(&self) -> Result<Box<dyn GroupedStates>> {
        Ok(match self {
            Self::Decimal64(s) => s.new_grouped_state(),
            Self::Decimal128(s) => s.new_grouped_state(),
            Self::Float64(s) => s.new_grouped_state(),
            Self::Int64(s) => s.new_grouped_state(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgDecimal64Impl {
    precision: u8,
    scale: i8,
}

impl AvgDecimal64Impl {
    fn finalize(&self, states: &mut [AvgStateDecimal<i64>]) -> Result<Array> {
        let mut builder = ArrayBuilder {
            datatype: DataType::Float64,
            buffer: PrimitiveBuffer::with_len(states.len()),
        };

        let mut validities = Bitmap::new_with_all_true(states.len());

        let scale = f64::powi(10.0, self.scale.abs() as i32);

        for (idx, state) in states.iter_mut().enumerate() {
            let ((sum, count), valid) = state.finalize()?;

            if !valid {
                validities.set_unchecked(idx, false);
                continue;
            }

            let val = (sum as f64) / (count as f64 * scale);
            builder.buffer.put(idx, &val);
        }

        Ok(Array::new_with_validity_and_array_data(
            builder.datatype,
            validities,
            builder.buffer.into_data(),
        ))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let this = *self;
        Box::new(DefaultGroupedStates::new(
            unary_update::<AvgStateDecimal<i64>, PhysicalI64, (i128, i64)>,
            move |states| this.finalize(states),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgDecimal128Impl {
    precision: u8,
    scale: i8,
}

impl AvgDecimal128Impl {
    fn finalize(&self, states: &mut [AvgStateDecimal<i128>]) -> Result<Array> {
        let mut builder = ArrayBuilder {
            datatype: DataType::Float64,
            buffer: PrimitiveBuffer::with_len(states.len()),
        };

        let mut validities = Bitmap::new_with_all_true(states.len());

        let scale = f64::powi(10.0, self.scale.abs() as i32);

        for (idx, state) in states.iter_mut().enumerate() {
            let ((sum, count), valid) = state.finalize()?;

            if !valid {
                validities.set_unchecked(idx, false);
                continue;
            }

            let val = (sum as f64) / (count as f64 * scale);
            builder.buffer.put(idx, &val);
        }

        Ok(Array::new_with_validity_and_array_data(
            builder.datatype,
            validities,
            builder.buffer.into_data(),
        ))
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let this = *self;
        Box::new(DefaultGroupedStates::new(
            unary_update::<AvgStateDecimal<i128>, PhysicalI128, (i128, i64)>,
            move |states| this.finalize(states),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgFloat64Impl;

impl AvgFloat64Impl {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(
            unary_update::<AvgStateF64<f64>, PhysicalF64, f64>,
            move |states| primitive_finalize(DataType::Float64, states),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvgInt64Impl;

impl AvgInt64Impl {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        Box::new(DefaultGroupedStates::new(
            unary_update::<AvgStateF64<i64>, PhysicalI64, f64>,
            move |states| primitive_finalize(DataType::Float64, states),
        ))
    }
}

#[derive(Debug, Default)]
struct AvgStateDecimal<I> {
    sum: i128,
    count: i64,
    _input: PhantomData<I>,
}

impl<I: Into<i128> + Default + Debug> AggregateState<I, (i128, i64)> for AvgStateDecimal<I> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: I) -> Result<()> {
        self.sum += input.into();
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<((i128, i64), bool)> {
        if self.count == 0 {
            return Ok(((0, 0), false));
        }
        Ok(((self.sum, self.count), true))
    }
}

#[derive(Debug, Default)]
struct AvgStateF64<T> {
    sum: T,
    count: i64,
}

impl<T: AsPrimitive<f64> + AddAssign + Debug + Default> AggregateState<T, f64> for AvgStateF64<T> {
    fn merge(&mut self, other: &mut Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        self.count += 1;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(f64, bool)> {
        if self.count == 0 {
            return Ok((0.0, false));
        }
        let sum: f64 = self.sum.as_();
        Ok((sum / self.count as f64, true))
    }
}

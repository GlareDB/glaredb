use super::{AggregateFunction, DefaultGroupedStates, GroupedStates, PlannedAggregateFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use rayexec_bullet::{
    array::{Array, DecimalArray, PrimitiveArray},
    bitmap::Bitmap,
    datatype::{DataType, DataTypeId},
    executor::aggregate::{AggregateState, StateFinalizer, UnaryNonNullUpdater},
    scalar::interval::Interval,
};
use rayexec_error::Result;
use std::{fmt::Debug, vec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Min;

impl FunctionInfo for Min {
    fn name(&self) -> &'static str {
        "min"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            return_type: DataTypeId::Any,
        }]
    }
}

impl AggregateFunction for Min {
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
            | DataType::TimestampSeconds
            | DataType::TimestampMilliseconds
            | DataType::TimestampMicroseconds
            | DataType::TimestampNanoseconds => Ok(Box::new(MinPrimitiveImpl {
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
            return_type: DataTypeId::Any,
        }]
    }
}

impl AggregateFunction for Max {
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
            | DataType::TimestampSeconds
            | DataType::TimestampMilliseconds
            | DataType::TimestampMicroseconds
            | DataType::TimestampNanoseconds => Ok(Box::new(MaxPrimitiveImpl {
                datatype: inputs[0].clone(),
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

macro_rules! create_primitive_grouped_state {
    ($variant:ident, $state:ty) => {
        Box::new(DefaultGroupedStates::new(
            |row_selection: &Bitmap,
             arrays: &[&Array],
             mapping: &[usize],
             states: &mut [$state]| {
                match &arrays[0] {
                    Array::$variant(arr) => {
                        UnaryNonNullUpdater::update(row_selection, arr, mapping, states)
                    }
                    other => panic!("unexpected array type: {other:?}"),
                }
            },
            |states: vec::Drain<$state>| {
                let mut buffer = Vec::with_capacity(states.len());
                let mut bitmap = Bitmap::with_capacity(states.len());
                StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
                Ok(Array::$variant(PrimitiveArray::new(buffer, Some(bitmap))))
            },
        ))
    };
}

macro_rules! create_decimal_grouped_state {
    ($variant:ident, $state:ty, $precision:expr, $scale:expr) => {{
        let precision = $precision.clone();
        let scale = $scale.clone();
        Box::new(DefaultGroupedStates::new(
            |row_selection: &Bitmap,
             arrays: &[&Array],
             mapping: &[usize],
             states: &mut [$state]| {
                match &arrays[0] {
                    Array::$variant(arr) => UnaryNonNullUpdater::update(
                        row_selection,
                        arr.get_primitive(),
                        mapping,
                        states,
                    ),
                    other => panic!("unexpected array type: {other:?}"),
                }
            },
            move |states: vec::Drain<$state>| {
                let mut buffer = Vec::with_capacity(states.len());
                let mut bitmap = Bitmap::with_capacity(states.len());
                StateFinalizer::finalize(states, &mut buffer, &mut bitmap)?;
                let arr = PrimitiveArray::new(buffer, Some(bitmap));
                Ok(Array::$variant(DecimalArray::new(precision, scale, arr)))
            },
        ))
    }};
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinPrimitiveImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction for MinPrimitiveImpl {
    fn name(&self) -> &'static str {
        "min_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        match &self.datatype {
            DataType::Int8 => create_primitive_grouped_state!(Int8, MinState<i8>),
            DataType::Int16 => create_primitive_grouped_state!(Int16, MinState<i16>),
            DataType::Int32 => create_primitive_grouped_state!(Int32, MinState<i32>),
            DataType::Int64 => create_primitive_grouped_state!(Int64, MinState<i64>),
            DataType::UInt8 => create_primitive_grouped_state!(UInt8, MinState<u8>),
            DataType::UInt16 => create_primitive_grouped_state!(UInt16, MinState<u16>),
            DataType::UInt32 => create_primitive_grouped_state!(UInt32, MinState<u32>),
            DataType::UInt64 => create_primitive_grouped_state!(UInt64, MinState<u64>),
            DataType::Float32 => create_primitive_grouped_state!(Float32, MinState<f32>),
            DataType::Float64 => create_primitive_grouped_state!(Float64, MinState<f64>),
            DataType::Interval => create_primitive_grouped_state!(Interval, MinState<Interval>),
            DataType::TimestampSeconds => {
                create_primitive_grouped_state!(TimestampSeconds, MinState<i64>)
            }
            DataType::TimestampMilliseconds => {
                create_primitive_grouped_state!(TimestampMilliseconds, MinState<i64>)
            }
            DataType::TimestampMicroseconds => {
                create_primitive_grouped_state!(TimestampMicroseconds, MinState<i64>)
            }
            DataType::TimestampNanoseconds => {
                create_primitive_grouped_state!(TimestampNanoseconds, MinState<i64>)
            }
            DataType::Decimal64(meta) => {
                create_decimal_grouped_state!(Decimal64, MinState<i64>, meta.precision, meta.scale)
            }
            DataType::Decimal128(meta) => {
                create_decimal_grouped_state!(
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxPrimitiveImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction for MaxPrimitiveImpl {
    fn name(&self) -> &'static str {
        "max_primitive_impl"
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        match &self.datatype {
            DataType::Int8 => create_primitive_grouped_state!(Int8, MaxState<i8>),
            DataType::Int16 => create_primitive_grouped_state!(Int16, MaxState<i16>),
            DataType::Int32 => create_primitive_grouped_state!(Int32, MaxState<i32>),
            DataType::Int64 => create_primitive_grouped_state!(Int64, MaxState<i64>),
            DataType::UInt8 => create_primitive_grouped_state!(UInt8, MaxState<u8>),
            DataType::UInt16 => create_primitive_grouped_state!(UInt16, MaxState<u16>),
            DataType::UInt32 => create_primitive_grouped_state!(UInt32, MaxState<u32>),
            DataType::UInt64 => create_primitive_grouped_state!(UInt64, MaxState<u64>),
            DataType::Float32 => create_primitive_grouped_state!(Float32, MaxState<f32>),
            DataType::Float64 => create_primitive_grouped_state!(Float64, MaxState<f64>),
            DataType::Interval => create_primitive_grouped_state!(Interval, MaxState<Interval>),
            DataType::TimestampSeconds => {
                create_primitive_grouped_state!(TimestampSeconds, MaxState<i64>)
            }
            DataType::TimestampMilliseconds => {
                create_primitive_grouped_state!(TimestampMilliseconds, MaxState<i64>)
            }
            DataType::TimestampMicroseconds => {
                create_primitive_grouped_state!(TimestampMicroseconds, MaxState<i64>)
            }
            DataType::TimestampNanoseconds => {
                create_primitive_grouped_state!(TimestampNanoseconds, MaxState<i64>)
            }
            DataType::Decimal64(meta) => {
                create_decimal_grouped_state!(Decimal64, MaxState<i64>, meta.precision, meta.scale)
            }
            DataType::Decimal128(meta) => {
                create_decimal_grouped_state!(
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

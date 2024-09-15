use rayexec_bullet::{
    datatype::{DataType, DataTypeId},
    executor::aggregate::AggregateState,
};
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use std::fmt::Debug;

use crate::functions::{plan_check_num_args, FunctionInfo, Signature};

use super::{
    helpers::{
        create_single_boolean_input_grouped_state, create_single_decimal_input_grouped_state,
        create_single_primitive_input_grouped_state, create_single_timestamp_input_grouped_state,
    },
    AggregateFunction, GroupedStates, PlannedAggregateFunction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct First;

impl FunctionInfo for First {
    fn name(&self) -> &'static str {
        "first"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::Any],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl AggregateFunction for First {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedAggregateFunction>> {
        Ok(Box::new(FirstImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(
        &self,
        inputs: &[DataType],
    ) -> Result<Box<dyn PlannedAggregateFunction>> {
        plan_check_num_args(self, inputs, 1)?;
        Ok(Box::new(FirstImpl {
            datatype: inputs[0].clone(),
        }))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FirstImpl {
    datatype: DataType,
}

impl PlannedAggregateFunction for FirstImpl {
    fn aggregate_function(&self) -> &dyn AggregateFunction {
        &First
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        match self.datatype {
            DataType::Boolean => create_single_boolean_input_grouped_state!(FirstState<bool>),
            DataType::Int8 => create_single_primitive_input_grouped_state!(Int8, FirstState<i8>),
            DataType::Int16 => create_single_primitive_input_grouped_state!(Int16, FirstState<i16>),
            DataType::Int32 => create_single_primitive_input_grouped_state!(Int32, FirstState<i32>),
            DataType::Int64 => create_single_primitive_input_grouped_state!(Int64, FirstState<i64>),
            DataType::Int128 => {
                create_single_primitive_input_grouped_state!(Int128, FirstState<i128>)
            }
            DataType::UInt8 => create_single_primitive_input_grouped_state!(UInt8, FirstState<u8>),
            DataType::UInt16 => {
                create_single_primitive_input_grouped_state!(UInt16, FirstState<u16>)
            }
            DataType::UInt32 => {
                create_single_primitive_input_grouped_state!(UInt32, FirstState<u32>)
            }
            DataType::UInt64 => {
                create_single_primitive_input_grouped_state!(UInt64, FirstState<u64>)
            }
            DataType::UInt128 => {
                create_single_primitive_input_grouped_state!(UInt128, FirstState<u128>)
            }
            DataType::Float32 => {
                create_single_primitive_input_grouped_state!(Float32, FirstState<f32>)
            }
            DataType::Float64 => {
                create_single_primitive_input_grouped_state!(Float64, FirstState<f64>)
            }
            DataType::Decimal64(m) => create_single_decimal_input_grouped_state!(
                Decimal64,
                FirstState<i64>,
                m.precision,
                m.scale
            ),
            DataType::Decimal128(m) => create_single_decimal_input_grouped_state!(
                Decimal128,
                FirstState<i128>,
                m.precision,
                m.scale
            ),
            DataType::Timestamp(ref m) => {
                create_single_timestamp_input_grouped_state::<FirstState<i64>>(m.unit)
            }
            DataType::Date32 => {
                create_single_primitive_input_grouped_state!(Date32, FirstState<i32>)
            }
            DataType::Date64 => {
                create_single_primitive_input_grouped_state!(Date64, FirstState<i64>)
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Default)]
pub struct FirstState<T> {
    value: Option<T>,
}

impl<T: Default + Debug> AggregateState<T, T> for FirstState<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        if self.value.is_none() {
            self.value = other.value;
            return Ok(());
        }
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input);
        }
        Ok(())
    }

    fn finalize(self) -> Result<(T, bool)> {
        match self.value {
            Some(v) => Ok((v, true)),
            None => Ok((T::default(), false)),
        }
    }
}

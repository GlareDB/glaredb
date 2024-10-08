use rayexec_bullet::{
    datatype::{DataType, DataTypeId},
    executor::{
        aggregate::{AggregateState, StateFinalizer},
        builder::{ArrayBuilder, GermanVarlenBuffer},
        physical_type::{
            PhysicalBinary, PhysicalBool, PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16,
            PhysicalI32, PhysicalI64, PhysicalI8, PhysicalInterval, PhysicalType, PhysicalU128,
            PhysicalU16, PhysicalU32, PhysicalU64, PhysicalU8, PhysicalUntypedNull, PhysicalUtf8,
        },
    },
    scalar::interval::Interval,
    storage::UntypedNull,
};
use rayexec_error::Result;
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use std::fmt::Debug;

use crate::functions::{plan_check_num_args, FunctionInfo, Signature};

use super::{
    boolean_finalize, primitive_finalize, unary_update, untyped_null_finalize, AggregateFunction,
    DefaultGroupedStates, GroupedStates, PlannedAggregateFunction,
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
        let datatype = self.datatype.clone();
        match self.datatype.physical_type().expect("to get physical type") {
            PhysicalType::UntypedNull => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<UntypedNull>, PhysicalUntypedNull, UntypedNull>,
                untyped_null_finalize,
            )),
            PhysicalType::Boolean => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<bool>, PhysicalBool, bool>,
                move |states| boolean_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int8 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<i8>, PhysicalI8, i8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int16 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<i16>, PhysicalI16, i16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int32 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<i32>, PhysicalI32, i32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int64 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<i64>, PhysicalI64, i64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Int128 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<i128>, PhysicalI128, i128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt8 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<u8>, PhysicalU8, u8>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt16 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<u16>, PhysicalU16, u16>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt32 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<u32>, PhysicalU32, u32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt64 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<u64>, PhysicalU64, u64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::UInt128 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<u128>, PhysicalU128, u128>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float32 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<f32>, PhysicalF32, f32>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Float64 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<f64>, PhysicalF64, f64>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Interval => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstState<Interval>, PhysicalInterval, Interval>,
                move |states| primitive_finalize(datatype.clone(), states),
            )),
            PhysicalType::Binary => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstStateBinary, PhysicalBinary, Vec<u8>>,
                move |states| {
                    let builder = ArrayBuilder {
                        datatype: datatype.clone(),
                        buffer: GermanVarlenBuffer::<[u8]>::with_len(states.len()),
                    };
                    StateFinalizer::finalize(states, builder)
                },
            )),
            PhysicalType::Utf8 => Box::new(DefaultGroupedStates::new(
                unary_update::<FirstStateUtf8, PhysicalUtf8, String>,
                move |states| {
                    let builder = ArrayBuilder {
                        datatype: datatype.clone(),
                        buffer: GermanVarlenBuffer::<str>::with_len(states.len()),
                    };
                    StateFinalizer::finalize(states, builder)
                },
            )),
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

#[derive(Debug, Default)]
pub struct FirstStateBinary {
    value: Option<Vec<u8>>,
}

impl AggregateState<&[u8], Vec<u8>> for FirstStateBinary {
    fn merge(&mut self, other: Self) -> Result<()> {
        if self.value.is_none() {
            self.value = other.value;
            return Ok(());
        }
        Ok(())
    }

    fn update(&mut self, input: &[u8]) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_owned());
        }
        Ok(())
    }

    fn finalize(self) -> Result<(Vec<u8>, bool)> {
        match self.value {
            Some(v) => Ok((v, true)),
            None => Ok((Vec::new(), false)),
        }
    }
}

#[derive(Debug, Default)]
pub struct FirstStateUtf8 {
    value: Option<String>,
}

impl AggregateState<&str, String> for FirstStateUtf8 {
    fn merge(&mut self, other: Self) -> Result<()> {
        if self.value.is_none() {
            self.value = other.value;
            return Ok(());
        }
        Ok(())
    }

    fn update(&mut self, input: &str) -> Result<()> {
        if self.value.is_none() {
            self.value = Some(input.to_owned());
        }
        Ok(())
    }

    fn finalize(self) -> Result<(String, bool)> {
        match self.value {
            Some(v) => Ok((v, true)),
            None => Ok((String::new(), false)),
        }
    }
}

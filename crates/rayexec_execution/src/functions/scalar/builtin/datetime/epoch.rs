use rayexec_error::Result;

use crate::arrays::array::physical_type::PhysicalI64;
use crate::arrays::array::Array;
use crate::arrays::datatype::{DataType, DataTypeId, TimeUnit, TimestampTypeMeta};
use crate::arrays::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Epoch;

impl FunctionInfo for Epoch {
    fn name(&self) -> &'static str {
        "epoch"
    }

    fn aliases(&self) -> &'static [&'static str] {
        &["epoch_s"]
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // S -> Timestamp
            Signature {
                positional_args: &[DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Timestamp,
                doc: None,
            },
        ]
    }
}

impl ScalarFunction for Epoch {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Int64 => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Timestamp(TimestampTypeMeta {
                    unit: TimeUnit::Microsecond,
                }),
                inputs,
                function_impl: Box::new(EpochImpl::<1_000_000>),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochMs;

impl FunctionInfo for EpochMs {
    fn name(&self) -> &'static str {
        "epoch_ms"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            // MS -> Timestamp
            Signature {
                positional_args: &[DataTypeId::Int64],
                variadic_arg: None,
                return_type: DataTypeId::Timestamp,
                doc: None,
            },
        ]
    }
}

impl ScalarFunction for EpochMs {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        plan_check_num_args(self, &inputs, 1)?;
        match inputs[0].datatype(table_list)? {
            DataType::Int64 => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Timestamp(TimestampTypeMeta {
                    unit: TimeUnit::Microsecond,
                }),
                inputs,
                function_impl: Box::new(EpochImpl::<1000>),
            }),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochImpl<const S: i64>;

impl<const S: i64> ScalarFunctionImpl for EpochImpl<S> {
    fn execute2(&self, inputs: &[&Array]) -> Result<Array> {
        let input = inputs[0];
        to_timestamp::<S>(input)
    }
}

fn to_timestamp<const S: i64>(input: &Array) -> Result<Array> {
    let builder = ArrayBuilder {
        datatype: DataType::Timestamp(TimestampTypeMeta {
            unit: TimeUnit::Microsecond,
        }),
        buffer: PrimitiveBuffer::with_len(input.logical_len()),
    };

    UnaryExecutor::execute2::<PhysicalI64, _, _>(input, builder, |v, buf| {
        buf.put(&(v * S));
    })
}

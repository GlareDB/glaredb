use std::str::FromStr;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, TimeUnit, TimestampTypeMeta};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalI64;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::{not_implemented, RayexecError, Result};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTrunc;

impl FunctionInfo for DateTrunc {
    fn name(&self) -> &'static str {
        "date_trunc"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Date32],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Date64],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
            Signature {
                input: &[DataTypeId::Utf8, DataTypeId::Timestamp],
                variadic: None,
                return_type: DataTypeId::Decimal64,
            },
        ]
    }
}

impl ScalarFunction for DateTrunc {
    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        not_implemented!("decoding date_part")
    }

    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        // TODO: 3rd arg for optional timezone
        plan_check_num_args(self, &datatypes, 2)?;

        // Requires first argument to be constant (for now)
        let field = ConstFold::rewrite(bind_context, inputs[0].clone())?
            .try_into_scalar()?
            .try_into_string()?
            .to_lowercase();

        let field = field.parse::<TruncField>()?;

        match &datatypes[1] {
            DataType::Timestamp(m) => Ok(Box::new(DateTruncImpl {
                input_unit: m.unit,
                field,
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruncField {
    Microseconds,
    Milliseconds,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
    Decade,
    Century,
    Millennium,
}

impl FromStr for TruncField {
    type Err = RayexecError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "microseconds" => Self::Microseconds,
            "milliseconds" => Self::Milliseconds,
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            "decade" => Self::Decade,
            "century" => Self::Century,
            "millennium" => Self::Millennium,
            other => return Err(RayexecError::new(format!("Unexpected date field: {other}"))),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTruncImpl {
    input_unit: TimeUnit,
    field: TruncField,
}

impl PlannedScalarFunction for DateTruncImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &DateTrunc
    }

    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        not_implemented!("encode date_trunc")
    }

    fn return_type(&self) -> DataType {
        DataType::Timestamp(TimestampTypeMeta {
            unit: self.input_unit,
        })
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        let input = &inputs[1];

        let trunc = match self.input_unit {
            TimeUnit::Second => match self.field {
                TruncField::Microseconds | TruncField::Milliseconds | TruncField::Second => {
                    return Ok((*input).clone())
                }
                TruncField::Minute => 60,
                TruncField::Hour => 60 * 60,
                TruncField::Day => 24 * 60 * 60,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Millisecond => match self.field {
                TruncField::Microseconds | TruncField::Milliseconds => return Ok((*input).clone()),
                TruncField::Second => 1000,
                TruncField::Minute => 60 * 1000,
                TruncField::Hour => 60 * 60 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Microsecond => match self.field {
                TruncField::Microseconds => return Ok((*input).clone()),
                TruncField::Milliseconds => 1000,
                TruncField::Second => 1000 * 1000,
                TruncField::Minute => 60 * 1000 * 1000,
                TruncField::Hour => 60 * 60 * 1000 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Nanosecond => match self.field {
                TruncField::Microseconds => 1000,
                TruncField::Milliseconds => 1000 * 1000,
                TruncField::Second => 1000 * 1000 * 1000,
                TruncField::Minute => 60 * 1000 * 1000 * 1000,
                TruncField::Hour => 60 * 60 * 1000 * 1000 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000 * 1000 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
        };

        let builder = ArrayBuilder {
            datatype: DataType::Timestamp(TimestampTypeMeta {
                unit: self.input_unit,
            }),
            buffer: PrimitiveBuffer::with_len(input.logical_len()),
        };

        UnaryExecutor::execute::<PhysicalI64, _, _>(input, builder, |v, buf| {
            let v = (v / trunc) * trunc;
            buf.put(&v)
        })
    }
}

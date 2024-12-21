use std::str::FromStr;

use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::datatype::{DataType, DataTypeId, TimeUnit, TimestampTypeMeta};
use rayexec_bullet::executor::builder::{ArrayBuilder, PrimitiveBuffer};
use rayexec_bullet::executor::physical_type::PhysicalI64;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::{not_implemented, RayexecError, Result};

use crate::expr::Expression;
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTrunc;

impl FunctionInfo for DateTrunc {
    fn name(&self) -> &'static str {
        "date_trunc"
    }

    fn signatures(&self) -> &[Signature] {
        // TODO: Need to fix
        // const DOC: &Documentation = &Documentation {
        //     category: Category::Date,
        //     description: "Truncate to a specified precision.",
        //     arguments: &["part", "date"],
        //     example: Some(Example {
        //         example: "date_trunc('month', DATE '2024-12-17')",
        //         output: "17.000",
        //     }),
        // };

        &[
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Date32],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: None,
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Date64],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: None,
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Timestamp],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: None,
            },
        ]
    }
}

impl ScalarFunction for DateTrunc {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(table_list))
            .collect::<Result<Vec<_>>>()?;

        // TODO: 3rd arg for optional timezone
        plan_check_num_args(self, &datatypes, 2)?;

        // Requires first argument to be constant (for now)
        let field = ConstFold::rewrite(table_list, inputs[0].clone())?
            .try_into_scalar()?
            .try_into_string()?
            .to_lowercase();

        let field = field.parse::<TruncField>()?;

        match &datatypes[1] {
            DataType::Timestamp(m) => Ok(PlannedScalarFunction {
                function: Box::new(*self),
                return_type: DataType::Timestamp(TimestampTypeMeta { unit: m.unit }),
                inputs,
                function_impl: Box::new(DateTruncImpl {
                    input_unit: m.unit,
                    field,
                }),
            }),
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

impl ScalarFunctionImpl for DateTruncImpl {
    fn execute(&self, inputs: &[&ArrayOld]) -> Result<ArrayOld> {
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

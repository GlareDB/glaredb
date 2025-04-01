use std::str::FromStr;

use glaredb_error::{DbError, Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::PhysicalI64;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, TimeUnit};
use crate::arrays::executor::OutBuffer;
use crate::arrays::executor::scalar::UnaryExecutor;
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub const FUNCTION_SET_DATE_TRUNC: ScalarFunctionSet = ScalarFunctionSet {
    name: "date_trunc",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Date,
        description: "Truncates a timestamp to the specified precision",
        arguments: &["field", "timestamp"],
        example: None,
    }],
    // TODO: Date32/64
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Timestamp],
            DataTypeId::Timestamp,
        ),
        &DateTrunc,
    )],
};

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
    type Err = DbError;
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
            other => return Err(DbError::new(format!("Unexpected date field: {other}"))),
        })
    }
}

#[derive(Debug)]
pub struct DateTruncState {
    input_unit: TimeUnit,
    field: TruncField,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTrunc;

impl ScalarFunction for DateTrunc {
    type State = DateTruncState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        // Requires first argument to be constant (for now)
        let field = ConstFold::rewrite(inputs[0].clone())?
            .try_into_scalar()?
            .try_into_string()?
            .to_lowercase();

        let field = field.parse::<TruncField>()?;

        let time_m = match inputs[1].datatype()? {
            DataType::Timestamp(m) => m,
            other => {
                return Err(DbError::new("Unexpected data type").with_field("datatype", other));
            }
        };

        Ok(BindState {
            state: DateTruncState {
                input_unit: time_m.unit,
                field,
            },
            return_type: DataType::Timestamp(time_m),
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        // First element is field name, skip.
        let input = &input.arrays()[1];

        let trunc = match state.input_unit {
            TimeUnit::Second => match state.field {
                TruncField::Microseconds | TruncField::Milliseconds | TruncField::Second => 1,
                TruncField::Minute => 60,
                TruncField::Hour => 60 * 60,
                TruncField::Day => 24 * 60 * 60,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Millisecond => match state.field {
                TruncField::Microseconds | TruncField::Milliseconds => 1,
                TruncField::Second => 1000,
                TruncField::Minute => 60 * 1000,
                TruncField::Hour => 60 * 60 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Microsecond => match state.field {
                TruncField::Microseconds => 1,
                TruncField::Milliseconds => 1000,
                TruncField::Second => 1000 * 1000,
                TruncField::Minute => 60 * 1000 * 1000,
                TruncField::Hour => 60 * 60 * 1000 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
            TimeUnit::Nanosecond => match state.field {
                TruncField::Microseconds => 1000,
                TruncField::Milliseconds => 1000 * 1000,
                TruncField::Second => 1000 * 1000 * 1000,
                TruncField::Minute => 60 * 1000 * 1000 * 1000,
                TruncField::Hour => 60 * 60 * 1000 * 1000 * 1000,
                TruncField::Day => 24 * 60 * 60 * 1000 * 1000 * 1000,
                other => not_implemented!("trunc field: {other:?}"),
            },
        };

        UnaryExecutor::execute::<PhysicalI64, PhysicalI64, _>(
            input,
            sel,
            OutBuffer::from_array(output)?,
            |&v, buf| {
                let v = (v / trunc) * trunc;
                buf.put(&v)
            },
        )
    }
}

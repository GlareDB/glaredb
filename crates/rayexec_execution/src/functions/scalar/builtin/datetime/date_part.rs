use rayexec_error::Result;
use rayexec_parser::ast;

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::compute::date::{self, extract_date_part};
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::scalar::decimal::{Decimal64Type, DecimalType};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::functions::Signature;
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

pub const FUNCTION_SET_DATE_PART: ScalarFunctionSet = ScalarFunctionSet {
    name: "date_part",
    aliases: &[],
    doc: Some(&Documentation {
        category: Category::Date,
        description: "Get a subfield.",
        arguments: &["part", "date"],
        example: Some(Example {
            example: "date_part('day', DATE '2024-12-17')",
            output: "17.000", // TODO: Gotta fix the trailing zeros.
        }),
    }),
    // TODO: Optional third arg for timezone.
    functions: &[
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Utf8, DataTypeId::Date32],
                DataTypeId::Decimal64,
            ),
            &DatePart,
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Utf8, DataTypeId::Date64],
                DataTypeId::Decimal64,
            ),
            &DatePart,
        ),
        RawScalarFunction::new(
            &Signature::new(
                &[DataTypeId::Utf8, DataTypeId::Timestamp],
                DataTypeId::Decimal64,
            ),
            &DatePart,
        ),
    ],
};

#[derive(Debug)]
pub struct DatePartState {
    part: date::DatePart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatePart;

impl ScalarFunction for DatePart {
    type State = DatePartState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        // Requires first argument to be constant (for now)
        let part = ConstFold::rewrite(inputs[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        let part = part.parse::<ast::DatePart>()?;
        let part = convert_ast_date_part(part);

        Ok(BindState {
            state: DatePartState { part },
            return_type: DataType::Decimal64(DecimalTypeMeta::new(
                Decimal64Type::MAX_PRECISION,
                Decimal64Type::DEFAULT_SCALE,
            )),
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        // First input ignored (the constant "part" to extract)
        let input = &input.arrays()[1];
        extract_date_part(state.part, input, sel, output)
    }
}

pub fn convert_ast_date_part(date_part: ast::DatePart) -> date::DatePart {
    match date_part {
        ast::DatePart::Century => date::DatePart::Century,
        ast::DatePart::Day => date::DatePart::Day,
        ast::DatePart::Decade => date::DatePart::Decade,
        ast::DatePart::DayOfWeek => date::DatePart::DayOfWeek,
        ast::DatePart::DayOfYear => date::DatePart::DayOfYear,
        ast::DatePart::Epoch => date::DatePart::Epoch,
        ast::DatePart::Hour => date::DatePart::Hour,
        ast::DatePart::IsoDayOfWeek => date::DatePart::IsoDayOfWeek,
        ast::DatePart::IsoYear => date::DatePart::IsoYear,
        ast::DatePart::Julian => date::DatePart::Julian,
        ast::DatePart::Microseconds => date::DatePart::Microseconds,
        ast::DatePart::Millenium => date::DatePart::Millenium,
        ast::DatePart::Milliseconds => date::DatePart::Milliseconds,
        ast::DatePart::Minute => date::DatePart::Minute,
        ast::DatePart::Month => date::DatePart::Month,
        ast::DatePart::Quarter => date::DatePart::Quarter,
        ast::DatePart::Second => date::DatePart::Second,
        ast::DatePart::Timezone => date::DatePart::Timezone,
        ast::DatePart::TimezoneHour => date::DatePart::TimezoneHour,
        ast::DatePart::TimezoneMinute => date::DatePart::TimezoneMinute,
        ast::DatePart::Week => date::DatePart::Week,
        ast::DatePart::Year => date::DatePart::Year,
    }
}

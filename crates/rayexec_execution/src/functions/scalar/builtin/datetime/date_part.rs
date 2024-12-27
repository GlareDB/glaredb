use rayexec_error::Result;
use rayexec_parser::ast;

use crate::arrays::array::Array2;
use crate::arrays::compute::date::{self, extract_date_part};
use crate::arrays::datatype::{DataType, DataTypeId, DecimalTypeMeta};
use crate::arrays::scalar::decimal::{Decimal64Type, DecimalType};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{invalid_input_types_error, plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatePart;

impl FunctionInfo for DatePart {
    fn name(&self) -> &'static str {
        "date_part"
    }

    fn signatures(&self) -> &[Signature] {
        // TODO: Specific docs for each.
        const DOC: &Documentation = &Documentation {
            category: Category::Date,
            description: "Get a subfield.",
            arguments: &["part", "date"],
            example: Some(Example {
                example: "date_part('day', DATE '2024-12-17')",
                output: "17.000", // TODO: Gotta fix the trailing zeros.
            }),
        };

        &[
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Date32],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Date64],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: Some(DOC),
            },
            Signature {
                positional_args: &[DataTypeId::Utf8, DataTypeId::Timestamp],
                variadic_arg: None,
                return_type: DataTypeId::Decimal64,
                doc: Some(DOC),
            },
        ]
    }
}

impl ScalarFunction for DatePart {
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
        let part = ConstFold::rewrite(table_list, inputs[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;

        let part = part.parse::<ast::DatePart>()?;
        let part = convert_ast_date_part(part);

        match &datatypes[1] {
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_) => {
                Ok(PlannedScalarFunction {
                    function: Box::new(*self),
                    return_type: DataType::Decimal64(DecimalTypeMeta::new(
                        Decimal64Type::MAX_PRECISION,
                        Decimal64Type::DEFAULT_SCALE,
                    )),
                    inputs,
                    function_impl: Box::new(DatePartImpl { part }),
                })
            }
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatePartImpl {
    part: date::DatePart,
}

impl ScalarFunctionImpl for DatePartImpl {
    fn execute(&self, inputs: &[&Array2]) -> Result<Array2> {
        // First input ignored (the constant "part" to extract)
        extract_date_part(self.part, inputs[1])
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

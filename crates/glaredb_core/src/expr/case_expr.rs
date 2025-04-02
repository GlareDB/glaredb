use std::fmt;

use glaredb_error::{DbError, Result};

use super::Expression;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::ScalarValue;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::expr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WhenThen {
    pub when: Expression,
    pub then: Expression,
}

impl fmt::Display for WhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

impl ContextDisplay for WhenThen {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "WHEN {} THEN {}",
            ContextDisplayWrapper::with_mode(&self.when, mode),
            ContextDisplayWrapper::with_mode(&self.then, mode),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CaseExpr {
    pub cases: Vec<WhenThen>,
    pub else_expr: Box<Expression>,
    pub datatype: DataType,
}

impl CaseExpr {
    pub fn try_new(mut cases: Vec<WhenThen>, else_expr: Option<Box<Expression>>) -> Result<Self> {
        // Find the data type of this expression.
        let mut datatype = DataType::Null;
        for case in &cases {
            let case_datatype = case.then.datatype()?;
            if case_datatype.is_null() {
                continue;
            }
            if datatype.is_null() {
                datatype = case_datatype;
                continue;
            }

            // TODO: We should be increasing the domain of the data type as much
            // as we can.
            if case_datatype != datatype {
                return Err(DbError::new(format!(
                    "Case expression produces two different types: {} and {}",
                    datatype, case_datatype
                )));
            }
        }

        // Now check that all 'then' expression return the right type, casting
        // if needed.
        //
        // TODO: This really only casts nulls for now. We need to change the
        // above loop in order for this to do more than that.
        for case in &mut cases {
            let case_datatype = case.then.datatype()?;
            if case_datatype != datatype {
                // Need to cast.
                case.then.replace_with(|then| {
                    let cast = expr::cast(then, datatype.clone())?;
                    Ok(cast.into())
                })?;
            }
        }

        let else_expr = match else_expr {
            Some(expr) => {
                if expr.datatype()? != datatype {
                    // Need to cast.
                    let cast = expr::cast(*expr, datatype.clone())?;
                    Box::new(cast.into())
                } else {
                    // Return as-is
                    expr
                }
            }
            None => {
                // No "else" given, create a typed null expression with the same
                // datatype as the output of this expression.
                Box::new(expr::cast(expr::lit(ScalarValue::Null), datatype.clone())?.into())
            }
        };

        Ok(CaseExpr {
            cases,
            else_expr,
            datatype,
        })
    }
}

impl ContextDisplay for CaseExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{} ", ContextDisplayWrapper::with_mode(case, mode),)?;
        }

        write!(
            f,
            "ELSE {}",
            ContextDisplayWrapper::with_mode(self.else_expr.as_ref(), mode),
        )?;

        Ok(())
    }
}

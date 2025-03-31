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
    pub fn try_new(cases: Vec<WhenThen>, else_expr: Option<Box<Expression>>) -> Result<Self> {
        let mut case_iter = cases.iter();
        let datatype = match case_iter.next() {
            Some(case) => case.then.datatype()?,
            None => {
                return Err(DbError::new(
                    "Case expression must have at least one condition",
                ));
            }
        };

        for case in case_iter {
            let next_datatype = case.then.datatype()?;
            // TODO: Union or cast.
            if next_datatype != datatype {
                return Err(DbError::new(format!(
                    "Case expression produces two different types: {} and {}",
                    datatype, next_datatype
                )));
            }
        }

        let else_expr = match else_expr {
            Some(expr) => expr,
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

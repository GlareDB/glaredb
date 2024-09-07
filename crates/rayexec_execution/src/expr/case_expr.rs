use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use crate::logical::binder::bind_context::BindContext;

use super::Expression;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct WhenThen {
    pub when: Expression,
    pub then: Expression,
}

impl fmt::Display for WhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpr {
    pub cases: Vec<WhenThen>,
    pub else_expr: Option<Box<Expression>>,
}

impl CaseExpr {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        let mut case_iter = self.cases.iter();
        let datatype = match case_iter.next() {
            Some(case) => case.then.datatype(bind_context)?,
            None => {
                return Err(RayexecError::new(
                    "Case expression must have at least one condition",
                ))
            }
        };

        for case in case_iter {
            let next_datatype = case.then.datatype(bind_context)?;
            // TODO: Union or cast.
            if next_datatype != datatype {
                return Err(RayexecError::new(format!(
                    "Case expression produces two different types: {} and {}",
                    datatype, next_datatype
                )));
            }
        }

        Ok(datatype)
    }
}

impl fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{}", case)?;
        }

        if let Some(else_expr) = self.else_expr.as_ref() {
            write!(f, "ELSE {}", else_expr)?;
        }

        Ok(())
    }
}

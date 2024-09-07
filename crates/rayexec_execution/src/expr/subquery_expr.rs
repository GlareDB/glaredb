use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use crate::logical::binder::{
    bind_context::{BindContext, BindScopeRef},
    bind_query::BoundQuery,
};
use std::fmt;

use super::Expression;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubqueryType {
    Scalar,
    Exists { negated: bool },
    Any,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryExpr {
    pub bind_idx: BindScopeRef,
    pub subquery: Box<BoundQuery>,
    pub subquery_type: SubqueryType,
    pub return_type: DataType,

    /// Optional operator for ANY/IN/ALL subqueries
    ///
    /// ... WHERE x > ALL (<subquery>) ...
    pub operator: Option<Box<Expression>>,
}

impl SubqueryExpr {
    pub fn has_correlations(&self, bind_context: &BindContext) -> Result<bool> {
        let cols = bind_context.correlated_columns(self.bind_idx)?;
        Ok(!cols.is_empty())
    }
}

impl fmt::Display for SubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(operator) = &self.operator {
            write!(f, "{operator} ")?;
        }

        match self.subquery_type {
            SubqueryType::Scalar => (),
            SubqueryType::Exists { negated: false } => write!(f, "EXISTS ")?,
            SubqueryType::Exists { negated: true } => write!(f, "NOT EXISTS ")?,
            SubqueryType::Any => write!(f, "ANY ")?,
        }

        write!(f, "<subquery>")
    }
}

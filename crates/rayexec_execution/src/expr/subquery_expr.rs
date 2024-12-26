use std::fmt;
use std::hash::Hash;

use crate::arrays::datatype::DataType;
use rayexec_error::Result;

use super::comparison_expr::ComparisonOperator;
use super::Expression;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::bind_query::BoundQuery;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubqueryType {
    Scalar,
    Exists {
        negated: bool,
    },
    Any {
        /// Expression for ANY/IN/ALL subqueries
        ///
        /// ... WHERE <expr> > ALL (<subquery>) ...
        expr: Box<Expression>,
        /// The comparison operator to use.
        op: ComparisonOperator,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubqueryExpr {
    pub bind_idx: BindScopeRef,
    pub subquery: Box<BoundQuery>,
    pub subquery_type: SubqueryType,
    pub return_type: DataType,
}

impl SubqueryExpr {
    pub fn has_correlations(&self, bind_context: &BindContext) -> Result<bool> {
        let cols = bind_context.correlated_columns(self.bind_idx)?;
        Ok(!cols.is_empty())
    }
}

impl ContextDisplay for SubqueryExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match &self.subquery_type {
            SubqueryType::Scalar => (),
            SubqueryType::Exists { negated: false } => write!(f, "EXISTS ")?,
            SubqueryType::Exists { negated: true } => write!(f, "NOT EXISTS ")?,
            SubqueryType::Any { expr, op } => write!(
                f,
                "{} {} ANY ",
                ContextDisplayWrapper::with_mode(expr.as_ref(), mode),
                op
            )?,
        }

        write!(f, "<subquery>")
    }
}

// Purposely skips the bound query part, eq impl will check it.
impl Hash for SubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bind_idx.hash(state);
        self.subquery_type.hash(state);
        self.return_type.hash(state);
    }
}

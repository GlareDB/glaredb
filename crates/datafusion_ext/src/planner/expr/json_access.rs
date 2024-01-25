use datafusion::common::{not_impl_err, DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::sql::sqlparser::ast::JsonOperator;

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    pub(crate) fn parse_sql_json_access(&self, op: JsonOperator) -> Result<Operator> {
        match op {
            JsonOperator::AtArrow => Ok(Operator::AtArrow),
            JsonOperator::ArrowAt => Ok(Operator::ArrowAt),
            _ => not_impl_err!("Unsupported SQL json operator {op:?}"),
        }
    }
}

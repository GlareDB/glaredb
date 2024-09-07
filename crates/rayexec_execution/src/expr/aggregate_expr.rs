use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;
use std::fmt;

use crate::{
    functions::aggregate::PlannedAggregateFunction, logical::binder::bind_context::BindContext,
};

use super::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    /// The function.
    pub agg: Box<dyn PlannedAggregateFunction>,
    /// Input expressions to the aggragate.
    pub inputs: Vec<Expression>,
    /// Optional filter to the aggregate.
    pub filter: Option<Box<Expression>>,
}

impl AggregateExpr {
    pub fn datatype(&self, _bind_context: &BindContext) -> Result<DataType> {
        Ok(self.agg.return_type())
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.agg.aggregate_function().name())?;
        let inputs = self
            .inputs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "({})", inputs)?;

        if let Some(filter) = self.filter.as_ref() {
            write!(f, "FILTER (WHERE {})", filter)?;
        }

        Ok(())
    }
}

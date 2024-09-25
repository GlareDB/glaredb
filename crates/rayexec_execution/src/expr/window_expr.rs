use fmtutil::IntoDisplayableSlice;

use crate::functions::aggregate::PlannedAggregateFunction;
use std::fmt;

use super::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowExpr {
    pub agg: Box<dyn PlannedAggregateFunction>,
    pub inputs: Vec<Expression>,
    pub filter: Box<Expression>,
    pub partition_by: Vec<Expression>,
}

impl fmt::Display for WindowExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Other fields
        write!(
            f,
            "{}({})",
            self.agg.aggregate_function().name(),
            self.inputs.display_as_list()
        )
    }
}

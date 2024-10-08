use std::fmt;

use fmtutil::IntoDisplayableSlice;

use super::Expression;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::aggregate::PlannedAggregateFunction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowExpr {
    pub agg: Box<dyn PlannedAggregateFunction>,
    pub inputs: Vec<Expression>,
    pub filter: Box<Expression>,
    pub partition_by: Vec<Expression>,
}

impl ContextDisplay for WindowExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let inputs: Vec<_> = self
            .inputs
            .iter()
            .map(|expr| ContextDisplayWrapper::with_mode(expr, mode))
            .collect();
        write!(
            f,
            "{}({})",
            self.agg.aggregate_function().name(),
            inputs.display_as_list()
        )
    }
}

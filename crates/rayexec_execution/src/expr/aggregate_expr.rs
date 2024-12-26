use std::fmt;

use rayexec_bullet::datatype::DataTypeOld;
use rayexec_error::Result;

use super::Expression;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::aggregate::PlannedAggregateFunction;
use crate::logical::binder::bind_context::BindContext;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    /// The function.
    pub agg: PlannedAggregateFunction,
    /// Optional filter to the aggregate.
    pub filter: Option<Box<Expression>>,
    /// If the inputs should be deduplicated.
    pub distinct: bool,
}

impl AggregateExpr {
    pub fn datatype(&self, _bind_context: &BindContext) -> Result<DataTypeOld> {
        Ok(self.agg.return_type.clone())
    }
}

impl ContextDisplay for AggregateExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}", self.agg.function.name())?;
        let inputs = self
            .agg
            .inputs
            .iter()
            .map(|e| ContextDisplayWrapper::with_mode(e, mode).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        if self.distinct {
            write!(f, "(DISTINCT {})", inputs)?;
        } else {
            write!(f, "({})", inputs)?;
        }

        if let Some(filter) = self.filter.as_ref() {
            write!(
                f,
                "FILTER (WHERE {})",
                ContextDisplayWrapper::with_mode(filter.as_ref(), mode)
            )?;
        }

        Ok(())
    }
}

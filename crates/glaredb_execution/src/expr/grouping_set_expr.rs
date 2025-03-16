use std::fmt;

use super::Expression;
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::util::fmt::displayable::IntoDisplayableSlice;

/// Expression that corresponds to a GROUPING call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupingSetExpr {
    pub inputs: Vec<Expression>,
}

impl GroupingSetExpr {
    pub fn datatype(&self) -> DataType {
        DataType::Int64
    }
}

impl ContextDisplay for GroupingSetExpr {
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
        write!(f, "GROUPING({})", inputs.display_as_list())
    }
}

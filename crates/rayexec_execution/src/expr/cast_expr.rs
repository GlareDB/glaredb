use std::fmt;

use super::Expression;
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub to: DataType,
    pub expr: Box<Expression>,
}

impl ContextDisplay for CastExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "CAST({} TO {})",
            ContextDisplayWrapper::with_mode(self.expr.as_ref(), mode),
            self.to
        )
    }
}

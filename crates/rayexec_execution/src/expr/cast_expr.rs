use rayexec_bullet::datatype::DataType;
use std::fmt;

use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};

use super::Expression;

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

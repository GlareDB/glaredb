use std::fmt;

use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use super::Expression;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::logical::binder::bind_context::BindContext;

// TODO: Include recurse depth.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnnestExpr {
    /// Expression being unnested.
    pub expr: Box<Expression>,
}

impl UnnestExpr {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        let child_datatype = self.expr.datatype(bind_context)?;

        match child_datatype {
            DataType::Null => Ok(DataType::Null),
            DataType::List(list) => Ok(list.datatype.as_ref().clone()),
            other => Err(RayexecError::new(format!(
                "Unsupported datatype for UNNEST: {other}"
            ))),
        }
    }
}

impl ContextDisplay for UnnestExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(
            f,
            "UNNEST({})",
            ContextDisplayWrapper::with_mode(self.expr.as_ref(), mode)
        )
    }
}

use std::fmt;

use glaredb_error::{DbError, Result};

use super::Expression;
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};

// TODO: Include recurse depth.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnnestExpr {
    /// Expression being unnested.
    pub expr: Box<Expression>,
}

impl UnnestExpr {
    pub fn datatype(&self) -> Result<DataType> {
        let child_datatype = self.expr.datatype()?;

        match child_datatype {
            DataType::Null => Ok(DataType::Null),
            DataType::List(list) => Ok(list.datatype.as_ref().clone()),
            other => Err(DbError::new(format!(
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

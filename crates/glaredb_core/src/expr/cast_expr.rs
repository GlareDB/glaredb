use std::fmt;

use glaredb_error::{DbError, Result};

use super::Expression;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::cast::builtin::BUILTIN_CAST_FUNCTION_SETS;
use crate::functions::cast::{CastFunctionSet, PlannedCastFunction, RawCastFunction};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub to: DataType,
    pub expr: Box<Expression>,
    pub cast_function: PlannedCastFunction,
}

impl CastExpr {
    /// Create a new cast expression using the default cast rules.
    pub fn new_using_default_casts(expr: impl Into<Expression>, to: DataType) -> Result<Self> {
        let expr = expr.into();
        let src = expr.datatype()?;

        let src_id = src.datatype_id();
        let target_id = to.datatype_id();

        let cast_set = find_cast_function_set(target_id)?;
        let cast_fn = find_cast_function(cast_set, src_id)?;
        let bind_state = cast_fn.call_bind(&src, &to)?;

        let planned = PlannedCastFunction {
            name: cast_set.name,
            raw: *cast_fn,
            state: bind_state,
        };

        Ok(CastExpr {
            to,
            expr: Box::new(expr),
            cast_function: planned,
        })
    }
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

fn find_cast_function_set(target: DataTypeId) -> Result<&'static CastFunctionSet> {
    for cast_set in BUILTIN_CAST_FUNCTION_SETS {
        if cast_set.target == target {
            return Ok(cast_set);
        }
    }

    Err(DbError::new(format!(
        "Unable to find cast function to handle target type: {target}"
    )))
}

fn find_cast_function(set: &CastFunctionSet, src: DataTypeId) -> Result<&RawCastFunction> {
    for cast_fn in set.functions {
        if cast_fn.src == src {
            return Ok(cast_fn);
        }
    }

    Err(DbError::new(format!(
        "Cast function '{}' cannot handle source type {}",
        set.name, src,
    )))
}

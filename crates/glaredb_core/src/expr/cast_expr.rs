use std::fmt;

use glaredb_error::{DbError, Result};

use super::Expression;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::cast::builtin::BUILTIN_CAST_FUNCTION_SETS;
use crate::functions::cast::{CastFlatten, CastFunctionSet, PlannedCastFunction, RawCastFunction};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CastExpr {
    pub to: DataType,
    pub expr: Box<Expression>,
    pub cast_function: PlannedCastFunction,
}

impl CastExpr {
    /// Create a new cast expression using the default cast rules.
    pub fn new_using_default_casts(expr: impl Into<Expression>, to: DataType) -> Result<Self> {
        // First make sure we even have a function set for casting to the target
        // type.
        let target_id = to.datatype_id();
        let cast_set = find_cast_function_set(target_id).ok_or_else(|| {
            DbError::new(format!(
                "Unable to find cast function to handle target type: {target_id}"
            ))
        })?;

        let expr = expr.into();

        // Now if the existing expression is already a CAST, try to see if we
        // can drop the inner cast by casting directly from the child type to
        // the target.
        if let Expression::Cast(existing_cast) = &expr {
            let child = &existing_cast.expr;
            let child_datatype = child.datatype()?;
            if let Some(cast_fn) = find_cast_function(cast_set, child_datatype.datatype_id()) {
                // It's valid to cast directly from the child to target.
                //
                // However, we need to check if this cast is "safe" to do
                // automatically.
                if matches!(cast_fn.flatten, CastFlatten::Safe) {
                    // Direct cast is safe to do.
                    let child = match expr {
                        Expression::Cast(cast) => cast.expr,
                        _ => unreachable!("expr variant checked in outer if statement"),
                    };

                    let bind_state = cast_fn.call_bind(&child_datatype, &to)?;
                    let planned = PlannedCastFunction {
                        name: cast_set.name,
                        raw: *cast_fn,
                        state: bind_state,
                    };

                    return Ok(CastExpr {
                        to,
                        expr: child,
                        cast_function: planned,
                    });
                }

                // Direct cast is not safe to do. Fall back to normal casting...
            }
            // No direct cast function, fall back to normal casting...
        }

        // Otherwise just wrap unconditionally in a new cast.
        let src_datatype = expr.datatype()?;
        let cast_fn =
            find_cast_function(cast_set, src_datatype.datatype_id()).ok_or_else(|| {
                DbError::new(format!(
                    "Cast function '{}' cannot handle source type {}",
                    cast_set.name, src_datatype,
                ))
            })?;

        let bind_state = cast_fn.call_bind(&src_datatype, &to)?;

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

fn find_cast_function_set(target: DataTypeId) -> Option<&'static CastFunctionSet> {
    for cast_set in BUILTIN_CAST_FUNCTION_SETS {
        if cast_set.target == target {
            return Some(cast_set);
        }
    }
    None
}

fn find_cast_function(set: &CastFunctionSet, src: DataTypeId) -> Option<&RawCastFunction> {
    for cast_fn in set.functions {
        if cast_fn.src == src {
            return Some(cast_fn);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr;

    #[test]
    fn no_flatten_unsafe() {
        let cast = CastExpr::new_using_default_casts(
            CastExpr::new_using_default_casts(expr::lit("123456789e-1234"), DataType::Float32)
                .unwrap(),
            DataType::Int64,
        )
        .unwrap();

        assert!(matches!(cast.expr.as_ref(), Expression::Cast(_)));
    }

    #[test]
    fn flatten_safe() {
        let cast = CastExpr::new_using_default_casts(
            CastExpr::new_using_default_casts(expr::lit(14_i16), DataType::Int32).unwrap(),
            DataType::Int64,
        )
        .unwrap();

        assert_eq!(Expression::from(expr::lit(14_i16)), *cast.expr);
        assert_eq!(DataType::Int64, cast.to);
    }
}

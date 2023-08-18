//! AST visitors for preprocessing queries before planning.
use crate::context::SessionContext;
use datafusion::sql::sqlparser::ast::{self, VisitMut, VisitorMut};
use std::ops::ControlFlow;

#[derive(Debug, thiserror::Error)]
pub enum PreprocessError {
    #[error("Relation '{0}' does not exist")]
    MissingRelation(String),

    #[error("Casting expressions to regclass unsupported")]
    ExprUnsupportedRegclassCast,
}

pub fn preprocess<V>(statement: &mut ast::Statement, visitor: &mut V) -> Result<(), PreprocessError>
where
    V: VisitorMut<Break = PreprocessError>,
{
    match statement.visit(visitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(e) => Err(e),
    }
}

/// Replace `CAST('table_name' as REGCLASS)` expressions with the oid of the
/// table.
pub struct CastRegclassReplacer<'a> {
    pub ctx: &'a SessionContext,
}

impl<'a> ast::VisitorMut for CastRegclassReplacer<'a> {
    type Break = PreprocessError;

    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        fn find_oid(ctx: &SessionContext, _rel: &str) -> Option<u32> {
            let _catalog = ctx.get_session_catalog();
            for _schema in ctx.implicit_search_paths() {
                // TODO
                todo!("IS IT POSSIBLE TO DO THIS WITHOUT THE CATALOG?")
                // if let Some(ent) = catalog.resolve_entry(DEFAULT_CATALOG, &schema, rel).await {
                //     // Table found.
                //     return Some(ent.get_meta().id);
                // }
            }
            None
        }

        let replace_expr = match expr {
            ast::Expr::Cast {
                expr: inner_expr,
                data_type: ast::DataType::Regclass,
            } => {
                if let ast::Expr::Value(ast::Value::SingleQuotedString(relation)) = &**inner_expr {
                    match find_oid(self.ctx, relation) {
                        Some(oid) => ast::Expr::Value(ast::Value::Number(oid.to_string(), false)),
                        None => {
                            return ControlFlow::Break(PreprocessError::MissingRelation(
                                relation.clone(),
                            ))
                        }
                    }
                } else {
                    // We don't currently support any other casts to regclass.
                    return ControlFlow::Break(PreprocessError::ExprUnsupportedRegclassCast);
                }
            }
            _ => return ControlFlow::Continue(()), // Nothing to do.
        };

        *expr = replace_expr;

        ControlFlow::Continue(())
    }
}

/// Replace `E'my_string'` with `"my_string"`.
///
/// TODO: Datafusion should be updated to properly handle escaped strings. This
/// is just a quick hack.
pub struct EscapedStringToDoubleQuoted;

impl ast::VisitorMut for EscapedStringToDoubleQuoted {
    type Break = PreprocessError;

    fn post_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        if let ast::Expr::Value(ast::Value::EscapedStringLiteral(s)) = expr {
            *expr = ast::Expr::Value(ast::Value::DoubleQuotedString(std::mem::take(s)));
        }
        ControlFlow::Continue(())
    }
}

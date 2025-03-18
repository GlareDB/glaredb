pub mod bind_from;
pub mod bind_group_by;
pub mod bind_having;
pub mod bind_modifier;
pub mod bind_select;
pub mod bind_select_list;
pub mod bind_setop;
pub mod bind_values;

pub mod select_expr_expander;
pub mod select_list;

use bind_select::{BoundSelect, SelectBinder};
use bind_setop::{BoundSetOp, SetOpBinder};
use bind_values::{BoundValues, ValuesBinder};
use glaredb_error::{not_implemented, DbError, Result};
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::table_list::TableRef;
use crate::logical::binder::bind_context::BoundCte;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundQuery {
    Select(BoundSelect),
    Setop(BoundSetOp),
    Values(BoundValues),
}

impl BoundQuery {
    pub fn output_table_ref(&self) -> TableRef {
        match self {
            BoundQuery::Select(select) => match &select.select_list.output {
                Some(pruned) => pruned.table,
                None => select.select_list.projections_table,
            },
            Self::Setop(setop) => setop.setop_table,
            BoundQuery::Values(values) => values.expressions_table,
        }
    }
}

#[derive(Debug)]
pub struct QueryBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> QueryBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        QueryBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        query: ast::QueryNode<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        if let Some(ctes) = query.ctes {
            self.bind_ctes(bind_context, ctes)?;
        }

        let body = self.bind_body(bind_context, query.body, query.order_by, query.limit)?;

        Ok(body)
    }

    pub fn bind_body(
        &self,
        bind_context: &mut BindContext,
        body: ast::QueryNodeBody<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        match body {
            ast::QueryNodeBody::Select(select) => {
                let binder = SelectBinder::new(self.current, self.resolve_context);
                let select = binder.bind(bind_context, *select, order_by, limit)?;
                Ok(BoundQuery::Select(select))
            }
            ast::QueryNodeBody::Nested(query) => self.bind(bind_context, *query),
            ast::QueryNodeBody::Values(values) => {
                let binder = ValuesBinder::new(self.current, self.resolve_context);
                let values = binder.bind(bind_context, values, order_by, limit)?;
                Ok(BoundQuery::Values(values))
            }
            ast::QueryNodeBody::Set(setop) => {
                let binder = SetOpBinder::new(self.current, self.resolve_context);
                let setop = binder.bind(bind_context, setop, order_by, limit)?;
                Ok(BoundQuery::Setop(setop))
            }
        }
    }

    fn bind_ctes(
        &self,
        bind_context: &mut BindContext,
        ctes: ast::CommonTableExprs<ResolvedMeta>,
    ) -> Result<()> {
        if ctes.recursive {
            not_implemented!("recursive CTEs");
        }

        for cte in ctes.ctes {
            self.bind_cte(bind_context, cte)?
        }

        Ok(())
    }

    fn bind_cte(
        &self,
        bind_context: &mut BindContext,
        cte: ast::CommonTableExpr<ResolvedMeta>,
    ) -> Result<()> {
        let nested = bind_context.new_child_scope(self.current);
        let binder = QueryBinder::new(nested, self.resolve_context);
        let bound = binder.bind(bind_context, *cte.body)?;

        let mut names = Vec::new();
        let mut types = Vec::new();
        for table in bind_context.iter_tables_in_scope(nested)? {
            types.extend(table.column_types.iter().cloned());
            names.extend(table.column_names.iter().cloned());
        }

        // Sets alias where cte is defined
        //
        // WITH my_cte(alias1, alias2) AS ...
        if let Some(col_aliases) = &cte.column_aliases {
            if col_aliases.len() > names.len() {
                return Err(DbError::new(format!(
                    "Expected at most {} column aliases, received {}",
                    names.len(),
                    col_aliases.len()
                )));
            }

            for (idx, col_alias) in col_aliases.iter().enumerate() {
                names[idx] = col_alias.as_normalized_string();
            }
        }

        let cte = BoundCte {
            bind_scope: nested,
            materialized: cte.materialized,
            name: cte.alias.into_normalized_string(),
            column_names: names,
            column_types: types,
            bound: Box::new(bound),
            mat_ref: None,
        };

        // Note that we bind the CTE in a nested scope, but add it to the
        // current scope so that it's visible to all child scopes of current.
        //
        // This allows for CTEs to reference previously defined CTEs, and we can
        // avoid accidentally clobbering columns that are already in scope for
        // the curent scope.
        bind_context.add_cte(self.current, cte)?;

        Ok(())
    }
}

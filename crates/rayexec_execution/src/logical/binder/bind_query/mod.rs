pub mod bind_from;
pub mod bind_group_by;
pub mod bind_having;
pub mod bind_modifier;
pub mod bind_select;
pub mod bind_setop;
pub mod bind_values;

pub mod condition_extractor;
pub mod select_expr_expander;
pub mod select_list;

use bind_setop::{BoundSetOp, SetOpBinder};
use bind_values::{BoundValues, ValuesBinder};
use rayexec_error::Result;
use rayexec_parser::ast;

use crate::logical::resolver::{resolve_context::ResolveContext, ResolvedMeta};
use bind_select::{BoundSelect, SelectBinder};

use super::bind_context::{BindContext, BindScopeRef, TableRef};

#[derive(Debug, Clone, PartialEq)]
pub enum BoundQuery {
    Select(BoundSelect),
    Setop(BoundSetOp),
    Values(BoundValues),
}

impl BoundQuery {
    pub fn output_table_ref(&self) -> TableRef {
        match self {
            BoundQuery::Select(select) => match &select.select_list.pruned {
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
        self.bind_body(bind_context, query.body, query.order_by, query.limit)
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
}

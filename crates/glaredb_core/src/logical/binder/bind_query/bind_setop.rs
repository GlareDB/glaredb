use glaredb_error::{DbError, Result, not_implemented};
use glaredb_parser::ast;

use super::BoundQuery;
use super::bind_modifier::{BoundLimit, BoundOrderBy};
use super::bind_select_list::SelectListBinder;
use crate::functions::implicit::implicit_cast_score;
use crate::logical::binder::bind_context::{BindContext, BindScopeRef};
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::binder::bind_query::bind_modifier::ModifierBinder;
use crate::logical::binder::table_list::TableRef;
use crate::logical::logical_setop::SetOpKind;
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

/// Cast requirements for the set operation.
///
/// If a cast is required, a table ref is generated which should be used by the
/// casting projection during planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpCastRequirement {
    /// Need to cast the left side to match expected types.
    LeftNeedsCast(TableRef),
    /// Need to cast the right side to match expected types.
    RightNeedsCast(TableRef),
    /// Both sides need casting.
    BothNeedsCast {
        left_cast_ref: TableRef,
        right_cast_ref: TableRef,
    },
    /// No sides need casting.
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSetOp {
    pub left: Box<BoundQuery>,
    pub left_scope: BindScopeRef,
    pub right: Box<BoundQuery>,
    pub right_scope: BindScopeRef,
    pub setop_table: TableRef,
    pub kind: SetOpKind,
    pub all: bool,
    /// Bound ORDER BY.
    pub order_by: Option<BoundOrderBy>,
    /// Bound LIMIT.
    pub limit: Option<BoundLimit>,
    pub cast_req: SetOpCastRequirement,
}

#[derive(Debug)]
pub struct SetOpBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SetOpBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SetOpBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind(
        &self,
        bind_context: &mut BindContext,
        setop: ast::SetOp<ResolvedMeta>,
        order_by: Option<ast::OrderByModifier<ResolvedMeta>>,
        limit: ast::LimitModifier<ResolvedMeta>,
    ) -> Result<BoundSetOp> {
        let left_scope = bind_context.new_child_scope(self.current);
        // TODO: Make limit modifier optional.
        let left = QueryBinder::new(left_scope, self.resolve_context).bind_body(
            bind_context,
            *setop.left,
            None,
            ast::LimitModifier {
                limit: None,
                offset: None,
            },
        )?;

        let right_scope = bind_context.new_child_scope(self.current);
        let right = QueryBinder::new(right_scope, self.resolve_context).bind_body(
            bind_context,
            *setop.right,
            None,
            ast::LimitModifier {
                limit: None,
                offset: None,
            },
        )?;

        let mut left_types = Vec::new();
        let mut left_names = Vec::new();
        for table in bind_context.iter_tables_in_scope(left_scope)? {
            left_types.extend_from_slice(&table.column_types);
            left_names.extend_from_slice(&table.column_names);
        }

        let right_types: Vec<_> = bind_context
            .iter_tables_in_scope(right_scope)?
            .flat_map(|t| t.column_types.iter().cloned())
            .collect();

        // Determine output types of this node by comparing both sides, and
        // marking which side neds casting.
        let mut output_types = Vec::with_capacity(left_types.len());
        let mut left_needs_cast = false;
        let mut right_needs_cast = false;

        for (left, right) in left_types.into_iter().zip(right_types) {
            if left == right {
                // Nothing to do.
                output_types.push(left);
                continue;
            }

            let left_score = implicit_cast_score(right.datatype_id(), left.datatype_id());
            let right_score = implicit_cast_score(left.datatype_id(), right.datatype_id());

            if left_score.is_none() && right_score.is_none() {
                return Err(DbError::new(format!(
                    "Cannot find suitable cast type for {left} and {right}"
                )));
            }

            if left_score >= right_score {
                output_types.push(left);
                right_needs_cast = true;
            } else {
                output_types.push(right);
                left_needs_cast = true;
            }
        }

        let cast_req = match (left_needs_cast, right_needs_cast) {
            (true, true) => SetOpCastRequirement::BothNeedsCast {
                left_cast_ref: bind_context
                    .new_ephemeral_table_from_types("__generated_left", output_types.clone())?,
                right_cast_ref: bind_context
                    .new_ephemeral_table_from_types("__generated_right", output_types.clone())?,
            },
            (true, false) => SetOpCastRequirement::LeftNeedsCast(
                bind_context
                    .new_ephemeral_table_from_types("__generated_left", output_types.clone())?,
            ),
            (false, true) => SetOpCastRequirement::RightNeedsCast(
                bind_context
                    .new_ephemeral_table_from_types("__generated_right", output_types.clone())?,
            ),
            (false, false) => SetOpCastRequirement::None,
        };

        // Move output into scope.
        let table_ref = bind_context.push_table(self.current, None, output_types, left_names)?;

        // ORDER BY and LIMIT on output of the setop.
        let modifier_binder =
            ModifierBinder::new(vec![left_scope, right_scope], self.resolve_context);
        // TODO: This select list should be able to reference aliases in the output.
        // TODO: What do we do with distinct? Is None ok?
        let mut empty_select_list = SelectListBinder::new(self.current, self.resolve_context)
            .bind(bind_context, Vec::new(), None)?;
        let order_by = order_by
            .map(|order_by| {
                modifier_binder.bind_order_by(bind_context, &mut empty_select_list, order_by)
            })
            .transpose()?;
        let limit = modifier_binder.bind_limit(bind_context, limit)?;

        if !empty_select_list.appended.is_empty() {
            // Only support ordering by columns, no expressions beyond that yet.
            not_implemented!("ORDER BY expressions");
        }

        let kind = match setop.operation {
            ast::SetOperation::Union => SetOpKind::Union,
            ast::SetOperation::Except => SetOpKind::Except,
            ast::SetOperation::Intersect => SetOpKind::Intersect,
        };

        Ok(BoundSetOp {
            left: Box::new(left),
            left_scope,
            right: Box::new(right),
            right_scope,
            setop_table: table_ref,
            kind,
            all: setop.all,
            order_by,
            limit,
            cast_req,
        })
    }
}

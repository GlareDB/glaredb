use crate::{
    expr::{cast_expr::CastExpr, column_expr::ColumnExpr, Expression},
    logical::{
        binder::{
            bind_context::{BindContext, BindScopeRef, Table, TableRef},
            bind_query::bind_setop::{BoundSetOp, SetOpCastRequirement},
        },
        logical_project::LogicalProject,
        logical_setop::LogicalSetop,
        operator::{LocationRequirement, LogicalOperator, Node},
        planner::plan_query::QueryPlanner,
    },
};
use rayexec_error::{RayexecError, Result};

#[derive(Debug)]
pub struct SetOpPlanner;

impl SetOpPlanner {
    pub fn plan(
        &self,
        bind_context: &mut BindContext,
        setop: BoundSetOp,
    ) -> Result<LogicalOperator> {
        let mut left = QueryPlanner.plan(bind_context, *setop.left)?;
        let mut right = QueryPlanner.plan(bind_context, *setop.right)?;

        match setop.cast_req {
            SetOpCastRequirement::LeftNeedsCast(left_cast_ref) => {
                left = self.wrap_cast(bind_context, left, setop.left_scope, left_cast_ref)?;
            }
            SetOpCastRequirement::RightNeedsCast(right_cast_ref) => {
                right = self.wrap_cast(bind_context, right, setop.right_scope, right_cast_ref)?;
            }
            SetOpCastRequirement::BothNeedsCast {
                left_cast_ref,
                right_cast_ref,
            } => {
                left = self.wrap_cast(bind_context, left, setop.left_scope, left_cast_ref)?;
                right = self.wrap_cast(bind_context, right, setop.right_scope, right_cast_ref)?;
            }
            SetOpCastRequirement::None => (),
        }

        Ok(LogicalOperator::SetOp(Node {
            node: LogicalSetop {
                kind: setop.kind,
                all: setop.all,
                table_ref: setop.setop_table,
            },
            location: LocationRequirement::Any,
            children: vec![left, right],
        }))
    }

    fn wrap_cast(
        &self,
        bind_context: &BindContext,
        orig_plan: LogicalOperator,
        orig_scope: BindScopeRef,
        cast_table_ref: TableRef,
    ) -> Result<LogicalOperator> {
        let orig_table = self.get_original_table(bind_context, orig_scope)?;

        Ok(LogicalOperator::Project(Node {
            node: LogicalProject {
                projections: self.generate_cast_expressions(
                    bind_context,
                    orig_table,
                    cast_table_ref,
                )?,
                projection_table: cast_table_ref,
            },
            location: LocationRequirement::Any,
            children: vec![orig_plan],
        }))
    }

    fn get_original_table<'a>(
        &self,
        bind_context: &'a BindContext,
        scope_ref: BindScopeRef,
    ) -> Result<&'a Table> {
        let mut iter = bind_context.iter_tables(scope_ref)?;
        let table = match iter.next() {
            Some(table) => table,
            None => return Err(RayexecError::new("No table is scope")),
        };

        if iter.next().is_some() {
            // TODO: Is this possible?
            return Err(RayexecError::new("Too many tables in scope"));
        }

        Ok(table)
    }

    fn generate_cast_expressions(
        &self,
        bind_context: &BindContext,
        orig_table: &Table,
        cast_table_ref: TableRef,
    ) -> Result<Vec<Expression>> {
        let cast_table = bind_context.get_table(cast_table_ref)?;

        let mut cast_exprs = Vec::with_capacity(orig_table.column_types.len());

        for (idx, (orig_type, need_type)) in orig_table
            .column_types
            .iter()
            .zip(&cast_table.column_types)
            .enumerate()
        {
            let col_expr = Expression::Column(ColumnExpr {
                table_scope: orig_table.reference,
                column: idx,
            });

            if orig_type == need_type {
                // No cast needed, reference original table.
                cast_exprs.push(col_expr);
                continue;
            }

            cast_exprs.push(Expression::Cast(CastExpr {
                to: need_type.clone(),
                expr: Box::new(col_expr),
            }));
        }

        Ok(cast_exprs)
    }
}

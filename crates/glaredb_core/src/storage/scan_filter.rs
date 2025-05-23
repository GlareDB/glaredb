use std::fmt;

use glaredb_error::Result;

use super::projections::{ProjectedColumn, Projections};
use crate::arrays::scalar::ScalarValue;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::expr::Expression;
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFilter {
    /// The filtering expression.
    pub expression: Expression,
}

impl ScanFilter {
    pub fn plan(
        self,
        table_ref: TableRef,
        _bind_context: &BindContext,
        projections: &Projections,
        expr_planner: &PhysicalExpressionPlanner,
    ) -> Result<PhysicalScanFilter> {
        let column_refs = self.expression.get_column_references();
        let planned = expr_planner.plan_scalar(&[table_ref], &self.expression)?;

        let filter_type = match self.expression {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                match (cmp.left.as_ref(), cmp.right.as_ref()) {
                    (Expression::Column(_), right) if right.is_const_foldable() => {
                        let right = ConstFold::rewrite(*cmp.right)?.try_into_scalar()?;
                        PhysicalScanFilterType::ConstantEq(right)
                    }
                    (left, Expression::Column(_)) if left.is_const_foldable() => {
                        let left = ConstFold::rewrite(*cmp.left)?.try_into_scalar()?;
                        PhysicalScanFilterType::ConstantEq(left)
                    }
                    _ => PhysicalScanFilterType::Unknown,
                }
            }
            _ => PhysicalScanFilterType::Unknown,
        };

        let columns = column_refs
            .into_iter()
            .map(|col_ref| {
                // Note this will change when we filter on metadata. Those will
                // point to a separate table ref.
                debug_assert_eq!(table_ref, col_ref.table_scope);
                // The col ref exposed from the table scan is relative to the
                // output projections.
                let data_col = projections.data_indices()[col_ref.column];
                ProjectedColumn::Data(data_col)
            })
            .collect();

        Ok(PhysicalScanFilter {
            columns,
            filter: planned,
            filter_type,
        })
    }
}

impl ContextDisplay for ScanFilter {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.expression.fmt_using_context(mode, f)
    }
}

// TODO: At some point I want to expand this so that we can pre-plan some more
// expressions and execute those arbitrary expressions on ranges/values.
#[derive(Debug, Clone)]
pub struct PhysicalScanFilter {
    /// Which columns this filter applies to.
    pub columns: Vec<ProjectedColumn>,
    /// The planned scalar function for the expression.
    pub filter: PhysicalScalarExpression,
    /// The filter type.
    pub filter_type: PhysicalScanFilterType,
}

#[derive(Debug, Clone)]
pub enum PhysicalScanFilterType {
    /// Filter represents a column equality with a constant value.
    ConstantEq(ScalarValue),
    /// Unknown filter type.
    ///
    /// Could still be valid, we just don't know what to do with it.
    Unknown,
}

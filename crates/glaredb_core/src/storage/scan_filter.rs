use std::fmt;

use glaredb_error::{DbError, Result};

use super::projections::ProjectedColumn;
use crate::arrays::scalar::ScalarValue;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::expr::comparison_expr::ComparisonOperator;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::expr::{self, Expression};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

#[derive(Debug, Clone, PartialEq)]
pub struct ScanFilter {
    /// The filtering expression.
    pub expression: Expression,
}

impl ScanFilter {
    pub fn plan(
        self,
        table_ref: TableRef,
        _bind_context: &BindContext,
        expr_planner: &PhysicalExpressionPlanner,
    ) -> Result<PhysicalScanFilter> {
        let column_refs = self.expression.get_column_references();
        let planned = expr_planner.plan_scalar(&[table_ref], &self.expression)?;

        let filter_type = match self.expression {
            Expression::Comparison(cmp) if cmp.op == ComparisonOperator::Eq => {
                let left_refs = cmp.left.get_column_references();
                let right_refs = cmp.right.get_column_references();

                match (left_refs.is_empty(), right_refs.is_empty()) {
                    (true, false) => {
                        // Left is constant.
                        let left = ConstFold::rewrite(*cmp.left)?;
                        PhysicalScanFilterType::Eq(PhysicalScanFilterEq {
                            constant: left.try_into_scalar()?,
                        })
                    }
                    (false, true) => {
                        // Right is constant.
                        let right = ConstFold::rewrite(*cmp.right)?;
                        PhysicalScanFilterType::Eq(PhysicalScanFilterEq {
                            constant: right.try_into_scalar()?,
                        })
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
                ProjectedColumn::Data(col_ref.column)
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

#[derive(Debug)]
pub struct PhysicalScanFilter {
    /// Which columns this filter applies to.
    pub columns: Vec<ProjectedColumn>,
    /// The planned scalar function for the expression.
    pub filter: PhysicalScalarExpression,
    /// The filter type.
    pub filter_type: PhysicalScanFilterType,
}

#[derive(Debug)]
pub enum PhysicalScanFilterType {
    /// Filter represents a column equality with a constant value.
    Eq(PhysicalScanFilterEq),
    /// Unknown filter type.
    ///
    /// Could still be valid, we just don't know what to do with it.
    Unknown,
}

#[derive(Debug)]
pub struct PhysicalScanFilterEq {
    /// The constant we're comparing with.
    pub constant: ScalarValue,
}

impl PhysicalScanFilterEq {
    /// Compares the constant with the provided min/max values to see if it's
    /// contained within the range.
    pub fn is_in_range(
        &self,
        min: impl Into<ScalarValue>,
        max: impl Into<ScalarValue>,
    ) -> Result<bool> {
        let min_gt_constant: Expression =
            expr::gt(expr::lit(min), expr::lit(self.constant.clone()))?.into();

        let min_is_gt_constant = match ConstFold::rewrite(min_gt_constant)? {
            Expression::Literal(lit) => lit.literal.try_as_bool()?,
            other => {
                return Err(DbError::new(format!(
                    "Expected literal after const folding, got: {other}",
                )));
            }
        };

        if min_is_gt_constant {
            // Not in range, min is greater than the constant.
            return Ok(false);
        }

        let max_lt_constant: Expression =
            expr::lt(expr::lit(max), expr::lit(self.constant.clone()))?.into();

        let max_is_lt_constant = match ConstFold::rewrite(max_lt_constant)? {
            Expression::Literal(lit) => lit.literal.try_as_bool()?,
            other => {
                return Err(DbError::new(format!(
                    "Expected literal after const folding, got: {other}",
                )));
            }
        };

        if max_is_lt_constant {
            // Not in range, max is less than constant.
            return Ok(false);
        }

        // Constant is in range.
        Ok(true)
    }
}

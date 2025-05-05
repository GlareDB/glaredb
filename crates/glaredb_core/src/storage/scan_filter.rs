use glaredb_error::{DbError, Result};

use super::projections::ProjectedColumn;
use crate::arrays::scalar::ScalarValue;
use crate::expr::physical::PhysicalScalarExpression;
use crate::expr::{self, Expression};
use crate::functions::scalar::builtin::comparison::FUNCTION_SET_LT;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

#[derive(Debug, Clone, PartialEq)]
pub struct ScanFilter {
    /// The filtering expression.
    pub expression: Expression,
}

#[derive(Debug)]
pub struct PhysicalScanFilter {
    /// Which column this filter applies to.
    pub column: ProjectedColumn,
    /// The planned scalar function for the expression.
    pub filter: PhysicalScalarExpression,
}

#[derive(Debug)]
pub enum PhysicalScanFilterType {
    /// Filter represents a column equality with a constant value.
    Eq(PhysicalScanFilterEq),
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

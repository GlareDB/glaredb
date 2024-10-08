use rayexec_bullet::scalar::OwnedScalarValue;

use crate::expr::comparison_expr::ComparisonOperator;

/// A simplified filter that can be pushed into a scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFilter {
    /// The column index this filter applies to.
    ///
    /// This is referencing a column prior to any projections being performed.
    pub column: usize,
    /// The filter itself.
    pub filter: ScanFilterType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanFilterType {
    ConstComparison {
        /// Operator used for comparing a column to a constant.
        ///
        /// This should be treated as if the column is on the left, and the
        /// constant on the right.
        op: ComparisonOperator,
        /// The value we're comparing to.
        constant: OwnedScalarValue,
    },
}

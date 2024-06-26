use rayexec_error::Result;

use crate::logical::operator::LogicalExpression;

/// Evaluate constant expressions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(dead_code)]
pub struct ConstEval {}

impl ConstEval {
    #[allow(dead_code)]
    pub fn fold(&self, _expr: LogicalExpression) -> Result<LogicalExpression> {
        unimplemented!()
    }
}

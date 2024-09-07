pub mod location;

use crate::logical::operator::LogicalOperator;
use location::LocationRule;
use rayexec_error::Result;

#[derive(Debug)]
pub struct Optimizer {}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {}
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator> {
        let location = LocationRule {};
        let optimized = location.optimize(plan)?;

        Ok(optimized)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator>;
}

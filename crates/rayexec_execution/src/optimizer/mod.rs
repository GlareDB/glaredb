pub mod join_order;
pub mod location;

use crate::{logical::operator::LogicalOperator, optimizer::join_order::JoinOrderRule};
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
        let join_order = JoinOrderRule {};
        let optimized = join_order.optimize(plan)?;

        let location = LocationRule {};
        let optimized = location.optimize(optimized)?;

        Ok(optimized)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(&self, plan: LogicalOperator) -> Result<LogicalOperator>;
}

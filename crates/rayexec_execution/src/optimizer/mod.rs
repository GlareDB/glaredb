use crate::planner::{bind_context::BindContext, operator::LogicalOperator};
use rayexec_error::Result;

#[derive(Debug)]
pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {}
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize(
        &self,
        bind_context: &BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator> {
        unimplemented!()
    }
}

pub trait OptimizeRule {
    fn optimize(
        &self,
        bind_context: &BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>;
}

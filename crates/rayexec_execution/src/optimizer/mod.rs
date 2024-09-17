pub mod filter_pushdown;
pub mod location;

use std::time::Duration;

use crate::{
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
    runtime::time::{RuntimeInstant, Timer},
};
use filter_pushdown::FilterPushdownRule;
use rayexec_error::Result;
use tracing::debug;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptimizerProfileData {
    pub filter_pushdown_1: Option<Duration>,
    pub filter_pushdown_2: Option<Duration>,
}

#[derive(Debug)]
pub struct Optimizer {
    profile_data: OptimizerProfileData,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {
            profile_data: OptimizerProfileData::default(),
        }
    }

    /// Run a logical plan through the optimizer.
    pub fn optimize<I>(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>
    where
        I: RuntimeInstant,
    {
        // First filter pushdown.
        let timer = Timer::<I>::start();
        let mut rule = FilterPushdownRule::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data.filter_pushdown_1 = Some(timer.stop());

        // DO THE OTHER RULES

        // let rule = LocationRule {};
        // let optimized = rule.optimize(bind_context, optimized)?;

        // Filter pushdown again.
        let timer = Timer::<I>::start();
        let mut rule = FilterPushdownRule::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data.filter_pushdown_2 = Some(timer.stop());

        debug!(?self.profile_data, "optimizer timings");

        Ok(plan)
    }
}

pub trait OptimizeRule {
    /// Apply an optimization rule to the logical plan.
    fn optimize(
        &mut self,
        bind_context: &mut BindContext,
        plan: LogicalOperator,
    ) -> Result<LogicalOperator>;
}

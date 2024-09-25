pub mod expr_rewrite;
pub mod filter_pushdown;
pub mod join_reorder;
pub mod limit_pushdown;
pub mod location;

use std::time::Duration;

use crate::{
    logical::{binder::bind_context::BindContext, operator::LogicalOperator},
    runtime::time::{RuntimeInstant, Timer},
};
use expr_rewrite::ExpressionRewriter;
use filter_pushdown::FilterPushdown;
use join_reorder::JoinReorder;
use limit_pushdown::LimitPushdown;
use rayexec_error::Result;
use tracing::debug;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OptimizerProfileData {
    pub total: Duration,
    pub timings: Vec<(&'static str, Duration)>,
}

#[derive(Debug)]
pub struct Optimizer {
    pub profile_data: OptimizerProfileData,
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
        let total = Timer::<I>::start();

        // Rewrite expressions first, makes it more likely the later
        // optimizations rules will be applied.
        let timer = Timer::<I>::start();
        let mut rule = ExpressionRewriter;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("expression_rewrite", timer.stop()));

        // First filter pushdown.
        let timer = Timer::<I>::start();
        let mut rule = FilterPushdown::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("filter_pushdown_1", timer.stop()));

        // Join reordering.
        let timer = Timer::<I>::start();
        let mut rule = JoinReorder::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("join_reorder", timer.stop()));

        // Limit pushdown.
        let timer = Timer::<I>::start();
        let mut rule = LimitPushdown;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("limit_pushdown", timer.stop()));

        // DO THE OTHER RULES

        // TODO: Location clustering once the rest is done.
        // let rule = LocationRule {};
        // let optimized = rule.optimize(bind_context, optimized)?;

        // TODO: Unsure if we actually want to do this again. We've already done
        // the first filter pushdown, then the join reordering is essentially
        // it's own filter push down localized to just trying to create inner joins.
        // // Filter pushdown again.
        // let timer = Timer::<I>::start();
        // let mut rule = FilterPushdown::default();
        // let plan = rule.optimize(bind_context, plan)?;
        // self.profile_data
        //     .timings
        //     .push(("filter_pushdown_2", timer.stop()));

        self.profile_data.total = total.stop();

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

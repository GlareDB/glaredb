pub mod column_prune;
pub mod expr_rewrite;
pub mod filter_pushdown;
pub mod join_reorder;
pub mod limit_pushdown;
pub mod location;

#[allow(dead_code)] // Until it's more robust
pub mod redundant_groups;

use std::time::Duration;

use column_prune::ColumnPrune;
use expr_rewrite::ExpressionRewriter;
use filter_pushdown::FilterPushdown;
use join_reorder::JoinReorder;
use limit_pushdown::LimitPushdown;
use glaredb_error::Result;
use tracing::debug;

use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::LogicalOperator;
use crate::runtime::time::{RuntimeInstant, Timer};

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

        // Limit pushdown.
        let timer = Timer::<I>::start();
        let mut rule = LimitPushdown;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("limit_pushdown", timer.stop()));

        // Column pruning.
        let timer = Timer::<I>::start();
        let mut rule = ColumnPrune::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("column_pruning", timer.stop()));

        // TODO: Re-enable this when it works better with duplicated expressions
        // across grouping sets.
        // let timer = Timer::<I>::start();
        // let mut rule = RemoveRedundantGroups::default();
        // let plan = rule.optimize(bind_context, plan)?;
        // self.profile_data
        //     .timings
        //     .push(("remove_redundant_groups", timer.stop()));

        // // Join reordering.
        let timer = Timer::<I>::start();
        let mut rule = JoinReorder::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("join_reorder", timer.stop()));

        // DO THE OTHER RULES

        // Second filter pushdown.
        //
        // Join order currently has a chance of producing a comparison join
        // followed by a filter with the comparison not being an equality.
        // Pushing down again gives us the best chance to get equalities into
        // the condition (for now, we can probably work on the join order more).
        // let timer = Timer::<I>::start();
        // let mut rule = FilterPushdown::default();
        // let plan = rule.optimize(bind_context, plan)?;
        // self.profile_data
        //     .timings
        //     .push(("filter_pushdown_2", timer.stop()));

        // TODO: Location clustering once the rest is done.
        // let rule = LocationRule {};
        // let optimized = rule.optimize(bind_context, optimized)?;

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

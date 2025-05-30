pub mod column_prune;
pub mod common_subexpression;
pub mod expr_rewrite;
pub mod filter_pushdown;
pub mod join_reorder;
pub mod limit_pushdown;
pub mod location;
pub mod scan_filter;
pub mod selection_reorder;
pub mod sort_limit_hint;

#[allow(dead_code)] // Until it's more robust
pub mod redundant_groups;

use column_prune::ColumnPrune;
use common_subexpression::CommonSubExpression;
use expr_rewrite::ExpressionRewriter;
use filter_pushdown::FilterPushdown;
use glaredb_error::Result;
use join_reorder::JoinReorder;
use limit_pushdown::LimitPushdown;
use scan_filter::ScanFilterPushdown;
use selection_reorder::SelectionReorder;
use sort_limit_hint::SortLimitHint;
use tracing::debug;

use crate::catalog::profile::OptimizerProfile;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::LogicalOperator;
use crate::runtime::time::{RuntimeInstant, Timer};

#[derive(Debug)]
pub struct Optimizer {
    pub profile_data: OptimizerProfile,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Optimizer {
            profile_data: OptimizerProfile::default(),
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

        // Push down scan filters.
        let timer = Timer::<I>::start();
        let mut rule = ScanFilterPushdown;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("scan_filter_pushdown", timer.stop()));

        // TODO: Re-enable this when it works better with duplicated expressions
        // across grouping sets.
        // let timer = Timer::<I>::start();
        // let mut rule = RemoveRedundantGroups::default();
        // let plan = rule.optimize(bind_context, plan)?;
        // self.profile_data
        //     .timings
        //     .push(("remove_redundant_groups", timer.stop()));

        // Common sub-expression eliminations.
        let timer = Timer::<I>::start();
        let mut rule = CommonSubExpression;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data.timings.push(("cse", timer.stop()));

        // Join reordering.
        let timer = Timer::<I>::start();
        let mut rule = JoinReorder::default();
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("join_reorder", timer.stop()));

        // Try to limit the amount of sorting that needs to happen.
        let timer = Timer::<I>::start();
        let mut rule = SortLimitHint;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("sort_limit_hint", timer.stop()));

        // Selection reorder.
        let timer = Timer::<I>::start();
        let mut rule = SelectionReorder;
        let plan = rule.optimize(bind_context, plan)?;
        self.profile_data
            .timings
            .push(("selection_reorder", timer.stop()));

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

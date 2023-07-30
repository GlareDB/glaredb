use chrono::{DateTime, Utc};
use datafusion::common::alias::AliasGenerator;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    Explain, LogicalPlan, PlanType, StringifiedPlan, ToStringifiedPlan,
};
use datafusion::optimizer::analyzer::Analyzer;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_expr::execution_props::ExecutionProps;
use std::sync::Arc;

pub struct SessionOptimizer<'a> {
    /// Logical plan optimizer.
    pub optimizer: &'a Optimizer,
    /// Analyzes the logical plan prior to it being optimized.
    pub analyzer: &'a Analyzer,
}

impl<'a> SessionOptimizer<'a> {
    /// Optimize a logical plan.
    pub fn optimize(
        &self,
        plan: &LogicalPlan,
        props: &ExecutionProps,
        opts: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        let opt_conf = SessionOptimizerConfig { props, opts };

        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // analyze & capture output of each rule
            let analyzed_plan = match self.analyzer.execute_and_check(
                e.plan.as_ref(),
                opts,
                |analyzed_plan, analyzer| {
                    let analyzer_name = analyzer.name().to_string();
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans.push(analyzed_plan.to_stringified(plan_type));
                },
            ) {
                Ok(plan) => plan,
                Err(DataFusionError::Context(analyzer_name, err)) => {
                    let plan_type = PlanType::AnalyzedLogicalPlan { analyzer_name };
                    stringified_plans.push(StringifiedPlan::new(plan_type, err.to_string()));

                    return Ok(LogicalPlan::Explain(Explain {
                        verbose: e.verbose,
                        plan: e.plan.clone(),
                        stringified_plans,
                        schema: e.schema.clone(),
                        logical_optimization_succeeded: false,
                    }));
                }
                Err(e) => return Err(e),
            };

            // to delineate the analyzer & optimizer phases in explain output
            stringified_plans
                .push(analyzed_plan.to_stringified(PlanType::FinalAnalyzedLogicalPlan));

            // optimize the child plan, capturing the output of each optimizer
            let (plan, logical_optimization_succeeded) = match self.optimizer.optimize(
                &analyzed_plan,
                &opt_conf,
                |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                },
            ) {
                Ok(plan) => (Arc::new(plan), true),
                Err(DataFusionError::Context(optimizer_name, err)) => {
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(StringifiedPlan::new(plan_type, err.to_string()));
                    (e.plan.clone(), false)
                }
                Err(e) => return Err(e),
            };

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan,
                stringified_plans,
                schema: e.schema.clone(),
                logical_optimization_succeeded,
            }))
        } else {
            let analyzed_plan = self.analyzer.execute_and_check(plan, opts, |_, _| {})?;
            self.optimizer
                .optimize(&analyzed_plan, &opt_conf, |_, _| {})
        }
    }
}

struct SessionOptimizerConfig<'a> {
    props: &'a ExecutionProps,
    opts: &'a ConfigOptions,
}

impl<'a> OptimizerConfig for SessionOptimizerConfig<'a> {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.props.query_execution_start_time
    }

    fn alias_generator(&self) -> Arc<AliasGenerator> {
        self.props.alias_generator.clone()
    }

    fn options(&self) -> &ConfigOptions {
        self.opts
    }
}

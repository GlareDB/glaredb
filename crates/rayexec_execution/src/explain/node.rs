use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tracing::error;

use super::context_display::ContextDisplayMode;
use super::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef};
use crate::logical::operator::LogicalOperator;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExplainNode {
    pub entry: ExplainEntry,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    pub fn new_from_planned_operators(verbose: bool, root: &PlannedOperatorWithChildren) -> Self {
        let config = ExplainConfig {
            context_mode: ContextDisplayMode::Raw,
            verbose,
        };
        Self::walk_physical(config, root)
    }

    pub fn new_from_logical_plan(
        bind_context: &BindContext,
        verbose: bool,
        root: &LogicalOperator,
    ) -> Self {
        let config = ExplainConfig {
            context_mode: ContextDisplayMode::Enriched(bind_context),
            verbose,
        };
        let mut visited_materializations = HashSet::new();

        Self::walk_logical(bind_context, config, root, &mut visited_materializations)
    }

    fn walk_physical(config: ExplainConfig, plan: &PlannedOperatorWithChildren) -> Self {
        let entry = plan.operator.call_explain_entry(config);
        let children = plan
            .children
            .iter()
            .map(|child| Self::walk_physical(config, child))
            .collect();

        ExplainNode { entry, children }
    }

    fn walk_logical(
        bind_context: &BindContext,
        config: ExplainConfig,
        plan: &LogicalOperator,
        visited_materializations: &mut HashSet<MaterializationRef>,
    ) -> Self {
        let (entry, children) = match plan {
            LogicalOperator::Invalid => (ExplainEntry::new("INVALID"), &Vec::new()),
            LogicalOperator::Project(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Filter(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Distinct(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Scan(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ExpressionList(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Aggregate(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SetOp(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Empty(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Limit(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Order(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SetVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ResetVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ShowVar(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::AttachDatabase(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::DetachDatabase(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Drop(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Insert(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CreateSchema(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CreateTable(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CreateView(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Describe(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Explain(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CopyTo(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::CrossJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ArbitraryJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::ComparisonJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::MagicJoin(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Unnest(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Window(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::TableExecute(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::MaterializationScan(n) => {
                // Materialization special case, walk children by get
                // materialization from bind context.
                let entry = n.explain_entry(config);

                let children = if !visited_materializations.contains(&n.node.mat) {
                    visited_materializations.insert(n.node.mat);
                    match bind_context.get_materialization(n.node.mat) {
                        Ok(mat) => vec![Self::walk_logical(
                            bind_context,
                            config,
                            &mat.plan,
                            visited_materializations,
                        )],
                        Err(e) => {
                            error!(%e, "failed to get materialization from bind context");
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                };

                return ExplainNode { entry, children };
            }
            LogicalOperator::MagicMaterializationScan(n) => {
                let entry = n.explain_entry(config);

                let children = if !visited_materializations.contains(&n.node.mat) {
                    visited_materializations.insert(n.node.mat);
                    match bind_context.get_materialization(n.node.mat) {
                        Ok(mat) => vec![Self::walk_logical(
                            bind_context,
                            config,
                            &mat.plan,
                            visited_materializations,
                        )],
                        Err(e) => {
                            error!(%e, "failed to get materialization from bind context");
                            Vec::new()
                        }
                    }
                } else {
                    Vec::new()
                };

                return ExplainNode { entry, children };
            }
        };

        let children = children
            .iter()
            .map(|c| Self::walk_logical(bind_context, config, c, visited_materializations))
            .collect();

        ExplainNode { entry, children }
    }
}

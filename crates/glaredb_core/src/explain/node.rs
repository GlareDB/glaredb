use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::context_display::ContextDisplayMode;
use super::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, ExplainValue, Explainable};
use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef};
use crate::logical::operator::LogicalOperator;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainedPlan {
    /// The base plan.
    pub base: ExplainNode,
    /// Materializations that are referenced from the base plan.
    pub materializations: BTreeMap<MaterializationRef, ExplainNode>,
}

impl ExplainedPlan {
    pub fn new_from_physical<'a>(
        verbose: bool,
        base_root: &PlannedOperatorWithChildren,
        materializations: impl IntoIterator<
            Item = (&'a MaterializationRef, &'a PlannedOperatorWithChildren),
        >,
    ) -> Self {
        let config = ExplainConfig {
            context_mode: ContextDisplayMode::Raw,
            verbose,
        };

        let base = ExplainNode::walk_physical(config, base_root);
        let materializations: BTreeMap<_, _> = materializations
            .into_iter()
            .map(|(mat_ref, plan)| {
                let node = ExplainNode::walk_physical(config, plan);
                (*mat_ref, node)
            })
            .collect();

        ExplainedPlan {
            base,
            materializations,
        }
    }

    pub fn new_from_logical(
        verbose: bool,
        bind_context: &BindContext,
        root: &LogicalOperator,
    ) -> Self {
        let config = ExplainConfig {
            context_mode: ContextDisplayMode::Enriched(bind_context),
            verbose,
        };

        let base = ExplainNode::walk_logical(config, bind_context, root);

        let materializations: BTreeMap<_, _> = bind_context
            .iter_materializations()
            .map(|mat| {
                let node = ExplainNode::walk_logical(config, bind_context, &mat.plan);
                (mat.mat_ref, node)
            })
            .collect();

        ExplainedPlan {
            base,
            materializations,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ExplainNode {
    pub entry: ExplainEntry,
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    fn walk_physical(config: ExplainConfig, plan: &PlannedOperatorWithChildren) -> Self {
        let entry = plan.operator.explain_entry(config);
        let children = plan
            .children
            .iter()
            .map(|child| Self::walk_physical(config, child))
            .collect();

        ExplainNode { entry, children }
    }

    fn walk_logical(
        config: ExplainConfig,
        bind_context: &BindContext,
        plan: &LogicalOperator,
    ) -> Self {
        let (entry, children) = match plan {
            LogicalOperator::Invalid => (EntryBuilder::new("INVALID", config).build(), &Vec::new()),
            LogicalOperator::Project(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Filter(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Distinct(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Scan(n) => {
                // Special case scan to get the column names and types to
                // display.
                let mut ent = n.explain_entry(config);
                if let Ok(data_table) = bind_context.get_table(n.node.data_scan.table_ref) {
                    ent.items.insert(
                        "data_column_types".to_string(),
                        ExplainValue::Values(
                            data_table
                                .column_types
                                .iter()
                                .map(|d| d.to_string())
                                .collect(),
                        ),
                    );
                    ent.items.insert(
                        "data_column_names".to_string(),
                        ExplainValue::Values(
                            data_table
                                .column_names
                                .iter()
                                .map(|n| n.as_raw_str().to_string())
                                .collect(),
                        ),
                    );
                }

                // Also include metadata columns if we have it.
                if let Some(meta_scan) = &n.node.meta_scan
                    && let Ok(meta_table) = bind_context.get_table(meta_scan.table_ref) {
                        ent.items.insert(
                            "meta_column_types".to_string(),
                            ExplainValue::Values(
                                meta_table
                                    .column_types
                                    .iter()
                                    .map(|d| d.to_string())
                                    .collect(),
                            ),
                        );
                        ent.items.insert(
                            "meta_column_names".to_string(),
                            ExplainValue::Values(
                                meta_table
                                    .column_names
                                    .iter()
                                    .map(|n| n.as_raw_str().to_string())
                                    .collect(),
                            ),
                        );
                    }

                (ent, &n.children)
            }
            LogicalOperator::ExpressionList(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::Aggregate(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SetOp(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::SingleRow(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::NoRows(n) => (n.explain_entry(config), &n.children),
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
            LogicalOperator::Discard(n) => (n.explain_entry(config), &n.children),
            LogicalOperator::MaterializationScan(n) => {
                // Materialization displayed separately.
                return ExplainNode {
                    entry: n.explain_entry(config),
                    children: Vec::new(),
                };
            }
            LogicalOperator::MagicMaterializationScan(n) => {
                // Materialization displayed separately.
                return ExplainNode {
                    entry: n.explain_entry(config),
                    children: Vec::new(),
                };
            }
        };

        let children = children
            .iter()
            .map(|c| Self::walk_logical(config, bind_context, c))
            .collect();

        ExplainNode { entry, children }
    }
}

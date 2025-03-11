mod plan_aggregate;
mod plan_create_schema;
mod plan_create_table;
mod plan_create_view;
mod plan_describe;
mod plan_distinct;
mod plan_drop;
mod plan_empty;
mod plan_explain;
mod plan_expression_list;
mod plan_filter;
mod plan_insert;
mod plan_join;
mod plan_limit;
mod plan_magic_scan;
mod plan_materialize_scan;
mod plan_project;
mod plan_scan;
mod plan_set_operation;
mod plan_show_var;
mod plan_sort;
mod plan_table_execute;
mod plan_unnest;

use std::collections::BTreeMap;

use rayexec_error::{not_implemented, RayexecError, Result};
use uuid::Uuid;

use super::operators::materialize::PhysicalMaterialize;
use super::operators::{PlannedOperatorWithChildren, PushOperator};
use crate::catalog::context::DatabaseContext;
use crate::config::execution::OperatorPlanConfig;
use crate::execution::operators::PlannedOperator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef, PlanMaterialization};
use crate::logical::operator::{self, LogicalOperator};

/// Output of physical planning.
#[derive(Debug)]
pub struct PlannedQueryGraph {
    pub materializations: BTreeMap<MaterializationRef, PlannedOperatorWithChildren>,
    pub root: PlannedOperatorWithChildren,
}

/// Planner for building the query graph containing physical operators.
#[derive(Debug)]
pub struct OperatorPlanner {
    config: OperatorPlanConfig,
    query_id: Uuid,
}

impl OperatorPlanner {
    pub fn new(config: OperatorPlanConfig, query_id: Uuid) -> Self {
        OperatorPlanner { config, query_id }
    }

    /// Plan the intermediate pipelines.
    pub fn plan<O>(
        &self,
        root: operator::LogicalOperator,
        db_context: &DatabaseContext,
        mut bind_context: BindContext,
        sink: O,
    ) -> Result<PlannedQueryGraph>
    where
        O: PushOperator,
    {
        let mats = bind_context.take_materializations();
        let mut state = OperatorPlanState::new(&self.config, db_context, &bind_context);

        // Plan materializations first.
        state.plan_materializations(mats)?;

        // Now plan to query with access to all materializations.
        let root = state.plan(root)?;

        let planned_sink = PlannedOperator::new_push(sink);

        let root = PlannedOperatorWithChildren {
            operator: planned_sink,
            children: vec![root],
        };

        Ok(PlannedQueryGraph {
            root,
            materializations: state.materializations,
        })
    }
}

#[derive(Debug)]
struct OperatorPlanState<'a> {
    config: &'a OperatorPlanConfig,
    /// Session database context.
    db_context: &'a DatabaseContext,
    /// Bind context used during logical planning.
    ///
    /// Used to generate physical expressions, and determined data types
    /// returned from operators.
    ///
    /// Also holds materialized plans.
    bind_context: &'a BindContext,
    /// Expression planner for converting logical to physical expressions.
    expr_planner: PhysicalExpressionPlanner<'a>,
    /// Mapping of materialization refs to the plans.
    ///
    /// When placing a materialization in the requesting plan, only the operator
    /// should be cloned, and its children set to empty. This will act as a
    /// "marker" during pipeline building allowing us to get the shared operator
    /// state.
    ///
    /// BTree map to retain order in which materializations were planned (as one
    /// might depend on another). We want to make sure we iterate in the same
    /// order when creating the operator states.
    materializations: BTreeMap<MaterializationRef, PlannedOperatorWithChildren>,
}

impl<'a> OperatorPlanState<'a> {
    fn new(
        config: &'a OperatorPlanConfig,
        db_context: &'a DatabaseContext,
        bind_context: &'a BindContext,
    ) -> Self {
        let expr_planner = PhysicalExpressionPlanner::new(bind_context.get_table_list());

        OperatorPlanState {
            config,
            db_context,
            bind_context,
            expr_planner,
            materializations: BTreeMap::new(),
        }
    }

    /// Plan materializations from the bind context.
    ///
    /// The planned materializations will be placed in this plan state.
    fn plan_materializations(&mut self, materializations: Vec<PlanMaterialization>) -> Result<()> {
        // TODO: The way this and the materialization ref is implemented allows
        // materializations to depend on previously planned materializations.
        // Unsure if we want to make that a strong guarantee (probably yes).

        for mat in materializations {
            let mat_root = self.plan(mat.plan)?;

            let operator = PhysicalMaterialize {
                datatypes: mat_root.operator.call_output_types(),
                materialization_ref: mat.mat_ref,
            };

            let operator = PlannedOperatorWithChildren {
                operator: PlannedOperator::new_materializing(operator),
                children: vec![mat_root],
            };

            if self
                .materializations
                .insert(mat.mat_ref, operator)
                .is_some()
            {
                return Err(RayexecError::new(format!(
                    "Duplicate materialization ref: {}",
                    mat.mat_ref
                )));
            }
        }

        Ok(())
    }

    fn plan(&mut self, plan: LogicalOperator) -> Result<PlannedOperatorWithChildren> {
        match plan {
            LogicalOperator::Project(node) => self.plan_project(node),
            LogicalOperator::Filter(node) => self.plan_filter(node),
            LogicalOperator::Explain(node) => self.plan_explain(node),
            LogicalOperator::ExpressionList(node) => self.plan_expression_list(node),
            LogicalOperator::Order(node) => self.plan_sort(node),
            LogicalOperator::Aggregate(node) => self.plan_aggregate(node),
            LogicalOperator::Limit(node) => self.plan_limit(node),
            LogicalOperator::MagicJoin(join) => self.plan_magic_join(join),
            LogicalOperator::CrossJoin(join) => self.plan_cross_join(join),
            LogicalOperator::ArbitraryJoin(join) => self.plan_arbitrary_join(join),
            LogicalOperator::ComparisonJoin(join) => self.plan_comparison_join(join),
            LogicalOperator::TableExecute(join) => self.plan_table_execute(join),
            LogicalOperator::Describe(node) => self.plan_describe(node),
            LogicalOperator::ShowVar(node) => self.plan_show_var(node),
            LogicalOperator::Scan(node) => self.plan_scan(node),
            LogicalOperator::Empty(node) => self.plan_empty(node),
            LogicalOperator::SetVar(_) => {
                Err(RayexecError::new("SET should be handled in the session"))
            }
            LogicalOperator::ResetVar(_) => {
                Err(RayexecError::new("RESET should be handled in the session"))
            }
            LogicalOperator::DetachDatabase(_) | LogicalOperator::AttachDatabase(_) => Err(
                RayexecError::new("ATTACH/DETACH should be handled in the session"),
            ),
            LogicalOperator::CreateView(node) => self.plan_create_view(node),
            LogicalOperator::CreateSchema(node) => self.plan_create_schema(node),
            LogicalOperator::CreateTable(node) => self.plan_create_table(node),
            LogicalOperator::Drop(node) => self.plan_drop(node),
            LogicalOperator::Insert(node) => self.plan_insert(node),
            other => not_implemented!("logical plan to physical plan: {other:?}"),
        }
    }
}

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
use std::fmt;

use glaredb_error::{DbError, Result, not_implemented};
use uuid::Uuid;

use super::operators::materialize::PhysicalMaterialize;
use super::operators::{PlannedOperatorWithChildren, PushOperator};
use crate::catalog::context::DatabaseContext;
use crate::config::execution::OperatorPlanConfig;
use crate::execution::operators::PlannedOperator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::{BindContext, MaterializationRef};
use crate::logical::operator::{self, LogicalOperator};

/// Output of physical planning.
#[derive(Debug)]
pub struct PlannedQueryGraph {
    pub materializations: BTreeMap<MaterializationRef, PlannedOperatorWithChildren>,
    pub root: PlannedOperatorWithChildren,
}

/// Identifier for an operator within a query.
///
/// Unique across all operators within a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorId(pub(crate) usize);

impl fmt::Display for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// State for generating unique operator IDs.
#[derive(Debug)]
pub struct OperatorIdGen {
    next: usize,
}

impl OperatorIdGen {
    pub fn next_id(&mut self) -> OperatorId {
        let id = OperatorId(self.next);
        self.next += 1;
        id
    }
}

/// Planner for building the query graph containing physical operators.
#[derive(Debug)]
pub struct OperatorPlanner {
    config: OperatorPlanConfig,
    _query_id: Uuid,
}

impl OperatorPlanner {
    pub fn new(config: OperatorPlanConfig, query_id: Uuid) -> Self {
        OperatorPlanner {
            config,
            _query_id: query_id,
        }
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
        // Get the plans making up materializations.
        let mut mat_plans: Vec<(MaterializationRef, LogicalOperator)> = Vec::new();
        for mat in bind_context.iter_materializations() {
            let plan = mat.plan.clone();
            mat_plans.push((mat.mat_ref, plan));
        }

        let mut state = OperatorPlanState::new(&self.config, db_context, &bind_context);

        // Plan materializations first.
        state.plan_materializations(mat_plans)?;

        // Now plan to query with access to all materializations.
        let root = state.plan(root)?;

        let planned_sink = PlannedOperator::new_push(state.id_gen.next_id(), sink);

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
    /// Generate unique ids for all operators.
    id_gen: OperatorIdGen,
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
            id_gen: OperatorIdGen { next: 0 },
        }
    }

    /// Plan materializations from the bind context.
    ///
    /// The planned materializations will be placed in this plan state.
    fn plan_materializations(
        &mut self,
        materializations: Vec<(MaterializationRef, LogicalOperator)>,
    ) -> Result<()> {
        // TODO: The way this and the materialization ref is implemented allows
        // materializations to depend on previously planned materializations.
        // Unsure if we want to make that a strong guarantee (probably yes).

        for (mat_ref, mat_plan) in materializations {
            let mat_root = self.plan(mat_plan)?;

            let operator = PhysicalMaterialize {
                datatypes: mat_root.operator.call_output_types(),
                materialization_ref: mat_ref,
            };

            let operator = PlannedOperatorWithChildren {
                operator: PlannedOperator::new_materializing(self.id_gen.next_id(), operator),
                children: vec![mat_root],
            };

            if self.materializations.insert(mat_ref, operator).is_some() {
                return Err(DbError::new(format!(
                    "Duplicate materialization ref: {}",
                    mat_ref
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
            LogicalOperator::MagicJoin(node) => self.plan_magic_join(node),
            LogicalOperator::CrossJoin(node) => self.plan_cross_join(node),
            LogicalOperator::ArbitraryJoin(node) => self.plan_arbitrary_join(node),
            LogicalOperator::ComparisonJoin(node) => self.plan_comparison_join(node),
            LogicalOperator::TableExecute(node) => self.plan_table_execute(node),
            LogicalOperator::SetOp(node) => self.plan_set_operation(node),
            LogicalOperator::Unnest(node) => self.plan_unnest(node),
            LogicalOperator::Distinct(node) => self.plan_distinct(node),
            LogicalOperator::Describe(node) => self.plan_describe(node),
            LogicalOperator::ShowVar(node) => self.plan_show_var(node),
            LogicalOperator::Scan(node) => self.plan_scan(node),
            LogicalOperator::MaterializationScan(node) => self.plan_materialize_scan(node),
            LogicalOperator::MagicMaterializationScan(node) => {
                self.plan_magic_materialize_scan(node)
            }
            LogicalOperator::Empty(node) => self.plan_empty(node),
            LogicalOperator::SetVar(_) => Err(DbError::new("SET should be handled in the session")),
            LogicalOperator::ResetVar(_) => {
                Err(DbError::new("RESET should be handled in the session"))
            }
            LogicalOperator::DetachDatabase(_) | LogicalOperator::AttachDatabase(_) => Err(
                DbError::new("ATTACH/DETACH should be handled in the session"),
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

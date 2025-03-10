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

use rayexec_error::{not_implemented, RayexecError, Result};
use uuid::Uuid;

use super::operators::{PlannedOperatorWithChildren, PushOperator};
use crate::catalog::context::DatabaseContext;
use crate::config::execution::OperatorPlanConfig;
use crate::execution::operators::PlannedOperator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{self, LogicalOperator};

#[derive(Debug)]
pub struct QueryGraph {
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
        bind_context: BindContext,
        sink: O,
    ) -> Result<QueryGraph>
    where
        O: PushOperator,
    {
        // TODO: Materializations....

        let mut state = OperatorPlanState::new(&self.config, db_context, &bind_context);
        let root = state.plan(root)?;

        let planned_sink = PlannedOperator::new_push(sink);

        let root = PlannedOperatorWithChildren {
            operator: planned_sink,
            children: vec![root],
        };

        Ok(QueryGraph { root })
    }
}

#[derive(Debug)]
struct Materializations {
    // local: IntermediateMaterializationGroup,
    // TODO: Remote materializations.
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
        }
    }

    /// Plan materializations from the bind context.
    fn plan_materializations(&mut self) -> Result<Materializations> {
        // TODO: The way this and the materialization ref is implemented allows
        // materializations to depend on previously planned materializations.
        // Unsure if we want to make that a strong guarantee (probably yes).

        unimplemented!()
        // let mut materializations = Materializations {
        //     local: IntermediateMaterializationGroup::default(),
        // };

        // for mat in self.bind_context.iter_materializations() {
        //     self.walk(&mut materializations, id_gen, mat.plan.clone())?; // TODO: The clone is unfortunate.

        //     let in_progress = self.take_in_progress_pipeline()?;
        //     if in_progress.location == LocationRequirement::Remote {
        //         not_implemented!("remote materializations");
        //     }

        //     let intermediate = IntermediateMaterialization {
        //         id: in_progress.id,
        //         source: in_progress.source,
        //         operators: in_progress.operators,
        //         scan_count: mat.scan_count,
        //     };

        //     materializations
        //         .local
        //         .materializations
        //         .insert(mat.mat_ref, intermediate);
        // }

        // Ok(materializations)
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
            LogicalOperator::Insert(node) => self.plan_insert(node),
            other => not_implemented!("logical plan to physical plan: {other:?}"),
        }
    }

    /// Recursively walk the plan, building up intermediate pipelines.
    fn walk(
        &mut self,
        materializations: &mut Materializations,
        plan: LogicalOperator,
    ) -> Result<()> {
        unimplemented!()
        // match plan {
        //     LogicalOperator::Project(proj) => self.plan_project(materializations, proj),
        //     LogicalOperator::Unnest(unnest) => self.plan_unnest(materializations, unnest),
        //     LogicalOperator::Filter(filter) => self.plan_filter(materializations, filter),
        //     LogicalOperator::Distinct(distinct) => self.plan_distinct(materializations, distinct),
        //     LogicalOperator::CrossJoin(join) => self.plan_cross_join(materializations, join),
        //     LogicalOperator::ArbitraryJoin(join) => {
        //         self.plan_arbitrary_join(materializations, join)
        //     }
        //     LogicalOperator::ComparisonJoin(join) => {
        //         self.plan_comparison_join(materializations, join)
        //     }
        //     LogicalOperator::MagicJoin(join) => self.plan_magic_join(materializations, join),
        //     LogicalOperator::Empty(empty) => self.plan_empty(empty),
        //     LogicalOperator::Aggregate(agg) => self.plan_aggregate(materializations, agg),
        //     LogicalOperator::Limit(limit) => self.plan_limit(materializations, limit),
        //     LogicalOperator::Order(order) => self.plan_sort(materializations, order),
        //     LogicalOperator::ShowVar(show_var) => self.plan_show_var(show_var),
        //     LogicalOperator::Explain(explain) => self.plan_explain(materializations, explain),
        //     LogicalOperator::Describe(describe) => self.plan_describe(describe),
        //     LogicalOperator::CreateTable(create) => {
        //         self.plan_create_table(materializations, create)
        //     }
        //     LogicalOperator::CreateView(create) => self.plan_create_view(create),
        //     LogicalOperator::CreateSchema(create) => self.plan_create_schema(create),
        //     LogicalOperator::Drop(drop) => self.plan_drop(drop),
        //     LogicalOperator::Insert(insert) => self.plan_insert(materializations, insert),
        //     LogicalOperator::CopyTo(copy_to) => self.plan_copy_to(materializations, copy_to),
        //     LogicalOperator::MaterializationScan(scan) => {
        //         self.plan_materialize_scan(materializations, scan)
        //     }
        //     LogicalOperator::MagicMaterializationScan(scan) => {
        //         self.plan_magic_materialize_scan(materializations, scan)
        //     }
        //     LogicalOperator::Scan(scan) => self.plan_scan(scan),
        //     LogicalOperator::SetOp(setop) => self.plan_set_operation(materializations, setop),
        //     LogicalOperator::InOut(inout) => self.plan_inout(materializations, inout),
        //     LogicalOperator::SetVar(_) => {
        //         Err(RayexecError::new("SET should be handled in the session"))
        //     }
        //     LogicalOperator::ResetVar(_) => {
        //         Err(RayexecError::new("RESET should be handled in the session"))
        //     }
        //     LogicalOperator::DetachDatabase(_) | LogicalOperator::AttachDatabase(_) => Err(
        //         RayexecError::new("ATTACH/DETACH should be handled in the session"),
        //     ),
        //     other => not_implemented!("logical plan to pipeline: {other:?}"),
        // }
    }
}

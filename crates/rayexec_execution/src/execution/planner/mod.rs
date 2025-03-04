mod plan_aggregate;
mod plan_copy_to;
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
mod plan_inout;
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
mod plan_unnest;

use rayexec_error::{not_implemented, RayexecError, Result};
use uuid::Uuid;

use super::operators::{PlannedOperatorWithChildren, PushOperator};
use crate::config::execution::OperatorPlanConfig;
use crate::execution::operators::{PhysicalOperator, PlannedOperator};
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{self, LocationRequirement, LogicalOperator};

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
        bind_context: BindContext,
        sink: O,
    ) -> Result<QueryGraph>
    where
        O: PushOperator,
    {
        // TODO: Materializations....

        let mut state = OperatorPlanState::new(&self.config, &bind_context);
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

/// Represents an intermediate pipeline that we're building up.
#[derive(Debug)]
struct InProgressPipeline {
    // id: IntermediatePipelineId,
    // /// All operators we've planned so far. Should be order left-to-right in
    // /// terms of execution flow.
    // operators: Vec<IntermediateOperator>,
    // /// Location where these operators should be running. This will determine
    // /// which pipeline group this pipeline will be placed in.
    // location: LocationRequirement,
    // /// Source of the pipeline.
    // source: PipelineSource,
}

#[derive(Debug)]
struct OperatorPlanState<'a> {
    config: &'a OperatorPlanConfig,
    /// Pipeline we're working on, as well as the location for where it should
    /// be executed.
    in_progress: Option<InProgressPipeline>,
    // /// Pipelines in the local group.
    // local_group: IntermediatePipelineGroup,
    // /// Pipelines in the remote group.
    // remote_group: IntermediatePipelineGroup,
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
    fn new(config: &'a OperatorPlanConfig, bind_context: &'a BindContext) -> Self {
        let expr_planner = PhysicalExpressionPlanner::new(bind_context.get_table_list());

        OperatorPlanState {
            config,
            in_progress: None,
            // local_group: IntermediatePipelineGroup::default(),
            // remote_group: IntermediatePipelineGroup::default(),
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
            LogicalOperator::Explain(node) => self.plan_explain(node),
            LogicalOperator::ExpressionList(node) => self.plan_expression_list(node),
            LogicalOperator::Order(node) => self.plan_sort(node),
            LogicalOperator::Empty(node) => self.plan_empty(node),
            other => unimplemented!("other: {other:?}"),
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

    /// Get the current in-progress pipeline.
    ///
    /// Errors if there's no pipeline in-progress.
    fn in_progress_pipeline_mut(&mut self) -> Result<&mut InProgressPipeline> {
        match &mut self.in_progress {
            Some(pipeline) => Ok(pipeline),
            None => Err(RayexecError::new("No pipeline in-progress")),
        }
    }

    fn take_in_progress_pipeline(&mut self) -> Result<InProgressPipeline> {
        self.in_progress
            .take()
            .ok_or_else(|| RayexecError::new("No in-progress pipeline to take"))
    }

    /// Pushes an intermedate operator onto the in-progress pipeline, erroring
    /// if there is no in-progress pipeline.
    ///
    /// If the location requirement of the operator differs from the in-progress
    /// pipeline, the in-progress pipeline will be finalized and a new
    /// in-progress pipeline created.
    fn push_intermediate_operator(
        &mut self,
        operator: PhysicalOperator,
        location: LocationRequirement,
    ) -> Result<()> {
        unimplemented!()
        // let current_location = &mut self
        //     .in_progress
        //     .as_mut()
        //     .required("in-progress pipeline")?
        //     .location;

        // // TODO: Determine if we want to allow Any to get this far. This means
        // // that either the optimizer didn't run, or the plan has no location
        // // requirements (no dependencies on tables or files).
        // if *current_location == LocationRequirement::Any {
        //     *current_location = location;
        // }

        // // If we're pushing an operator for any location, just inherit the
        // // location for the current pipeline.
        // if location == LocationRequirement::Any {
        //     location = *current_location
        // }

        // if *current_location == location {
        //     // Same location, just push
        //     let in_progress = self.in_progress_pipeline_mut()?;
        //     in_progress.operators.push(operator);
        // } else {
        //     // // Different locations, finalize in-progress and start a new one.
        //     // let in_progress = self.take_in_progress_pipeline()?;

        //     // let stream_id = id_gen.new_stream_id();

        //     // let new_in_progress = InProgressPipeline {
        //     //     id: id_gen.next_pipeline_id(),
        //     //     operators: vec![operator],
        //     //     location,
        //     //     source: PipelineSource::OtherGroup {
        //     //         stream_id,
        //     //         partitions: 1,
        //     //     },
        //     // };

        //     unimplemented!()
        //     // let finalized = IntermediatePipeline {
        //     //     id: in_progress.id,
        //     //     sink: PipelineSink::OtherGroup {
        //     //         stream_id,
        //     //         partitions: 1,
        //     //     },
        //     //     source: in_progress.source,
        //     //     operators: in_progress.operators,
        //     // };

        //     // match in_progress.location {
        //     //     LocationRequirement::ClientLocal => {
        //     //         self.local_group.pipelines.insert(finalized.id, finalized);
        //     //     }
        //     //     LocationRequirement::Remote => {
        //     //         self.remote_group.pipelines.insert(finalized.id, finalized);
        //     //     }
        //     //     LocationRequirement::Any => {
        //     //         self.local_group.pipelines.insert(finalized.id, finalized);
        //     //     }
        //     // }

        //     // self.in_progress = Some(new_in_progress)
        // }

        // Ok(())
    }
}

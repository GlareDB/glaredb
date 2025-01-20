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

use std::sync::Arc;

use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use uuid::Uuid;

use super::pipeline::{
    IntermediateMaterialization,
    IntermediateMaterializationGroup,
    IntermediateOperator,
    IntermediatePipeline,
    IntermediatePipelineGroup,
    IntermediatePipelineId,
    PipelineSink,
    PipelineSource,
    StreamId,
};
use crate::config::execution::IntermediatePlanConfig;
use crate::execution::operators::PhysicalOperator;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::logical::binder::bind_context::BindContext;
use crate::logical::operator::{self, LocationRequirement, LogicalOperator};

/// Planned pipelines grouped into locations for where they should be executed.
#[derive(Debug)]
pub struct PlannedPipelineGroups {
    pub local: IntermediatePipelineGroup,
    pub remote: IntermediatePipelineGroup,
    pub materializations: IntermediateMaterializationGroup,
}

/// Planner for building intermedate pipelines.
///
/// Intermediate pipelines still retain some structure with which pipeline feeds
/// into another, but are not yet executable.
#[derive(Debug)]
pub struct IntermediatePipelinePlanner {
    config: IntermediatePlanConfig,
    query_id: Uuid,
}

impl IntermediatePipelinePlanner {
    pub fn new(config: IntermediatePlanConfig, query_id: Uuid) -> Self {
        IntermediatePipelinePlanner { config, query_id }
    }

    /// Plan the intermediate pipelines.
    pub fn plan_pipelines(
        &self,
        root: operator::LogicalOperator,
        bind_context: BindContext,
    ) -> Result<PlannedPipelineGroups> {
        let mut state = IntermediatePipelineBuildState::new(&self.config, &bind_context);
        let mut id_gen = PipelineIdGen::new(self.query_id);

        let mut materializations = state.plan_materializations(&mut id_gen)?;
        state.walk(&mut materializations, &mut id_gen, root)?;

        state.finish(&mut id_gen)?;

        debug_assert!(state.in_progress.is_none());

        Ok(PlannedPipelineGroups {
            local: state.local_group,
            remote: state.remote_group,
            materializations: materializations.local,
        })
    }
}

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    query_id: Uuid,
    pipeline_gen: IntermediatePipelineId,
}

impl PipelineIdGen {
    fn new(query_id: Uuid) -> Self {
        PipelineIdGen {
            query_id,
            pipeline_gen: IntermediatePipelineId(0),
        }
    }

    fn next_pipeline_id(&mut self) -> IntermediatePipelineId {
        let id = self.pipeline_gen;
        self.pipeline_gen.0 += 1;
        id
    }

    fn new_stream_id(&self) -> StreamId {
        StreamId {
            query_id: self.query_id,
            stream_id: Uuid::new_v4(),
        }
    }
}

#[derive(Debug)]
struct Materializations {
    local: IntermediateMaterializationGroup,
    // TODO: Remote materializations.
}

/// Represents an intermediate pipeline that we're building up.
#[derive(Debug)]
struct InProgressPipeline {
    id: IntermediatePipelineId,
    /// All operators we've planned so far. Should be order left-to-right in
    /// terms of execution flow.
    operators: Vec<IntermediateOperator>,
    /// Location where these operators should be running. This will determine
    /// which pipeline group this pipeline will be placed in.
    location: LocationRequirement,
    /// Source of the pipeline.
    source: PipelineSource,
}

#[derive(Debug)]
struct IntermediatePipelineBuildState<'a> {
    config: &'a IntermediatePlanConfig,
    /// Pipeline we're working on, as well as the location for where it should
    /// be executed.
    in_progress: Option<InProgressPipeline>,
    /// Pipelines in the local group.
    local_group: IntermediatePipelineGroup,
    /// Pipelines in the remote group.
    remote_group: IntermediatePipelineGroup,
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

impl<'a> IntermediatePipelineBuildState<'a> {
    fn new(config: &'a IntermediatePlanConfig, bind_context: &'a BindContext) -> Self {
        let expr_planner = PhysicalExpressionPlanner::new(bind_context.get_table_list());

        IntermediatePipelineBuildState {
            config,
            in_progress: None,
            local_group: IntermediatePipelineGroup::default(),
            remote_group: IntermediatePipelineGroup::default(),
            bind_context,
            expr_planner,
        }
    }

    /// Plan materializations from the bind context.
    fn plan_materializations(&mut self, id_gen: &mut PipelineIdGen) -> Result<Materializations> {
        // TODO: The way this and the materialization ref is implemented allows
        // materializations to depend on previously planned materializations.
        // Unsure if we want to make that a strong guarantee (probably yes).

        let mut materializations = Materializations {
            local: IntermediateMaterializationGroup::default(),
        };

        for mat in self.bind_context.iter_materializations() {
            self.walk(&mut materializations, id_gen, mat.plan.clone())?; // TODO: The clone is unfortunate.

            let in_progress = self.take_in_progress_pipeline()?;
            if in_progress.location == LocationRequirement::Remote {
                not_implemented!("remote materializations");
            }

            let intermediate = IntermediateMaterialization {
                id: in_progress.id,
                source: in_progress.source,
                operators: in_progress.operators,
                scan_count: mat.scan_count,
            };

            materializations
                .local
                .materializations
                .insert(mat.mat_ref, intermediate);
        }

        Ok(materializations)
    }

    /// Recursively walk the plan, building up intermediate pipelines.
    fn walk(
        &mut self,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        plan: LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Project(proj) => self.plan_project(id_gen, materializations, proj),
            LogicalOperator::Unnest(unnest) => self.plan_unnest(id_gen, materializations, unnest),
            LogicalOperator::Filter(filter) => self.plan_filter(id_gen, materializations, filter),
            LogicalOperator::Distinct(distinct) => {
                self.plan_distinct(id_gen, materializations, distinct)
            }
            LogicalOperator::CrossJoin(join) => {
                self.plan_cross_join(id_gen, materializations, join)
            }
            LogicalOperator::ArbitraryJoin(join) => {
                self.plan_arbitrary_join(id_gen, materializations, join)
            }
            LogicalOperator::ComparisonJoin(join) => {
                self.plan_comparison_join(id_gen, materializations, join)
            }
            LogicalOperator::MagicJoin(join) => {
                self.plan_magic_join(id_gen, materializations, join)
            }
            LogicalOperator::Empty(empty) => self.plan_empty(id_gen, empty),
            LogicalOperator::Aggregate(agg) => self.plan_aggregate(id_gen, materializations, agg),
            LogicalOperator::Limit(limit) => self.plan_limit(id_gen, materializations, limit),
            LogicalOperator::Order(order) => self.plan_sort(id_gen, materializations, order),
            LogicalOperator::ShowVar(show_var) => self.plan_show_var(id_gen, show_var),
            LogicalOperator::Explain(explain) => {
                self.plan_explain(id_gen, materializations, explain)
            }
            LogicalOperator::Describe(describe) => self.plan_describe(id_gen, describe),
            LogicalOperator::CreateTable(create) => {
                self.plan_create_table(id_gen, materializations, create)
            }
            LogicalOperator::CreateView(create) => self.plan_create_view(id_gen, create),
            LogicalOperator::CreateSchema(create) => self.plan_create_schema(id_gen, create),
            LogicalOperator::Drop(drop) => self.plan_drop(id_gen, drop),
            LogicalOperator::Insert(insert) => self.plan_insert(id_gen, materializations, insert),
            LogicalOperator::CopyTo(copy_to) => {
                self.plan_copy_to(id_gen, materializations, copy_to)
            }
            LogicalOperator::MaterializationScan(scan) => {
                self.plan_materialize_scan(id_gen, materializations, scan)
            }
            LogicalOperator::MagicMaterializationScan(scan) => {
                self.plan_magic_materialize_scan(id_gen, materializations, scan)
            }
            LogicalOperator::Scan(scan) => self.plan_scan(id_gen, scan),
            LogicalOperator::SetOp(setop) => {
                self.plan_set_operation(id_gen, materializations, setop)
            }
            LogicalOperator::InOut(inout) => self.plan_inout(id_gen, materializations, inout),
            LogicalOperator::SetVar(_) => {
                Err(RayexecError::new("SET should be handled in the session"))
            }
            LogicalOperator::ResetVar(_) => {
                Err(RayexecError::new("RESET should be handled in the session"))
            }
            LogicalOperator::DetachDatabase(_) | LogicalOperator::AttachDatabase(_) => Err(
                RayexecError::new("ATTACH/DETACH should be handled in the session"),
            ),
            other => not_implemented!("logical plan to pipeline: {other:?}"),
        }
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

    /// Marks some other in-progress pipeline as a child that feeds into the
    /// current in-progress pipeline.
    ///
    /// The operator in which the child feeds into is the last operator in the
    /// current in-progress pipeline. `input_idx` is relative to that operator.
    fn push_as_child_pipeline(
        &mut self,
        child: InProgressPipeline,
        input_idx: usize,
    ) -> Result<()> {
        let in_progress = self.in_progress_pipeline_mut()?;

        unimplemented!()
        // let child_pipeline = IntermediatePipeline {
        //     id: child.id,
        //     sink: PipelineSink::InGroup {
        //         pipeline_id: in_progress.id,
        //         operator_idx: in_progress.operators.len() - 1,
        //         input_idx,
        //     },
        //     source: child.source,
        //     operators: child.operators,
        // };

        // match child.location {
        //     LocationRequirement::ClientLocal => {
        //         self.local_group
        //             .pipelines
        //             .insert(child_pipeline.id, child_pipeline);
        //     }
        //     LocationRequirement::Remote => {
        //         self.remote_group
        //             .pipelines
        //             .insert(child_pipeline.id, child_pipeline);
        //     }
        //     LocationRequirement::Any => {
        //         // TODO: Determine if any should be allowed here.
        //         self.local_group
        //             .pipelines
        //             .insert(child_pipeline.id, child_pipeline);
        //     }
        // }

        // Ok(())
    }

    /// Pushes an intermedate operator onto the in-progress pipeline, erroring
    /// if there is no in-progress pipeline.
    ///
    /// If the location requirement of the operator differs from the in-progress
    /// pipeline, the in-progress pipeline will be finalized and a new
    /// in-progress pipeline created.
    fn push_intermediate_operator(
        &mut self,
        operator: IntermediateOperator,
        mut location: LocationRequirement,
        id_gen: &mut PipelineIdGen,
    ) -> Result<()> {
        let current_location = &mut self
            .in_progress
            .as_mut()
            .required("in-progress pipeline")?
            .location;

        // TODO: Determine if we want to allow Any to get this far. This means
        // that either the optimizer didn't run, or the plan has no location
        // requirements (no dependencies on tables or files).
        if *current_location == LocationRequirement::Any {
            *current_location = location;
        }

        // If we're pushing an operator for any location, just inherit the
        // location for the current pipeline.
        if location == LocationRequirement::Any {
            location = *current_location
        }

        if *current_location == location {
            // Same location, just push
            let in_progress = self.in_progress_pipeline_mut()?;
            in_progress.operators.push(operator);
        } else {
            // // Different locations, finalize in-progress and start a new one.
            // let in_progress = self.take_in_progress_pipeline()?;

            // let stream_id = id_gen.new_stream_id();

            // let new_in_progress = InProgressPipeline {
            //     id: id_gen.next_pipeline_id(),
            //     operators: vec![operator],
            //     location,
            //     source: PipelineSource::OtherGroup {
            //         stream_id,
            //         partitions: 1,
            //     },
            // };

            unimplemented!()
            // let finalized = IntermediatePipeline {
            //     id: in_progress.id,
            //     sink: PipelineSink::OtherGroup {
            //         stream_id,
            //         partitions: 1,
            //     },
            //     source: in_progress.source,
            //     operators: in_progress.operators,
            // };

            // match in_progress.location {
            //     LocationRequirement::ClientLocal => {
            //         self.local_group.pipelines.insert(finalized.id, finalized);
            //     }
            //     LocationRequirement::Remote => {
            //         self.remote_group.pipelines.insert(finalized.id, finalized);
            //     }
            //     LocationRequirement::Any => {
            //         self.local_group.pipelines.insert(finalized.id, finalized);
            //     }
            // }

            // self.in_progress = Some(new_in_progress)
        }

        Ok(())
    }

    fn finish(&mut self, id_gen: &mut PipelineIdGen) -> Result<()> {
        let mut in_progress = self.take_in_progress_pipeline()?;
        if in_progress.location == LocationRequirement::Any {
            in_progress.location = LocationRequirement::ClientLocal;
        }

        unimplemented!()
        // if in_progress.location != LocationRequirement::ClientLocal {
        //     let stream_id = id_gen.new_stream_id();

        //     let final_pipeline = IntermediatePipeline {
        //         id: id_gen.next_pipeline_id(),
        //         sink: PipelineSink::QueryOutput,
        //         source: PipelineSource::OtherGroup {
        //             stream_id,
        //             partitions: 1,
        //         },
        //         operators: Vec::new(),
        //     };

        //     let pipeline = IntermediatePipeline {
        //         id: in_progress.id,
        //         sink: PipelineSink::OtherGroup {
        //             stream_id,
        //             partitions: 1,
        //         },
        //         source: in_progress.source,
        //         operators: in_progress.operators,
        //     };

        //     self.remote_group.pipelines.insert(pipeline.id, pipeline);
        //     self.local_group
        //         .pipelines
        //         .insert(final_pipeline.id, final_pipeline);
        // } else {
        //     let pipeline = IntermediatePipeline {
        //         id: in_progress.id,
        //         sink: PipelineSink::QueryOutput,
        //         source: in_progress.source,
        //         operators: in_progress.operators,
        //     };

        //     self.local_group.pipelines.insert(pipeline.id, pipeline);
        // }

        // Ok(())
    }
}

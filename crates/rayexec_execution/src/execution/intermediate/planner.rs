use crate::{
    database::create::{CreateSchemaInfo, CreateTableInfo},
    engine::vars::SessionVars,
    execution::{
        explain::format_logical_plan_for_explain,
        intermediate::PipelineSink,
        operators::{
            copy_to::PhysicalCopyTo,
            create_schema::PhysicalCreateSchema,
            create_table::PhysicalCreateTable,
            drop::PhysicalDrop,
            empty::PhysicalEmpty,
            filter::FilterOperation,
            hash_aggregate::PhysicalHashAggregate,
            insert::PhysicalInsert,
            join::{hash_join::PhysicalHashJoin, nl_join::PhysicalNestedLoopJoin},
            limit::PhysicalLimit,
            materialize::PhysicalMaterialize,
            project::ProjectOperation,
            scan::PhysicalScan,
            simple::SimpleOperator,
            sort::{local_sort::PhysicalLocalSort, merge_sorted::PhysicalMergeSortedInputs},
            table_function::PhysicalTableFunction,
            ungrouped_aggregate::PhysicalUngroupedAggregate,
            union::PhysicalUnion,
            values::PhysicalValues,
            PhysicalOperator,
        },
    },
    expr::{PhysicalAggregateExpression, PhysicalScalarExpression, PhysicalSortExpression},
    logical::{
        context::QueryContext,
        grouping_set::GroupingSets,
        operator::{self, LocationRequirement, LogicalNode, LogicalOperator},
    },
};
use rayexec_bullet::{
    array::{Array, Utf8Array},
    batch::Batch,
    compute::concat::concat,
    field::TypeSchema,
};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::{
    IntermediateOperator, IntermediatePipeline, IntermediatePipelineGroup, IntermediatePipelineId,
    PipelineSource, StreamId,
};

/// Configuration used during intermediate pipeline planning.
#[derive(Debug, Clone, Default)]
pub struct IntermediateConfig {
    /// Trigger an error if we attempt to plan a nested loop join.
    pub error_on_nested_loop_join: bool,
}

impl IntermediateConfig {
    pub fn from_session_vars(vars: &SessionVars) -> Self {
        IntermediateConfig {
            error_on_nested_loop_join: vars
                .get_var_expect("debug_error_on_nested_loop_join")
                .value
                .try_as_bool()
                .unwrap(),
        }
    }
}

/// Planned pipelines grouped into locations for where they should be executed.
#[derive(Debug)]
pub struct PlannedPipelineGroups {
    pub local: IntermediatePipelineGroup,
    pub remote: IntermediatePipelineGroup,
}

/// Planner for building intermedate pipelines.
///
/// Intermediate pipelines still retain some structure with which pipeline feeds
/// into another, but are not yet executable.
#[derive(Debug)]
pub struct IntermediatePipelinePlanner {
    config: IntermediateConfig,
}

impl IntermediatePipelinePlanner {
    pub fn new(config: IntermediateConfig) -> Self {
        IntermediatePipelinePlanner { config }
    }

    /// Plan the intermediate pipelines.
    pub fn plan_pipelines(
        &self,
        root: operator::LogicalOperator,
        context: QueryContext,
    ) -> Result<PlannedPipelineGroups> {
        let mut state = IntermediatePipelineBuildState::new(&self.config);
        let mut id_gen = PipelineIdGen::new(Uuid::new_v4());

        let mut materializations = state.plan_materializations(context, &mut id_gen)?;
        state.walk(&mut materializations, &mut id_gen, root)?;

        state.finish(&mut id_gen)?;

        debug_assert!(state.in_progress.is_none());

        Ok(PlannedPipelineGroups {
            local: state.local_group,
            remote: state.remote_group,
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

/// Key for a pipeline that's being materialized.
#[derive(Debug, Clone, Copy)]
enum MaterializationSource {
    Local(IntermediatePipelineId),
    Remote(IntermediatePipelineId),
}

#[derive(Debug, Default)]
struct Materializations {
    /// Source keys for `MaterializeScan` operators.
    ///
    /// Key corresponds to the index of the materialized plan in the
    /// QueryContext. Since multiple pipelines can read from the same
    /// materialization, each key has a vec of pipelines that we take from.
    materialize_sources: HashMap<usize, Vec<MaterializationSource>>,
}

impl Materializations {
    /// Checks if there's any pipelines still in the map.
    ///
    /// This is used as a debugging check. After planning the entire query, all
    /// pending pipelines should have been consumed. If there's still pipelines,
    /// that means we're not accuratately tracking the number of materialized
    /// scans.
    #[allow(dead_code)]
    fn has_remaining_pipelines(&self) -> bool {
        for pipelines in self.materialize_sources.values() {
            if !pipelines.is_empty() {
                return true;
            }
        }
        false
    }
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
    config: &'a IntermediateConfig,
    /// Pipeline we're working on, as well as the location for where it should
    /// be executed.
    in_progress: Option<InProgressPipeline>,
    /// Pipelines in the local group.
    local_group: IntermediatePipelineGroup,
    /// Pipelines in the remote group.
    remote_group: IntermediatePipelineGroup,
}

impl<'a> IntermediatePipelineBuildState<'a> {
    fn new(config: &'a IntermediateConfig) -> Self {
        IntermediatePipelineBuildState {
            config,
            in_progress: None,
            local_group: IntermediatePipelineGroup::default(),
            remote_group: IntermediatePipelineGroup::default(),
        }
    }

    /// Plans all materialized logical plans in the query context.
    ///
    /// For each materialized plan, this will do two things:
    ///
    /// 1. Build the complete pipeline representing a plan whose sink will be a
    ///    PhysicalMaterialize. This pipeline will be placed in one of the pipeline
    ///    in one of the pipeline groups.
    ///
    /// 2. Create materialization keys the correspond to the materialized
    ///    pipeline. When an operator encounters a materialization scan, it'll
    ///    look at the key to determine the pipeline source.
    ///
    /// A materialized plan may depend on earlier materialized plans. What gets
    /// returned is the set of materializations that should be used in the rest
    /// of the plan.
    fn plan_materializations(
        &mut self,
        context: QueryContext,
        id_gen: &mut PipelineIdGen,
    ) -> Result<Materializations> {
        let mut materializations = Materializations::default();

        for materialized in context.materialized {
            // Generate the pipeline(s) for this plan.
            self.walk(&mut materializations, id_gen, materialized.root)?;

            // Finish off the pipeline with a PhysicalMaterialize as the sink.
            let operator = IntermediateOperator {
                operator: Arc::new(PhysicalOperator::Materialize(PhysicalMaterialize::new(
                    materialized.num_scans,
                ))),
                partitioning_requirement: None,
            };

            let location = LocationRequirement::Any;
            self.push_intermediate_operator(operator, location, id_gen)?;

            let pipeline = self.take_in_progress_pipeline()?;
            let location = pipeline.location;

            let pipeline = IntermediatePipeline {
                id: pipeline.id,
                sink: PipelineSink::InPipeline,
                source: pipeline.source,
                operators: pipeline.operators,
            };

            let id = pipeline.id;
            let source = match location {
                LocationRequirement::ClientLocal => {
                    self.local_group.pipelines.insert(id, pipeline);
                    MaterializationSource::Local(id)
                }
                LocationRequirement::Remote => {
                    self.remote_group.pipelines.insert(id, pipeline);
                    MaterializationSource::Remote(id)
                }
                LocationRequirement::Any => {
                    self.local_group.pipelines.insert(id, pipeline);
                    MaterializationSource::Local(id)
                }
            };

            let sources: Vec<_> = (0..materialized.num_scans).map(|_| source).collect();
            materializations
                .materialize_sources
                .insert(materialized.idx, sources);
        }

        Ok(materializations)
    }

    fn walk(
        &mut self,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        plan: LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Projection(proj) => self.push_project(id_gen, materializations, proj),
            LogicalOperator::Filter(filter) => self.push_filter(id_gen, materializations, filter),
            LogicalOperator::ExpressionList(values) => self.push_values(id_gen, values),
            LogicalOperator::CrossJoin(join) => {
                self.push_cross_join(id_gen, materializations, join)
            }
            LogicalOperator::AnyJoin(join) => self.push_any_join(id_gen, materializations, join),
            LogicalOperator::EqualityJoin(join) => {
                self.push_equality_join(id_gen, materializations, join)
            }
            LogicalOperator::DependentJoin(_join) => Err(RayexecError::new(
                "Dependent joins cannot be made into a pipeline",
            )),
            LogicalOperator::Empty(empty) => self.push_empty(id_gen, empty),
            LogicalOperator::Aggregate(agg) => self.push_aggregate(id_gen, materializations, agg),
            LogicalOperator::Limit(limit) => self.push_limit(id_gen, materializations, limit),
            LogicalOperator::Order(order) => self.push_global_sort(id_gen, materializations, order),
            LogicalOperator::ShowVar(show_var) => self.push_show_var(id_gen, show_var),
            LogicalOperator::Explain(explain) => {
                self.push_explain(id_gen, materializations, explain)
            }
            LogicalOperator::Describe(describe) => self.push_describe(id_gen, describe),
            LogicalOperator::CreateTable(create) => {
                self.push_create_table(id_gen, materializations, create)
            }
            LogicalOperator::CreateSchema(create) => self.push_create_schema(id_gen, create),
            LogicalOperator::Drop(drop) => self.push_drop(id_gen, drop),
            LogicalOperator::Insert(insert) => self.push_insert(id_gen, materializations, insert),
            LogicalOperator::CopyTo(copy_to) => {
                self.push_copy_to(id_gen, materializations, copy_to)
            }
            LogicalOperator::MaterializedScan(scan) => {
                self.push_materialized_scan(materializations, id_gen, scan)
            }
            LogicalOperator::Scan(scan) => self.push_scan(id_gen, scan),
            LogicalOperator::TableFunction(table_func) => {
                self.push_table_function(id_gen, table_func)
            }
            LogicalOperator::SetOperation(setop) => {
                self.push_set_operation(id_gen, materializations, setop)
            }
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

        let child_pipeline = IntermediatePipeline {
            id: child.id,
            sink: PipelineSink::InGroup {
                pipeline_id: in_progress.id,
                operator_idx: in_progress.operators.len() - 1,
                input_idx,
            },
            source: child.source,
            operators: child.operators,
        };

        match child.location {
            LocationRequirement::ClientLocal => {
                self.local_group
                    .pipelines
                    .insert(child_pipeline.id, child_pipeline);
            }
            LocationRequirement::Remote => {
                self.remote_group
                    .pipelines
                    .insert(child_pipeline.id, child_pipeline);
            }
            LocationRequirement::Any => {
                // TODO: Determine if any should be allowed here.
                self.local_group
                    .pipelines
                    .insert(child_pipeline.id, child_pipeline);
            }
        }

        Ok(())
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
            // Different locations, finalize in-progress and start a new one.
            let in_progress = self.take_in_progress_pipeline()?;

            let stream_id = id_gen.new_stream_id();

            let new_in_progress = InProgressPipeline {
                id: id_gen.next_pipeline_id(),
                operators: vec![operator],
                location,
                // TODO: partitions? include other pipeline id
                source: PipelineSource::OtherGroup {
                    stream_id,
                    partitions: 1,
                },
            };

            let finalized = IntermediatePipeline {
                id: in_progress.id,
                // TODO: partitions? include other pipeline id
                sink: PipelineSink::OtherGroup {
                    stream_id,
                    partitions: 1,
                },
                source: in_progress.source,
                operators: in_progress.operators,
            };

            match in_progress.location {
                LocationRequirement::ClientLocal => {
                    self.local_group.pipelines.insert(finalized.id, finalized);
                }
                LocationRequirement::Remote => {
                    self.remote_group.pipelines.insert(finalized.id, finalized);
                }
                LocationRequirement::Any => {
                    self.local_group.pipelines.insert(finalized.id, finalized);
                }
            }

            self.in_progress = Some(new_in_progress)
        }

        Ok(())
    }

    fn finish(&mut self, id_gen: &mut PipelineIdGen) -> Result<()> {
        let mut in_progress = self.take_in_progress_pipeline()?;
        if in_progress.location == LocationRequirement::Any {
            in_progress.location = LocationRequirement::ClientLocal;
        }

        if in_progress.location != LocationRequirement::ClientLocal {
            let stream_id = id_gen.new_stream_id();

            let final_pipeline = IntermediatePipeline {
                id: id_gen.next_pipeline_id(),
                sink: PipelineSink::QueryOutput,
                source: PipelineSource::OtherGroup {
                    stream_id,
                    partitions: 1,
                },
                operators: Vec::new(),
            };

            let pipeline = IntermediatePipeline {
                id: in_progress.id,
                sink: PipelineSink::OtherGroup {
                    stream_id,
                    partitions: 1,
                },
                source: in_progress.source,
                operators: in_progress.operators,
            };

            self.remote_group.pipelines.insert(pipeline.id, pipeline);
            self.local_group
                .pipelines
                .insert(final_pipeline.id, final_pipeline);
        } else {
            let pipeline = IntermediatePipeline {
                id: in_progress.id,
                sink: PipelineSink::QueryOutput,
                source: in_progress.source,
                operators: in_progress.operators,
            };

            self.local_group.pipelines.insert(pipeline.id, pipeline);
        }

        Ok(())
    }

    fn push_copy_to(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        copy_to: LogicalNode<operator::CopyTo>,
    ) -> Result<()> {
        let location = copy_to.location;
        let copy_to = copy_to.into_inner();

        self.walk(materializations, id_gen, *copy_to.source)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CopyTo(PhysicalCopyTo::new(
                copy_to.copy_to,
                copy_to.source_schema,
                copy_to.location,
            ))),
            // This should be temporary until there's a better understanding of
            // how we want to handle parallel writes.
            partitioning_requirement: Some(1),
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_set_operation(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        setop: LogicalNode<operator::SetOperation>,
    ) -> Result<()> {
        let location = setop.location;
        let setop = setop.into_inner();

        // Schema from the top. Used as the input to a GROUP BY if ALL is
        // omitted.
        let top_schema = setop.top.output_schema(&[])?;

        // Continue building top.
        self.walk(materializations, id_gen, *setop.top)?;

        // Create new pipelines for bottom.
        let mut bottom_builder = IntermediatePipelineBuildState::new(self.config);
        bottom_builder.walk(materializations, id_gen, *setop.bottom)?;
        self.local_group
            .merge_from_other(&mut bottom_builder.local_group);
        self.remote_group
            .merge_from_other(&mut bottom_builder.remote_group);

        let bottom_in_progress = bottom_builder.take_in_progress_pipeline()?;

        match setop.kind {
            operator::SetOpKind::Union => {
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Union(PhysicalUnion)),
                    partitioning_requirement: None,
                };

                self.push_intermediate_operator(operator, location, id_gen)?;

                // The union operator is the "sink" for the bottom pipeline.
                self.push_as_child_pipeline(bottom_in_progress, 1)?;
            }
            other => not_implemented!("set op {other}"),
        }

        // Make output distinct by grouping on all columns. No output
        // aggregates, so the output schema remains the same.
        if !setop.all {
            let grouping_sets =
                GroupingSets::new_for_group_by((0..top_schema.types.len()).collect());
            let group_types = top_schema.types;

            let operator =
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(group_types, grouping_sets, Vec::new()),
                    )),
                    partitioning_requirement: None,
                };

            self.push_intermediate_operator(operator, location, id_gen)?;
        }

        Ok(())
    }

    fn push_drop(
        &mut self,
        id_gen: &mut PipelineIdGen,
        drop: LogicalNode<operator::DropEntry>,
    ) -> Result<()> {
        let location = drop.location;
        let drop = drop.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Drop(PhysicalDrop::new(drop.info))),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_insert(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        insert: LogicalNode<operator::Insert>,
    ) -> Result<()> {
        let location = insert.location;
        let insert = insert.into_inner();

        self.walk(materializations, id_gen, *insert.input)?;

        // TODO: Need a "resolved" type on the logical operator that gets us the catalog/schema.
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Insert(PhysicalInsert::new(
                "temp",
                "temp",
                insert.table,
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_table_function(
        &mut self,
        id_gen: &mut PipelineIdGen,
        table_func: LogicalNode<operator::TableFunction>,
    ) -> Result<()> {
        let location = table_func.location;
        let table_func = table_func.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::TableFunction(PhysicalTableFunction::new(
                table_func.function,
            ))),
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_materialized_scan(
        &mut self,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        scan: LogicalNode<operator::MaterializedScan>,
    ) -> Result<()> {
        // TODO: Do we care? Currently just defaulting to the materialization
        // location.
        let _location = scan.location;

        let scan = scan.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let (source_id, location) = match materializations.materialize_sources.get_mut(&scan.idx) {
            Some(sources) => {
                let source = sources.pop().required("materialization source key")?;
                match source {
                    MaterializationSource::Local(id) => (id, LocationRequirement::ClientLocal),
                    MaterializationSource::Remote(id) => (id, LocationRequirement::Remote),
                }
            }
            None => {
                return Err(RayexecError::new(format!(
                    "Missing pipelines for materialized plan at index {}",
                    scan.idx
                )))
            }
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: Vec::new(),
            location,
            source: PipelineSource::OtherPipeline {
                pipeline: source_id,
            },
        });

        Ok(())
    }

    fn push_scan(
        &mut self,
        id_gen: &mut PipelineIdGen,
        scan: LogicalNode<operator::Scan>,
    ) -> Result<()> {
        let location = scan.location;
        let scan = scan.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Scan(PhysicalScan::new(
                scan.catalog,
                scan.schema,
                scan.source,
            ))),
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_create_schema(
        &mut self,
        id_gen: &mut PipelineIdGen,
        create: LogicalNode<operator::CreateSchema>,
    ) -> Result<()> {
        let location = create.location;
        let create = create.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CreateSchema(PhysicalCreateSchema::new(
                create.catalog,
                CreateSchemaInfo {
                    name: create.name,
                    on_conflict: create.on_conflict,
                },
            ))),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_create_table(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        create: LogicalNode<operator::CreateTable>,
    ) -> Result<()> {
        let location = create.location;
        let create = create.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let is_ctas = create.input.is_some();
        match create.input {
            Some(input) => {
                // CTAS, plan the input. It'll be the source of this pipeline.
                self.walk(materializations, id_gen, *input)?;
            }
            None => {
                // No input, just have an empty operator as the source.
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Empty(PhysicalEmpty)),
                    partitioning_requirement: Some(1),
                };

                self.in_progress = Some(InProgressPipeline {
                    id: id_gen.next_pipeline_id(),
                    operators: vec![operator],
                    location,
                    source: PipelineSource::InPipeline,
                });
            }
        };

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CreateTable(PhysicalCreateTable::new(
                create.catalog,
                create.schema,
                CreateTableInfo {
                    name: create.name,
                    columns: create.columns,
                    on_conflict: create.on_conflict,
                },
                is_ctas,
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_describe(
        &mut self,
        id_gen: &mut PipelineIdGen,
        describe: LogicalNode<operator::Describe>,
    ) -> Result<()> {
        let location = describe.location;
        let describe = describe.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let names = Array::Utf8(Utf8Array::from_iter(
            describe.schema.iter().map(|f| f.name.as_str()),
        ));
        let datatypes = Array::Utf8(Utf8Array::from_iter(
            describe.schema.iter().map(|f| f.datatype.to_string()),
        ));
        let batch = Batch::try_new(vec![names, datatypes])?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![batch]))),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_explain(
        &mut self,
        id_gen: &mut PipelineIdGen,
        _materializations: &mut Materializations,
        explain: LogicalNode<operator::Explain>,
    ) -> Result<()> {
        let location = explain.location;
        let explain = explain.into_inner();

        if explain.analyze {
            not_implemented!("explain analyze")
        }

        let formatted_logical =
            format_logical_plan_for_explain(None, &explain.input, explain.format, explain.verbose)?;

        // TODO: Executable, intermediate (w/ location)

        let physical = Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![
            Batch::try_new(vec![
                Array::Utf8(Utf8Array::from_iter(["logical"])),
                Array::Utf8(Utf8Array::from_iter([formatted_logical.as_str()])),
            ])?,
        ])));

        let operator = IntermediateOperator {
            operator: physical,
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_show_var(
        &mut self,
        id_gen: &mut PipelineIdGen,
        show: LogicalNode<operator::ShowVar>,
    ) -> Result<()> {
        let location = show.location;
        let show = show.into_inner();

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![
                Batch::try_new(vec![Array::Utf8(Utf8Array::from_iter([show
                    .var
                    .value
                    .to_string()
                    .as_str()]))])?,
            ]))),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    fn push_project(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        project: LogicalNode<operator::Projection>,
    ) -> Result<()> {
        let input_schema = project.as_ref().input.output_schema(&[])?;
        let location = project.location;
        let project = project.into_inner();
        self.walk(materializations, id_gen, *project.input)?;

        let projections = project
            .exprs
            .into_iter()
            .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, &input_schema))
            .collect::<Result<Vec<_>>>()?;
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Project(SimpleOperator::new(
                ProjectOperation::new(projections),
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_filter(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        filter: LogicalNode<operator::Filter>,
    ) -> Result<()> {
        let input_schema = filter.as_ref().input.output_schema(&[])?;
        let location = filter.location;
        let filter = filter.into_inner();
        self.walk(materializations, id_gen, *filter.input)?;

        let predicate =
            PhysicalScalarExpression::try_from_uncorrelated_expr(filter.predicate, &input_schema)?;
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Filter(SimpleOperator::new(
                FilterOperation::new(predicate),
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_global_sort(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        order: LogicalNode<operator::Order>,
    ) -> Result<()> {
        let location = order.location;
        let order = order.into_inner();

        let input_schema = order.input.output_schema(&[])?;
        self.walk(materializations, id_gen, *order.input)?;

        let exprs = order
            .exprs
            .into_iter()
            .map(|order_expr| {
                PhysicalSortExpression::try_from_uncorrelated_expr(
                    order_expr.expr,
                    &input_schema,
                    order_expr.desc,
                    order_expr.nulls_first,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // Partition-local sorting.
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::LocalSort(PhysicalLocalSort::new(
                exprs.clone(),
            ))),
            partitioning_requirement: None,
        };
        self.push_intermediate_operator(operator, location, id_gen)?;

        // Global sorting.
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::MergeSorted(
                PhysicalMergeSortedInputs::new(exprs),
            )),
            partitioning_requirement: None,
        };
        self.push_intermediate_operator(operator, location, id_gen)?;

        // Global sorting accepts n-partitions, but produces only a single
        // partition. We finish the current pipeline

        // TODO: Actually enforce that.

        let in_progress = self.take_in_progress_pipeline()?;
        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: Vec::new(),
            location,
            // TODO:
            source: PipelineSource::OtherPipeline {
                pipeline: in_progress.id,
            },
        });

        let pipeline = IntermediatePipeline {
            id: in_progress.id,
            sink: PipelineSink::InPipeline,
            source: in_progress.source,
            operators: in_progress.operators,
        };
        match location {
            LocationRequirement::ClientLocal => {
                self.local_group.pipelines.insert(pipeline.id, pipeline);
            }
            LocationRequirement::Remote => {
                self.remote_group.pipelines.insert(pipeline.id, pipeline);
            }
            LocationRequirement::Any => {
                // TODO
                self.local_group.pipelines.insert(pipeline.id, pipeline);
            }
        }

        Ok(())
    }

    fn push_limit(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        limit: LogicalNode<operator::Limit>,
    ) -> Result<()> {
        let location = limit.location;
        let limit = limit.into_inner();

        self.walk(materializations, id_gen, *limit.input)?;

        // TODO: Who sets partitioning? How was that working before?

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Limit(PhysicalLimit::new(
                limit.limit,
                limit.offset,
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_aggregate(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        agg: LogicalNode<operator::Aggregate>,
    ) -> Result<()> {
        let location = agg.location;
        let agg = agg.into_inner();

        let input_schema = agg.input.output_schema(&[])?;
        self.walk(materializations, id_gen, *agg.input)?;

        let mut agg_exprs = Vec::with_capacity(agg.aggregates.len());
        for expr in agg.aggregates.into_iter() {
            let agg_expr =
                PhysicalAggregateExpression::try_from_logical_expression(expr, &input_schema)?;
            agg_exprs.push(agg_expr);
        }

        match agg.grouping_sets {
            Some(grouping_sets) => {
                // If we're working with groups, push a hash aggregate operator.

                let group_types: Vec<_> = grouping_sets
                    .columns()
                    .iter()
                    .map(|idx| input_schema.types.get(*idx).expect("type to exist").clone())
                    .collect();

                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(group_types, grouping_sets, agg_exprs),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
            None => {
                // Otherwise push an ungrouped aggregate operator.

                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::UngroupedAggregate(
                        PhysicalUngroupedAggregate::new(agg_exprs),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
        };

        Ok(())
    }

    fn push_empty(&mut self, id_gen: &mut PipelineIdGen, empty: LogicalNode<()>) -> Result<()> {
        // "Empty" is a source of data by virtue of emitting a batch consisting
        // of no columns and 1 row.
        //
        // This enables expression evualtion to work without needing to special
        // case a query without a FROM clause. E.g. `SELECT 1+1` would execute
        // the expression `1+1` with the input being the batch with 1 row and no
        // columns.
        //
        // Because this this batch is really just to drive execution on an
        // expression with no input, we just hard the partitions for this
        // pipeline to 1.
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // This has a partitioning requirement of 1 since it's only used to
        // drive output of a query that contains no FROM (typically just a
        // simple projection).
        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Empty(PhysicalEmpty)),
            partitioning_requirement: Some(1),
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location: empty.location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }

    /// Push an equality (hash) join.
    fn push_equality_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: LogicalNode<operator::EqualityJoin>,
    ) -> Result<()> {
        let location = join.location;
        let join = join.into_inner();

        // Build up all inputs on the right (probe) side. This is going to
        // continue with the the current pipeline.
        self.walk(materializations, id_gen, *join.right)?;

        // Build up the left (build) side in a separate pipeline. This will feed
        // into the currently pipeline at the join operator.
        let mut left_state = IntermediatePipelineBuildState::new(self.config);
        left_state.walk(materializations, id_gen, *join.left)?;

        // Take any completed pipelines from the left side and put them in our
        // list.
        self.local_group
            .merge_from_other(&mut left_state.local_group);
        self.remote_group
            .merge_from_other(&mut left_state.remote_group);

        // Get the left pipeline.
        let left_pipeline = left_state.in_progress.take().ok_or_else(|| {
            RayexecError::new("expected in-progress pipeline from left side of join")
        })?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::HashJoin(PhysicalHashJoin::new(
                join.join_type,
                join.left_on,
                join.right_on,
            ))),
            partitioning_requirement: None,
        };
        self.push_intermediate_operator(operator, location, id_gen)?;

        // Left pipeline will be child this this pipeline at the current
        // operator.
        self.push_as_child_pipeline(left_pipeline, PhysicalHashJoin::BUILD_SIDE_INPUT_INDEX)?;

        Ok(())
    }

    fn push_any_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: LogicalNode<operator::AnyJoin>,
    ) -> Result<()> {
        let location = join.location;
        let join = join.into_inner();

        let left_schema = join.left.output_schema(&[])?;
        let right_schema = join.right.output_schema(&[])?;
        let input_schema = left_schema.merge(right_schema);
        let filter = PhysicalScalarExpression::try_from_uncorrelated_expr(join.on, &input_schema)?;

        // Modify the filter as to match the join type.
        let filter = match join.join_type {
            operator::JoinType::Inner => filter,
            other => {
                // TODO: Other join types.
                return Err(RayexecError::new(format!(
                    "Unhandled join type for any join: {other:?}"
                )));
            }
        };

        self.push_nl_join(
            id_gen,
            materializations,
            location,
            *join.left,
            *join.right,
            Some(filter),
        )
    }

    fn push_cross_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: LogicalNode<operator::CrossJoin>,
    ) -> Result<()> {
        let location = join.location;
        let join = join.into_inner();
        self.push_nl_join(
            id_gen,
            materializations,
            location,
            *join.left,
            *join.right,
            None,
        )
    }

    /// Push a nest loop join.
    ///
    /// This will create a complete pipeline for the left side of the join
    /// (build), right right side (probe) will be pushed onto the current
    /// pipeline.
    fn push_nl_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        location: LocationRequirement,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
    ) -> Result<()> {
        if self.config.error_on_nested_loop_join {
            return Err(RayexecError::new("Debug trigger: nested loop join"));
        }

        // Continue to build up all the inputs into the right side.
        self.walk(materializations, id_gen, right)?;

        // Create a completely independent pipeline (or pipelines) for left
        // side.
        let mut left_state = IntermediatePipelineBuildState::new(self.config);
        left_state.walk(materializations, id_gen, left)?;

        // Take completed pipelines from the left and merge them into this
        // state's completed set of pipelines.
        self.local_group
            .merge_from_other(&mut left_state.local_group);
        self.remote_group
            .merge_from_other(&mut left_state.remote_group);

        // Get the left in-progress pipeline. This will be one of the inputs
        // into the current in-progress pipeline.
        let left_pipeline = left_state.in_progress.take().ok_or_else(|| {
            RayexecError::new("expected in-progress pipeline from left side of join")
        })?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::NestedLoopJoin(
                PhysicalNestedLoopJoin::new(filter),
            )),
            partitioning_requirement: None,
        };
        self.push_intermediate_operator(operator, location, id_gen)?;

        // Left pipeline will be input to this pipeline.
        self.push_as_child_pipeline(
            left_pipeline,
            PhysicalNestedLoopJoin::BUILD_SIDE_INPUT_INDEX,
        )?;

        Ok(())
    }

    fn push_values(
        &mut self,
        id_gen: &mut PipelineIdGen,
        values: LogicalNode<operator::ExpressionList>,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: This could probably be simplified.

        let location = values.location;
        let values = values.into_inner();

        let mut row_arrs: Vec<Vec<Arc<Array>>> = Vec::new(); // Row oriented.
        let dummy_batch = Batch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in values.rows {
            let exprs = row_exprs
                .into_iter()
                .map(|expr| {
                    PhysicalScalarExpression::try_from_uncorrelated_expr(expr, &TypeSchema::empty())
                })
                .collect::<Result<Vec<_>>>()?;
            let arrs = exprs
                .into_iter()
                .map(|expr| expr.eval(&dummy_batch))
                .collect::<Result<Vec<_>>>()?;
            row_arrs.push(arrs);
        }

        let num_cols = row_arrs.first().map(|row| row.len()).unwrap_or(0);
        let mut col_arrs = Vec::with_capacity(num_cols); // Column oriented.

        // Convert the row-oriented vector into a column oriented one.
        for _ in 0..num_cols {
            let cols: Vec<_> = row_arrs.iter_mut().map(|row| row.pop().unwrap()).collect();
            col_arrs.push(cols);
        }

        // Reverse since we worked from right to left when converting to
        // column-oriented.
        col_arrs.reverse();

        // Concat column values into a single array.
        let mut cols = Vec::with_capacity(col_arrs.len());
        for arrs in col_arrs {
            let refs: Vec<&Array> = arrs.iter().map(|a| a.as_ref()).collect();
            let col = concat(&refs)?;
            cols.push(col);
        }

        let batch = Batch::try_new(cols)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![batch]))),
            partitioning_requirement: None,
        };

        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: vec![operator],
            location,
            source: PipelineSource::InPipeline,
        });

        Ok(())
    }
}

use crate::{
    database::create::{CreateSchemaInfo, CreateTableInfo},
    engine::vars::SessionVars,
    execution::{
        intermediate::PipelineSink,
        operators::{
            copy_to::CopyToOperation,
            create_schema::PhysicalCreateSchema,
            create_table::CreateTableSinkOperation,
            drop::PhysicalDrop,
            empty::PhysicalEmpty,
            filter::FilterOperation,
            hash_aggregate::PhysicalHashAggregate,
            hash_join::PhysicalHashJoin,
            insert::InsertOperation,
            limit::PhysicalLimit,
            nl_join::PhysicalNestedLoopJoin,
            project::{PhysicalProject, ProjectOperation},
            scan::PhysicalScan,
            simple::SimpleOperator,
            sink::SinkOperator,
            sort::{local_sort::PhysicalLocalSort, merge_sorted::PhysicalMergeSortedInputs},
            table_function::PhysicalTableFunction,
            ungrouped_aggregate::PhysicalUngroupedAggregate,
            union::PhysicalUnion,
            values::PhysicalValues,
            PhysicalOperator,
        },
    },
    explain::{explainable::ExplainConfig, formatter::ExplainFormatter},
    expr::{
        comparison_expr::ComparisonOperator,
        physical::{
            column_expr::PhysicalColumnExpr, planner::PhysicalExpressionPlanner,
            scalar_function_expr::PhysicalScalarFunctionExpr, PhysicalAggregateExpression,
            PhysicalScalarExpression,
        },
        Expression,
    },
    functions::scalar::boolean::AndImpl,
    logical::{
        binder::bind_context::BindContext,
        logical_aggregate::LogicalAggregate,
        logical_copy::LogicalCopyTo,
        logical_create::{LogicalCreateSchema, LogicalCreateTable},
        logical_describe::LogicalDescribe,
        logical_distinct::LogicalDistinct,
        logical_drop::LogicalDrop,
        logical_empty::LogicalEmpty,
        logical_explain::LogicalExplain,
        logical_filter::LogicalFilter,
        logical_insert::LogicalInsert,
        logical_join::{JoinType, LogicalArbitraryJoin, LogicalComparisonJoin, LogicalCrossJoin},
        logical_limit::LogicalLimit,
        logical_materialization::LogicalMaterializationScan,
        logical_order::LogicalOrder,
        logical_project::LogicalProject,
        logical_scan::{LogicalScan, ScanSource},
        logical_set::LogicalShowVar,
        logical_setop::{LogicalSetop, SetOpKind},
        operator::{self, LocationRequirement, LogicalNode, LogicalOperator, Node},
    },
};
use rayexec_bullet::{
    array::{Array, Utf8Array},
    batch::Batch,
    compute::concat::concat,
};
use rayexec_error::{not_implemented, OptionExt, RayexecError, Result, ResultExt};
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

use super::{
    IntermediateMaterialization, IntermediateMaterializationGroup, IntermediateOperator,
    IntermediatePipeline, IntermediatePipelineGroup, IntermediatePipelineId, PipelineSource,
    StreamId,
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
    pub materializations: IntermediateMaterializationGroup,
}

/// Planner for building intermedate pipelines.
///
/// Intermediate pipelines still retain some structure with which pipeline feeds
/// into another, but are not yet executable.
#[derive(Debug)]
pub struct IntermediatePipelinePlanner {
    config: IntermediateConfig,
    query_id: Uuid,
}

impl IntermediatePipelinePlanner {
    pub fn new(config: IntermediateConfig, query_id: Uuid) -> Self {
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
    config: &'a IntermediateConfig,
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
    bind_context: &'a BindContext,
    /// Expression planner for converting logical to physical expressions.
    expr_planner: PhysicalExpressionPlanner<'a>,
}

impl<'a> IntermediatePipelineBuildState<'a> {
    fn new(config: &'a IntermediateConfig, bind_context: &'a BindContext) -> Self {
        let expr_planner = PhysicalExpressionPlanner::new(bind_context);

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

    fn walk(
        &mut self,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        plan: LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Project(proj) => self.push_project(id_gen, materializations, proj),
            LogicalOperator::Filter(filter) => self.push_filter(id_gen, materializations, filter),
            LogicalOperator::Distinct(distinct) => {
                self.push_distinct(id_gen, materializations, distinct)
            }
            LogicalOperator::CrossJoin(join) => {
                self.push_cross_join(id_gen, materializations, join)
            }
            LogicalOperator::ArbitraryJoin(join) => {
                self.push_arbitrary_join(id_gen, materializations, join)
            }
            LogicalOperator::ComparisonJoin(join) => {
                self.push_comparison_join(id_gen, materializations, join)
            }
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
            LogicalOperator::MaterializationScan(scan) => {
                self.push_materialize_scan(id_gen, materializations, scan)
            }
            LogicalOperator::Scan(scan) => self.push_scan(id_gen, scan),
            LogicalOperator::SetOp(setop) => {
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
                source: PipelineSource::OtherGroup {
                    stream_id,
                    partitions: 1,
                },
            };

            let finalized = IntermediatePipeline {
                id: in_progress.id,
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
        mut copy_to: Node<LogicalCopyTo>,
    ) -> Result<()> {
        let location = copy_to.location;
        let source = copy_to.take_one_child_exact()?;

        self.walk(materializations, id_gen, source)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CopyTo(SinkOperator::new(
                CopyToOperation {
                    copy_to: copy_to.node.copy_to,
                    location: copy_to.node.location,
                    schema: copy_to.node.source_schema,
                },
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
        mut setop: Node<LogicalSetop>,
    ) -> Result<()> {
        let location = setop.location;

        let [left, right] = setop.take_two_children_exact()?;
        let top = left;
        let bottom = right;

        // Continue building left/top.
        self.walk(materializations, id_gen, top)?;

        // Create new pipelines for bottom.
        let mut bottom_builder =
            IntermediatePipelineBuildState::new(self.config, self.bind_context);
        bottom_builder.walk(materializations, id_gen, bottom)?;
        self.local_group
            .merge_from_other(&mut bottom_builder.local_group);
        self.remote_group
            .merge_from_other(&mut bottom_builder.remote_group);

        let bottom_in_progress = bottom_builder.take_in_progress_pipeline()?;

        match setop.node.kind {
            SetOpKind::Union => {
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
        if !setop.node.all {
            let output_types = self
                .bind_context
                .get_table(setop.node.table_ref)?
                .column_types
                .clone();

            let grouping_sets = vec![(0..output_types.len()).collect()];

            let operator =
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(output_types, Vec::new(), grouping_sets),
                    )),
                    partitioning_requirement: None,
                };

            self.push_intermediate_operator(operator, location, id_gen)?;
        }

        Ok(())
    }

    fn push_drop(&mut self, id_gen: &mut PipelineIdGen, drop: Node<LogicalDrop>) -> Result<()> {
        let location = drop.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Drop(PhysicalDrop::new(
                drop.node.catalog,
                drop.node.info,
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

    fn push_insert(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut insert: Node<LogicalInsert>,
    ) -> Result<()> {
        let location = insert.location;
        let input = insert.take_one_child_exact()?;

        self.walk(materializations, id_gen, input)?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Insert(SinkOperator::new(
                InsertOperation {
                    catalog: insert.node.catalog,
                    schema: insert.node.schema,
                    table: insert.node.table,
                },
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_materialize_scan(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        scan: Node<LogicalMaterializationScan>,
    ) -> Result<()> {
        if !materializations
            .local
            .materializations
            .contains_key(&scan.node.mat)
        {
            return Err(RayexecError::new(format!(
                "Missing materialization for ref: {}",
                scan.node.mat
            )));
        }

        if self.in_progress.is_some() {
            return Err(RayexecError::new(
                "Expected in progress to be None for materialization scan",
            ));
        }

        // Initialize in-progress with no operators, but scan source being this
        // materialization.
        self.in_progress = Some(InProgressPipeline {
            id: id_gen.next_pipeline_id(),
            operators: Vec::new(),
            location: LocationRequirement::ClientLocal, // Currently only support local.
            source: PipelineSource::Materialization {
                mat_ref: scan.node.mat,
            },
        });

        Ok(())
    }

    fn push_scan(&mut self, id_gen: &mut PipelineIdGen, scan: Node<LogicalScan>) -> Result<()> {
        let location = scan.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: use this.
        let _projections = scan.node.projection;

        let operator = match scan.node.source {
            ScanSource::Table {
                catalog,
                schema,
                source,
            } => IntermediateOperator {
                operator: Arc::new(PhysicalOperator::Scan(PhysicalScan::new(
                    catalog, schema, source,
                ))),
                partitioning_requirement: None,
            },
            ScanSource::TableFunction { function } => IntermediateOperator {
                operator: Arc::new(PhysicalOperator::TableFunction(PhysicalTableFunction::new(
                    function,
                ))),
                partitioning_requirement: None,
            },
            ScanSource::ExpressionList { rows } => {
                let batch = self.create_batch_for_row_values(rows)?;
                IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![batch]))),
                    partitioning_requirement: None,
                }
            }
            ScanSource::View { .. } => not_implemented!("view physical planning"),
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
        create: Node<LogicalCreateSchema>,
    ) -> Result<()> {
        let location = create.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CreateSchema(PhysicalCreateSchema::new(
                create.node.catalog,
                CreateSchemaInfo {
                    name: create.node.name,
                    on_conflict: create.node.on_conflict,
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
        mut create: Node<LogicalCreateTable>,
    ) -> Result<()> {
        let location = create.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let input = match create.children.len() {
            1 | 0 => create.children.pop(),
            other => {
                return Err(RayexecError::new(format!(
                    "Create table has more than one child: {other}",
                )))
            }
        };

        let is_ctas = input.is_some();
        match input {
            Some(input) => {
                // CTAS, plan the input. It'll be the source of this pipeline.
                self.walk(materializations, id_gen, input)?;
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
            operator: Arc::new(PhysicalOperator::CreateTable(SinkOperator::new(
                CreateTableSinkOperation {
                    catalog: create.node.catalog,
                    schema: create.node.schema,
                    info: CreateTableInfo {
                        name: create.node.name,
                        columns: create.node.columns,
                        on_conflict: create.node.on_conflict,
                    },
                    is_ctas,
                },
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_describe(
        &mut self,
        id_gen: &mut PipelineIdGen,
        describe: Node<LogicalDescribe>,
    ) -> Result<()> {
        let location = describe.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let names = Array::Utf8(Utf8Array::from_iter(
            describe.node.schema.iter().map(|f| f.name.as_str()),
        ));
        let datatypes = Array::Utf8(Utf8Array::from_iter(
            describe.node.schema.iter().map(|f| f.datatype.to_string()),
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
        materializations: &mut Materializations,
        mut explain: Node<LogicalExplain>,
    ) -> Result<()> {
        let location = explain.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        if explain.node.analyze {
            not_implemented!("explain analyze")
        }

        // Plan in seperate planner to avoid conmingling pipelines we will be
        // executing (the explain) with the ones we won't (the explained plan
        // itself).
        let input = explain.take_one_child_exact()?;
        let mut planner = Self::new(self.config, self.bind_context);
        // Done in a closure so that we can at least output the logical plans is
        // physical planning errors. This is entirely for dev purposes right now
        // and I expect the conditional will be removed at some point.
        let plan = || {
            planner.walk(materializations, id_gen, input)?;
            planner.finish(id_gen)?;
            Ok::<_, RayexecError>(())
        };
        let plan_result = plan();

        let formatter = ExplainFormatter::new(
            self.bind_context,
            ExplainConfig {
                verbose: explain.node.verbose,
            },
            explain.node.format,
        );

        let mut type_strings = Vec::new();
        let mut plan_strings = Vec::new();

        type_strings.push("unoptimized".to_string());
        plan_strings.push(formatter.format_logical_plan(&explain.node.logical_unoptimized)?);

        if let Some(optimized) = explain.node.logical_optimized {
            type_strings.push("optimized".to_string());
            plan_strings.push(formatter.format_logical_plan(&optimized)?);
        }

        match plan_result {
            Ok(_) => {
                type_strings.push("physical".to_string());
                plan_strings.push(formatter.format_intermedate_groups(&[
                    ("local", &planner.local_group),
                    ("remote", &planner.remote_group),
                ])?);
            }
            Err(e) => {
                error!(%e, "error planning explain input")
            }
        }

        let physical = Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![
            Batch::try_new(vec![
                Array::Utf8(Utf8Array::from(type_strings)),
                Array::Utf8(Utf8Array::from(plan_strings)),
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
        show: Node<LogicalShowVar>,
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
        mut project: Node<LogicalProject>,
    ) -> Result<()> {
        let location = project.location;

        let input = project.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs();
        self.walk(materializations, id_gen, input)?;

        let projections = self
            .expr_planner
            .plan_scalars(&input_refs, &project.node.projections)
            .context("Failed to plan expressions for projection")?;

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
        mut filter: Node<LogicalFilter>,
    ) -> Result<()> {
        let location = filter.location;

        let input = filter.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs();
        self.walk(materializations, id_gen, input)?;

        let predicate = self
            .expr_planner
            .plan_scalar(&input_refs, &filter.node.filter)
            .context("Failed to plan expressions for filter")?;

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Filter(SimpleOperator::new(
                FilterOperation::new(predicate),
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }

    fn push_distinct(
        &mut self,
        _id_gen: &mut PipelineIdGen,
        _materializations: &mut Materializations,
        _distinct: Node<LogicalDistinct>,
    ) -> Result<()> {
        // TODO: https://github.com/GlareDB/rayexec/issues/226
        Ok(())
    }

    fn push_global_sort(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut order: Node<LogicalOrder>,
    ) -> Result<()> {
        let location = order.location;

        let input = order.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs();
        self.walk(materializations, id_gen, input)?;

        let exprs = self
            .expr_planner
            .plan_sorts(&input_refs, &order.node.exprs)?;

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
        mut limit: Node<LogicalLimit>,
    ) -> Result<()> {
        let location = limit.location;
        let input = limit.take_one_child_exact()?;

        self.walk(materializations, id_gen, input)?;

        // TODO: Who sets partitioning? How was that working before?

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::Limit(PhysicalLimit::new(
                limit.node.limit,
                limit.node.offset,
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
        mut agg: Node<LogicalAggregate>,
    ) -> Result<()> {
        let location = agg.location;

        let input = agg.take_one_child_exact()?;
        let input_refs = input.get_output_table_refs();
        self.walk(materializations, id_gen, input)?;

        let mut phys_aggs = Vec::new();

        // Extract arg expressions, place in their own pre-projection.
        let mut preproject_exprs = Vec::new();
        for agg_expr in agg.node.aggregates {
            let agg = match agg_expr {
                Expression::Aggregate(agg) => agg,
                other => {
                    return Err(RayexecError::new(format!(
                        "Expected aggregate, got: {other}"
                    )))
                }
            };

            let start_col_index = preproject_exprs.len();
            for arg in &agg.inputs {
                let scalar = self
                    .expr_planner
                    .plan_scalar(&input_refs, arg)
                    .context("Failed to plan expressions for aggregate pre-projection")?;
                preproject_exprs.push(scalar);
            }
            let end_col_index = preproject_exprs.len();

            let phys_agg = PhysicalAggregateExpression {
                output_type: agg.agg.return_type(),
                function: agg.agg,
                columns: (start_col_index..end_col_index)
                    .map(|idx| PhysicalColumnExpr { idx })
                    .collect(),
            };

            phys_aggs.push(phys_agg);
        }

        // Place group by expressions in pre-projection as well.
        let mut group_types = Vec::with_capacity(agg.node.group_exprs.len());
        for group_expr in agg.node.group_exprs {
            group_types.push(group_expr.datatype(self.bind_context)?);
            let scalar = self
                .expr_planner
                .plan_scalar(&input_refs, &group_expr)
                .context("Failed to plan expressions for group by pre-projection")?;

            preproject_exprs.push(scalar);
        }

        self.push_intermediate_operator(
            IntermediateOperator {
                operator: Arc::new(PhysicalOperator::Project(PhysicalProject {
                    operation: ProjectOperation::new(preproject_exprs),
                })),
                partitioning_requirement: None,
            },
            location,
            id_gen,
        )?;

        match agg.node.grouping_sets {
            Some(grouping_sets) => {
                // If we're working with groups, push a hash aggregate operator.
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::HashAggregate(
                        PhysicalHashAggregate::new(group_types, phys_aggs, grouping_sets),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
            None => {
                // Otherwise push an ungrouped aggregate operator.

                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::UngroupedAggregate(
                        PhysicalUngroupedAggregate::new(phys_aggs),
                    )),
                    partitioning_requirement: None,
                };
                self.push_intermediate_operator(operator, location, id_gen)?;
            }
        };

        Ok(())
    }

    fn push_empty(&mut self, id_gen: &mut PipelineIdGen, empty: Node<LogicalEmpty>) -> Result<()> {
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

    fn push_comparison_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalComparisonJoin>,
    ) -> Result<()> {
        let location = join.location;

        let table_refs = join.get_children_table_refs();

        let equality_idx = join
            .node
            .conditions
            .iter()
            .position(|c| c.op == ComparisonOperator::Eq);

        if let Some(equality_idx) = equality_idx {
            // Use hash join

            let [left, right] = join.take_two_children_exact()?;
            let left_ref = table_refs[0];
            let right_ref = table_refs[1];
            let left_types = self.bind_context.get_table(left_ref)?.column_types.clone();
            let right_types = self.bind_context.get_table(right_ref)?.column_types.clone();

            // Build up all inputs on the right (probe) side. This is going to
            // continue with the the current pipeline.
            self.walk(materializations, id_gen, right)?;

            // Build up the left (build) side in a separate pipeline. This will feed
            // into the currently pipeline at the join operator.
            let mut left_state =
                IntermediatePipelineBuildState::new(self.config, self.bind_context);
            left_state.walk(materializations, id_gen, left)?;

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

            let conditions = join
                .node
                .conditions
                .iter()
                .map(|condition| {
                    self.expr_planner
                        .plan_join_condition_as_hash_join_condition(&table_refs, condition)
                })
                .collect::<Result<Vec<_>>>()?;

            let operator = IntermediateOperator {
                operator: Arc::new(PhysicalOperator::HashJoin(PhysicalHashJoin::new(
                    join.node.join_type,
                    equality_idx,
                    conditions,
                    left_types,
                    right_types,
                ))),
                partitioning_requirement: None,
            };
            self.push_intermediate_operator(operator, location, id_gen)?;

            // Left pipeline will be child this this pipeline at the current
            // operator.
            self.push_as_child_pipeline(left_pipeline, PhysicalHashJoin::BUILD_SIDE_INPUT_INDEX)?;

            Ok(())
        } else {
            // Need to fall back to nested loop join.

            if join.node.join_type != JoinType::Inner {
                not_implemented!("join type with nl join: {}", join.node.join_type);
            }

            let conditions = self
                .expr_planner
                .plan_join_conditions_as_expression(&table_refs, &join.node.conditions)?;

            let condition = PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
                function: Box::new(AndImpl),
                inputs: conditions,
            });

            let [left, right] = join.take_two_children_exact()?;

            self.push_nl_join(
                id_gen,
                materializations,
                location,
                left,
                right,
                Some(condition),
                join.node.join_type,
            )?;

            Ok(())
        }
    }

    fn push_arbitrary_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalArbitraryJoin>,
    ) -> Result<()> {
        let location = join.location;
        let filter = self
            .expr_planner
            .plan_scalar(&join.get_children_table_refs(), &join.node.condition)
            .context("Failed to plan expressions arbitrary join filter")?;

        // Modify the filter as to match the join type.
        let filter = match join.node.join_type {
            JoinType::Inner => filter,
            other => {
                // TODO: Other join types.
                return Err(RayexecError::new(format!(
                    "Unhandled join type for any join: {other:?}"
                )));
            }
        };

        let [left, right] = join.take_two_children_exact()?;

        self.push_nl_join(
            id_gen,
            materializations,
            location,
            left,
            right,
            Some(filter),
            join.node.join_type,
        )
    }

    fn push_cross_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut join: Node<LogicalCrossJoin>,
    ) -> Result<()> {
        let location = join.location;
        let [left, right] = join.take_two_children_exact()?;

        self.push_nl_join(
            id_gen,
            materializations,
            location,
            left,
            right,
            None,
            JoinType::Inner,
        )
    }

    /// Push a nest loop join.
    ///
    /// This will create a complete pipeline for the left side of the join
    /// (build), right right side (probe) will be pushed onto the current
    /// pipeline.
    #[allow(clippy::too_many_arguments)]
    fn push_nl_join(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        location: LocationRequirement,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
        join_type: JoinType,
    ) -> Result<()> {
        if self.config.error_on_nested_loop_join {
            return Err(RayexecError::new("Debug trigger: nested loop join"));
        }

        // Continue to build up all the inputs into the right side.
        self.walk(materializations, id_gen, right)?;

        // Create a completely independent pipeline (or pipelines) for left
        // side.
        let mut left_state = IntermediatePipelineBuildState::new(self.config, self.bind_context);
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
                PhysicalNestedLoopJoin::new(filter, join_type),
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

    fn create_batch_for_row_values(&self, rows: Vec<Vec<Expression>>) -> Result<Batch> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        // TODO: This could probably be simplified.

        let mut row_arrs: Vec<Vec<Arc<Array>>> = Vec::new(); // Row oriented.
        let dummy_batch = Batch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in rows {
            let exprs = self
                .expr_planner
                .plan_scalars(&[], &row_exprs)
                .context("Failed to plan expressions for values")?;
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

        Batch::try_new(cols)
    }
}

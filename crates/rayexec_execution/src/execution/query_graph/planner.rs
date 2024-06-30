use crate::{
    database::{
        create::{CreateSchemaInfo, CreateTableInfo},
        DatabaseContext,
    },
    engine::{vars::SessionVars, EngineRuntime},
    execution::{
        operators::{
            create_schema::PhysicalCreateSchema,
            create_table::PhysicalCreateTable,
            drop::PhysicalDrop,
            empty::{EmptyPartitionState, PhysicalEmpty},
            filter::FilterOperation,
            hash_aggregate::PhysicalHashAggregate,
            insert::PhysicalInsert,
            join::{
                hash_join::PhysicalHashJoin,
                nl_join::{
                    NestedLoopJoinBuildPartitionState, NestedLoopJoinOperatorState,
                    NestedLoopJoinProbePartitionState, PhysicalNestedLoopJoin,
                },
            },
            limit::PhysicalLimit,
            materialize::PhysicalMaterialize,
            project::ProjectOperation,
            query_sink::{PhysicalQuerySink, QuerySinkPartitionState},
            round_robin::{round_robin_states, PhysicalRoundRobinRepartition},
            scan::PhysicalScan,
            simple::{SimpleOperator, SimplePartitionState},
            sort::{local_sort::PhysicalLocalSort, merge_sorted::PhysicalMergeSortedInputs},
            table_function::PhysicalTableFunction,
            ungrouped_aggregate::PhysicalUngroupedAggregate,
            values::PhysicalValues,
            OperatorState, PartitionState,
        },
        pipeline::{Pipeline, PipelineId},
    },
    expr::{PhysicalAggregateExpression, PhysicalScalarExpression, PhysicalSortExpression},
    logical::{
        context::QueryContext,
        operator::{self, LogicalOperator},
    },
};
use rayexec_bullet::{
    array::{Array, Utf8Array},
    batch::Batch,
    compute::concat::concat,
    field::TypeSchema,
};
use rayexec_error::{RayexecError, Result};
use std::{collections::HashMap, sync::Arc};

use super::{
    explain::{format_logical_plan_for_explain, format_pipelines_for_explain},
    sink::QuerySink,
    QueryGraph,
};

/// Configuration used for trigger debug condititions during planning.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryGraphDebugConfig {
    /// Trigger an error if we attempt to plan a nested loop join.
    pub error_on_nested_loop_join: bool,
}

impl QueryGraphDebugConfig {
    pub fn new(vars: &SessionVars) -> Self {
        QueryGraphDebugConfig {
            error_on_nested_loop_join: vars
                .get_var_expect("debug_error_on_nested_loop_join")
                .value
                .try_as_bool()
                .unwrap(),
        }
    }
}

/// Create a query graph from a logical plan.
// TODO: This planner should be split up into two planners.
//
// Planner 1: Build the intermediate pipeline. An intermediate pipeline is the
// same as the current pipeline, just without the states. An intermediate
// pipeline should be fully serializable to enable dist exec. We should also
// plan to have diagnostic info as well (which pipeline feeds into another).
//
// Planner 2: Take the intermediate pipeline and generate the states. This
// should happen on the node that's doing the execution. The final pipeline
// won't be fully serializable due to the states not having a requirement on
// being serializable (since they hold things like clients, hash tables, etc and
// it's not worth making those serializable)
//
// The current `BuildConfig` contains parameters that are only applicable to
// planner 2.
#[derive(Debug)]
pub struct QueryGraphPlanner<'a> {
    conf: BuildConfig<'a>,
}

impl<'a> QueryGraphPlanner<'a> {
    pub fn new(
        db_context: &'a DatabaseContext,
        runtime: &'a Arc<EngineRuntime>,
        target_partitions: usize,
        debug: QueryGraphDebugConfig,
    ) -> Self {
        QueryGraphPlanner {
            conf: BuildConfig {
                db_context,
                runtime,
                target_partitions,
                debug,
            },
        }
    }

    /// Create a query graph from a logical plan.
    ///
    /// The provided query sink will be where all the results of a query get
    /// pushed to (e.g. the client).
    pub fn create_graph(
        &self,
        plan: operator::LogicalOperator,
        context: QueryContext,
        sink: QuerySink,
    ) -> Result<QueryGraph> {
        // For debug checking. If we're an actual plan, assert that we've
        // consumed all pipelines after building the graph.
        //
        // Skipping describe because it doens't actually walk the plan, it just
        // gets the top-level schema.
        let expect_consume_all = !matches!(plan, operator::LogicalOperator::Describe(_));

        let mut id_gen = PipelineIdGen { gen: PipelineId(0) };
        let mut build_state = BuildState::new();

        // Build materialized plans first.
        let mut materializations =
            build_state.plan_materializations(&self.conf, context, &mut id_gen)?;
        // Plan the rest.
        build_state.walk(&self.conf, &mut materializations, &mut id_gen, plan)?;
        // Finish everything off with the final results sink.
        build_state.push_query_sink(&self.conf, &mut id_gen, sink)?;

        assert!(build_state.in_progress.is_none());

        if expect_consume_all {
            assert!(
                !materializations.has_remaining_pipelines(),
                "remaining pipelines in materializations: {materializations:?}"
            );
        }

        let pipelines = build_state.completed;

        Ok(QueryGraph { pipelines })
    }
}

#[derive(Debug, Default)]
struct Materializations {
    /// Source pipelines for `MaterializeScan` operators.
    ///
    /// Key corresponds to the index of the materialized plan in the
    /// QueryContext. Since multiple pipelines can read from the same
    /// materialization, each key has a vec of pipelines that we take from.
    materialize_sources: HashMap<usize, Vec<Pipeline>>,
}

impl Materializations {
    /// Checks if there's any pipelines still in the map.
    ///
    /// This is used as a debugging check. After planning the entire query, all
    /// pending pipelines should have been consumed. If there's still pipelines,
    /// that means we're not accuratately tracking the number of materialized
    /// scans.
    fn has_remaining_pipelines(&self) -> bool {
        for pipelines in self.materialize_sources.values() {
            if !pipelines.is_empty() {
                return true;
            }
        }
        false
    }
}

#[derive(Debug)]
struct BuildConfig<'a> {
    /// Database context scoped to the "session" that's running this query.
    db_context: &'a DatabaseContext,

    /// Reference to the engine runtime. Provied to table functions on scan.
    runtime: &'a Arc<EngineRuntime>,

    /// Target number of partitions to achieve when executing operators.
    target_partitions: usize,

    /// Debug variables for triggering errors on certain conditions.
    debug: QueryGraphDebugConfig,
}

/// Used for ensuring every pipeline in a query has a unique id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PipelineIdGen {
    gen: PipelineId,
}

impl PipelineIdGen {
    fn next(&mut self) -> PipelineId {
        let id = self.gen;
        self.gen.0 += 1;
        id
    }
}

#[derive(Debug)]
struct BuildState {
    /// In-progress pipeline we're building.
    in_progress: Option<Pipeline>,

    /// Completed pipelines.
    completed: Vec<Pipeline>,
}

impl BuildState {
    fn new() -> Self {
        BuildState {
            in_progress: None,
            completed: Vec::new(),
        }
    }

    /// Plans all materialized logical plans in the query context.
    ///
    /// For each materialize plan, this will do two things:
    ///
    /// 1. Build the complete pipeline representing a plan whose sink will be a
    ///    PhysicalMaterialize. This pipeline will be placed in `completed`.
    /// 2. Create a partial pipeline whose source is the same
    ///    `PhysicalMaterialize` from above. This pipeline will be placed in
    ///    `materialize_sources`. When we reach a MaterializeScan during
    ///    planning, we will take the corresponding pipeline from
    ///    `materialize_sources` and build on top of that.
    ///
    /// A materialized plan may depend on earlier materialized plans. What gets
    /// returned is the set of materializations that should be used in the rest
    /// of the plan.
    fn plan_materializations(
        &mut self,
        conf: &BuildConfig,
        context: QueryContext,
        id_gen: &mut PipelineIdGen,
    ) -> Result<Materializations> {
        let mut materializations = Materializations::default();

        for materialized in context.materialized {
            // Generate the pipeline(s) for this plan.
            self.walk(conf, &mut materializations, id_gen, materialized.root)?;

            // Finish off the pipeline with a PhysicalMaterialize as the sink.
            let mut pipeline = self.in_progress.take().ok_or_else(|| {
                RayexecError::new("Expected materialization pipeline to be in-progress")
            })?;

            // Materialization technically also acts as repartition.
            let partitions_per_pipeline: Vec<_> = (0..materialized.num_scans)
                .map(|_| conf.target_partitions)
                .collect();

            let operator = Arc::new(PhysicalMaterialize);
            let (operator_state, push_states, pull_pipeline_states) =
                operator.create_states(pipeline.num_partitions(), partitions_per_pipeline);
            let operator_state = Arc::new(OperatorState::Materialize(operator_state));

            let push_states = push_states
                .into_iter()
                .map(PartitionState::MaterializePush)
                .collect();

            pipeline.push_operator(operator.clone(), operator_state.clone(), push_states)?;
            self.completed.push(pipeline);

            // Generate the partial pipelines for all scans that will be pulling
            // from the materialization.
            let mut partial_pipelines = Vec::with_capacity(materialized.num_scans);
            for states in pull_pipeline_states {
                let mut pipeline = Pipeline::new(id_gen.next(), states.len());
                let states = states
                    .into_iter()
                    .map(PartitionState::MaterializePull)
                    .collect();
                pipeline.push_operator(operator.clone(), operator_state.clone(), states)?;

                partial_pipelines.push(pipeline);
            }

            let existing = materializations
                .materialize_sources
                .insert(materialized.idx, partial_pipelines);
            assert!(existing.is_none());
        }

        Ok(materializations)
    }

    /// Walk a logical plan, creating pipelines along the way.
    ///
    /// Pipeline creation is done in a depth-first approach.
    fn walk(
        &mut self,
        conf: &BuildConfig,
        materializations: &mut Materializations,
        id_gen: &mut PipelineIdGen,
        plan: LogicalOperator,
    ) -> Result<()> {
        match plan {
            LogicalOperator::Projection(proj) => {
                self.push_project(conf, id_gen, materializations, proj)
            }
            LogicalOperator::Filter(filter) => {
                self.push_filter(conf, id_gen, materializations, filter)
            }
            LogicalOperator::ExpressionList(values) => self.push_values(conf, id_gen, values),
            LogicalOperator::CrossJoin(join) => {
                self.push_cross_join(conf, id_gen, materializations, join)
            }
            LogicalOperator::AnyJoin(join) => {
                self.push_any_join(conf, id_gen, materializations, join)
            }
            LogicalOperator::EqualityJoin(join) => {
                self.push_equality_join(conf, id_gen, materializations, join)
            }
            LogicalOperator::DependentJoin(_join) => Err(RayexecError::new(
                "Dependent joins cannot be made into a physical pipeline",
            )),
            LogicalOperator::Empty => self.push_empty(conf, id_gen),
            LogicalOperator::Aggregate(agg) => {
                self.push_aggregate(conf, id_gen, materializations, agg)
            }
            LogicalOperator::Limit(limit) => self.push_limit(conf, id_gen, materializations, limit),
            LogicalOperator::Order(order) => {
                self.push_global_sort(conf, id_gen, materializations, order)
            }
            LogicalOperator::ShowVar(show_var) => self.push_show_var(conf, id_gen, show_var),
            LogicalOperator::Explain(explain) => {
                self.push_explain(conf, id_gen, materializations, explain)
            }
            LogicalOperator::Describe(describe) => self.push_describe(conf, id_gen, describe),
            LogicalOperator::CreateTable(create) => {
                self.push_create_table(conf, id_gen, materializations, create)
            }
            LogicalOperator::CreateSchema(create) => self.push_create_schema(conf, id_gen, create),
            LogicalOperator::Drop(drop) => self.push_drop(conf, id_gen, drop),
            LogicalOperator::Insert(insert) => {
                self.push_insert(conf, id_gen, materializations, insert)
            }
            LogicalOperator::MaterializedScan(scan) => {
                self.push_materialized_scan(conf, materializations, scan)
            }
            LogicalOperator::Scan(scan) => self.push_scan(conf, id_gen, scan),
            LogicalOperator::TableFunction(table_func) => {
                self.push_table_function(conf, id_gen, table_func)
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
            other => unimplemented!("other: {other:?}"),
        }
    }

    /// Get the current in-progress pipeline.
    ///
    /// Errors if there's no pipeline in-progress.
    fn in_progress_pipeline_mut(&mut self) -> Result<&mut Pipeline> {
        match &mut self.in_progress {
            Some(pipeline) => Ok(pipeline),
            None => Err(RayexecError::new("No pipeline in-progress")),
        }
    }

    /// Push a query sink onto the current pipeline. This marks the current
    /// pipeline as completed.
    ///
    /// This is the last step when building up pipelines for a query graph.
    fn push_query_sink(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        sink: QuerySink,
    ) -> Result<()> {
        let current_partitions = self.in_progress_pipeline_mut()?.num_partitions();

        // Push a repartition if the current pipeline has a different number of
        // partitions than the sink we'll be sending results to.
        if sink.num_partitions() != current_partitions {
            self.push_round_robin(conf, id_gen, sink.num_partitions())?;
        }

        let mut current = self
            .in_progress
            .take()
            .ok_or_else(|| RayexecError::new("Missing in-progress pipeline"))?;

        let physical = Arc::new(PhysicalQuerySink);
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = sink
            .partition_sinks
            .into_iter()
            .map(|sink| PartitionState::QuerySink(QuerySinkPartitionState::new(sink)))
            .collect();

        current.push_operator(physical, operator_state, partition_states)?;
        self.completed.push(current);

        Ok(())
    }

    fn push_drop(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        drop: operator::DropEntry,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let physical = Arc::new(PhysicalDrop::new(drop.info));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = vec![PartitionState::Drop(
            physical.try_create_state(conf.db_context)?,
        )];

        let mut pipeline = Pipeline::new(id_gen.next(), partition_states.len());
        pipeline.push_operator(physical, operator_state, partition_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_insert(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        insert: operator::Insert,
    ) -> Result<()> {
        self.walk(conf, materializations, id_gen, *insert.input)?;

        // TODO: Need a "resolved" type on the logical operator that gets us the catalog/schema.
        let physical = Arc::new(PhysicalInsert::new("temp", "temp", insert.table));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states: Vec<_> = physical
            .try_create_states(conf.db_context, conf.target_partitions)?
            .into_iter()
            .map(PartitionState::Insert)
            .collect();

        let pipeline = self.in_progress_pipeline_mut()?;
        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(())
    }

    fn push_table_function(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        table_func: operator::TableFunction,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let physical = Arc::new(PhysicalTableFunction::new(table_func.function));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states: Vec<_> = physical
            .try_create_states(conf.runtime, conf.target_partitions)?
            .into_iter()
            .map(PartitionState::TableFunction)
            .collect();

        let mut pipeline = Pipeline::new(id_gen.next(), partition_states.len());
        pipeline.push_operator(physical, operator_state, partition_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_materialized_scan(
        &mut self,
        _conf: &BuildConfig,
        materializations: &mut Materializations,
        scan: operator::MaterializedScan,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let pipeline = match materializations.materialize_sources.get_mut(&scan.idx) {
            Some(pipelines) => pipelines.pop().ok_or_else(|| {
                RayexecError::new("Invalid number of pipelines created for materialized plan")
            })?,
            None => {
                return Err(RayexecError::new(format!(
                    "Missing pipelines for materialized plan at index {}",
                    scan.idx
                )))
            }
        };

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_scan(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        scan: operator::Scan,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let physical = Arc::new(PhysicalScan::new(scan.catalog, scan.schema, scan.source));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states: Vec<_> = physical
            .try_create_states(conf.db_context, conf.target_partitions)?
            .into_iter()
            .map(PartitionState::Scan)
            .collect();

        let mut pipeline = Pipeline::new(id_gen.next(), partition_states.len());
        pipeline.push_operator(physical, operator_state, partition_states)?;

        self.in_progress = Some(pipeline);

        // TODO: If we don't get the desired number of partitions, we should
        // push a repartition here.

        Ok(())
    }

    fn push_create_schema(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        create: operator::CreateSchema,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let physical = Arc::new(PhysicalCreateSchema::new(
            create.catalog,
            CreateSchemaInfo {
                name: create.name,
                on_conflict: create.on_conflict,
            },
        ));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = vec![PartitionState::CreateSchema(
            physical.try_create_state(conf.db_context)?,
        )];

        let mut pipeline = Pipeline::new(id_gen.next(), partition_states.len());
        pipeline.push_operator(physical, operator_state, partition_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_create_table(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        create: operator::CreateTable,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let is_ctas = create.input.is_some();
        match create.input {
            Some(input) => {
                // CTAS, plan the input. It'll be the source of this pipeline.
                self.walk(conf, materializations, id_gen, *input)?;
            }
            None => {
                // No input, just have an empty operator as the source.
                let mut pipeline = Pipeline::new(id_gen.next(), 1);
                pipeline.push_operator(
                    Arc::new(PhysicalEmpty),
                    Arc::new(OperatorState::None),
                    vec![PartitionState::Empty(EmptyPartitionState::default())],
                )?;

                self.in_progress = Some(pipeline);
            }
        };

        let pipeline = self.in_progress_pipeline_mut()?;

        // To explain my TODO above, this would be what happens in "planner 1".
        // Just creating the operator, and the planning can happen anywhere.
        let physical = Arc::new(PhysicalCreateTable::new(
            create.catalog,
            create.schema,
            CreateTableInfo {
                name: create.name,
                columns: create.columns,
                on_conflict: create.on_conflict,
            },
            is_ctas,
        ));

        // And creating the states would happen in "planner 2". This relies on
        // the database context, and so should happen on the node that will be
        // executing the pipeline.
        let (operator_state, partition_states) =
            physical.try_create_states(conf.db_context, pipeline.num_partitions())?;
        let operator_state = Arc::new(OperatorState::CreateTable(operator_state));
        let partition_states: Vec<_> = partition_states
            .into_iter()
            .map(PartitionState::CreateTable)
            .collect();

        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(())
    }

    fn push_describe(
        &mut self,
        _conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        describe: operator::Describe,
    ) -> Result<()> {
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

        let physical = Arc::new(PhysicalValues::new(vec![batch]));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = physical
            .create_states(1)
            .into_iter()
            .map(PartitionState::Values)
            .collect();

        let mut pipeline = Pipeline::new(id_gen.next(), 1);
        pipeline.push_operator(physical, operator_state, partition_states)?;
        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_explain(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        explain: operator::Explain,
    ) -> Result<()> {
        if explain.analyze {
            unimplemented!()
        }

        let formatted_logical =
            format_logical_plan_for_explain(None, &explain.input, explain.format, explain.verbose)?;

        // Build up the pipeline.
        self.walk(conf, materializations, id_gen, *explain.input)?;

        // And then take it, we'll be discarding this for non-analyze explains.
        let current = self
            .in_progress
            .take()
            .ok_or_else(|| RayexecError::new("Missing in-progress pipeline"))?;

        let formatted_pipelines = format_pipelines_for_explain(
            self.completed.iter().chain(std::iter::once(&current)),
            explain.format,
            explain.verbose,
        )?;

        let physical = Arc::new(PhysicalValues::new(vec![Batch::try_new(vec![
            Array::Utf8(Utf8Array::from_iter(["logical", "pipelines"])),
            Array::Utf8(Utf8Array::from_iter([
                formatted_logical.as_str(),
                formatted_pipelines.as_str(),
            ])),
        ])?]));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = physical
            .create_states(1)
            .into_iter()
            .map(PartitionState::Values)
            .collect();

        let mut pipeline = Pipeline::new(id_gen.next(), 1);
        pipeline.push_operator(physical, operator_state, partition_states)?;
        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_show_var(
        &mut self,
        _conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        show: operator::ShowVar,
    ) -> Result<()> {
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let physical = Arc::new(PhysicalValues::new(vec![Batch::try_new(vec![
            Array::Utf8(Utf8Array::from_iter([show.var.value.to_string().as_str()])),
        ])?]));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = physical
            .create_states(1)
            .into_iter()
            .map(PartitionState::Values)
            .collect();

        let mut pipeline = Pipeline::new(id_gen.next(), 1);
        pipeline.push_operator(physical, operator_state, partition_states)?;
        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_global_sort(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        order: operator::Order,
    ) -> Result<()> {
        let input_schema = order.input.output_schema(&[])?;
        self.walk(conf, materializations, id_gen, *order.input)?;

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
        let mut current = self
            .in_progress
            .take()
            .ok_or_else(|| RayexecError::new("Missing in-progress pipeline"))?;

        let operator = Arc::new(PhysicalLocalSort::new(exprs.clone()));
        let partition_states: Vec<_> = operator
            .create_states(current.num_partitions())
            .into_iter()
            .map(PartitionState::LocalSort)
            .collect();
        let operator_state = Arc::new(OperatorState::None);
        current.push_operator(operator, operator_state, partition_states)?;

        // Global sorting.
        let operator = Arc::new(PhysicalMergeSortedInputs::new(exprs));
        let (operator_state, push_states, pull_states) =
            operator.create_states(current.num_partitions());
        let operator_state = Arc::new(OperatorState::MergeSorted(operator_state));

        // Push side finishes up the current pipeline.
        current.push_operator(
            operator.clone(),
            operator_state.clone(),
            push_states
                .into_iter()
                .map(PartitionState::MergeSortedPush)
                .collect(),
        )?;
        self.completed.push(current);

        // Pull side creates a new pipeline, number of pull states determines
        // number of partitions in this pipeline.
        let mut pipeline = Pipeline::new(id_gen.next(), pull_states.len());
        pipeline.push_operator(
            operator,
            operator_state,
            pull_states
                .into_iter()
                .map(PartitionState::MergeSortedPull)
                .collect(),
        )?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_limit(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        limit: operator::Limit,
    ) -> Result<()> {
        self.walk(conf, materializations, id_gen, *limit.input)?;

        let pipeline = self.in_progress_pipeline_mut()?;

        let operator = Arc::new(PhysicalLimit::new(limit.limit, limit.offset));
        let partition_states: Vec<_> = operator
            .create_states(pipeline.num_partitions())
            .into_iter()
            .map(PartitionState::Limit)
            .collect();

        // No global state in limit.
        let operator_state = Arc::new(OperatorState::None);
        pipeline.push_operator(operator, operator_state, partition_states)?;

        Ok(())
    }

    fn push_aggregate(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        agg: operator::Aggregate,
    ) -> Result<()> {
        let input_schema = agg.input.output_schema(&[])?;
        self.walk(conf, materializations, id_gen, *agg.input)?;

        let pipeline = self.in_progress_pipeline_mut()?;

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

                let (operator, operator_state, partition_states) = PhysicalHashAggregate::try_new(
                    pipeline.num_partitions(),
                    group_types,
                    grouping_sets,
                    agg_exprs,
                )?;

                let operator = Arc::new(operator);
                let operator_state = Arc::new(OperatorState::HashAggregate(operator_state));
                let partition_states = partition_states
                    .into_iter()
                    .map(PartitionState::HashAggregate)
                    .collect();

                pipeline.push_operator(operator, operator_state, partition_states)?;
            }
            None => {
                // Otherwise push an ungrouped aggregate operator.
                let operator = PhysicalUngroupedAggregate::new(agg_exprs);
                let (operator_state, partition_states) =
                    operator.create_states(pipeline.num_partitions());
                let operator_state = Arc::new(OperatorState::UngroupedAggregate(operator_state));
                let partition_states: Vec<_> = partition_states
                    .into_iter()
                    .map(PartitionState::UngroupedAggregate)
                    .collect();

                pipeline.push_operator(Arc::new(operator), operator_state, partition_states)?;
            }
        };

        Ok(())
    }

    /// Pushes a round robin repartition onto the pipeline.
    ///
    /// This will mark the current pipeline completed, and start a new pipeline
    /// using the repartition as its source.
    fn push_round_robin(
        &mut self,
        _conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        target_partitions: usize,
    ) -> Result<()> {
        let mut current = self
            .in_progress
            .take()
            .ok_or_else(|| RayexecError::new("Missing in-progress pipeline"))?;

        let (operator_state, push_states, pull_states) =
            round_robin_states(current.num_partitions(), target_partitions);

        let operator_state = Arc::new(OperatorState::RoundRobin(operator_state));
        let push_states = push_states
            .into_iter()
            .map(PartitionState::RoundRobinPush)
            .collect();
        let pull_states = pull_states
            .into_iter()
            .map(PartitionState::RoundRobinPull)
            .collect();

        let physical = Arc::new(PhysicalRoundRobinRepartition);

        // Current pipeline is now completed.
        current.push_operator(physical.clone(), operator_state.clone(), push_states)?;
        self.completed.push(current);

        // We have a new pipeline with its inputs being the output of the
        // repartition.
        let mut pipeline = Pipeline::new(id_gen.next(), target_partitions);
        pipeline.push_operator(physical, operator_state, pull_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_project(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        project: operator::Projection,
    ) -> Result<()> {
        let input_schema = project.input.output_schema(&[])?;
        self.walk(conf, materializations, id_gen, *project.input)?;

        let pipeline = self.in_progress_pipeline_mut()?;

        let projections = project
            .exprs
            .into_iter()
            .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, &input_schema))
            .collect::<Result<Vec<_>>>()?;
        let physical = Arc::new(SimpleOperator::new(ProjectOperation::new(projections)));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = (0..pipeline.num_partitions())
            .map(|_| PartitionState::Simple(SimplePartitionState::new()))
            .collect();

        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(())
    }

    fn push_filter(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        filter: operator::Filter,
    ) -> Result<()> {
        let input_schema = filter.input.output_schema(&[])?;
        self.walk(conf, materializations, id_gen, *filter.input)?;

        let pipeline = self.in_progress_pipeline_mut()?;

        let predicate =
            PhysicalScalarExpression::try_from_uncorrelated_expr(filter.predicate, &input_schema)?;
        let physical = Arc::new(SimpleOperator::new(FilterOperation::new(predicate)));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = (0..pipeline.num_partitions())
            .map(|_| PartitionState::Simple(SimplePartitionState::new()))
            .collect();

        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(())
    }

    /// Push an equality (hash) join.
    fn push_equality_join(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: operator::EqualityJoin,
    ) -> Result<()> {
        // Build up all inputs on the right (probe) side. This is going to
        // continue with the the current pipeline.
        self.walk(conf, materializations, id_gen, *join.right)?;

        // Build up the left (build) side in a separate pipeline. This will feed
        // into the currently pipeline at the join operator.
        let mut left_state = BuildState::new();
        left_state.walk(conf, materializations, id_gen, *join.left)?;

        // Take any completed pipelines from the left side and put them in our
        // list.
        //
        // Ordering doesn't matter.
        self.completed.append(&mut left_state.completed);

        // Get the left pipeline.
        let mut left_pipeline = left_state.in_progress.take().ok_or_else(|| {
            RayexecError::new("expected in-progress pipeline from left side of join")
        })?;

        let num_build_partitions = left_pipeline.num_partitions();
        let num_probe_partitions = self.in_progress_pipeline_mut()?.num_partitions();

        let operator = Arc::new(PhysicalHashJoin::new(
            join.join_type,
            join.left_on,
            join.right_on,
        ));
        let (operator_state, build_states, probe_states) =
            operator.create_states(num_build_partitions, num_probe_partitions);

        let operator_state = Arc::new(OperatorState::HashJoin(operator_state));
        let build_states: Vec<_> = build_states
            .into_iter()
            .map(PartitionState::HashJoinBuild)
            .collect();
        let probe_states: Vec<_> = probe_states
            .into_iter()
            .map(PartitionState::HashJoinProbe)
            .collect();

        // Push build states to left pipeline.
        left_pipeline.push_operator(operator.clone(), operator_state.clone(), build_states)?;

        // Left pipeline is now completed.
        self.completed.push(left_pipeline);

        // Push probe states to current pipeline along the shared operator
        // state.
        //
        // This pipeline is not completed, we'll continue to push more operators
        // on it.
        self.in_progress_pipeline_mut()?
            .push_operator(operator, operator_state, probe_states)?;

        Ok(())
    }

    fn push_any_join(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: operator::AnyJoin,
    ) -> Result<()> {
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
            conf,
            id_gen,
            materializations,
            *join.left,
            *join.right,
            Some(filter),
        )
    }

    fn push_cross_join(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        join: operator::CrossJoin,
    ) -> Result<()> {
        self.push_nl_join(
            conf,
            id_gen,
            materializations,
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
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
    ) -> Result<()> {
        if conf.debug.error_on_nested_loop_join {
            return Err(RayexecError::new("Debug trigger: nested loop join"));
        }

        // Continue to build up all the inputs into the right side.
        self.walk(conf, materializations, id_gen, right)?;

        // Create a completely independent pipeline (or pipelines) for left
        // side.
        let mut left_state = BuildState::new();
        left_state.walk(conf, materializations, id_gen, left)?;

        // Take completed pipelines from the left and merge them into this
        // state's completed set of pipelines.
        self.completed.append(&mut left_state.completed);

        // Get the left in-progress pipeline. This will be one of the inputs
        // into the current in-progress pipeline.
        let mut left_pipeline = left_state.in_progress.take().ok_or_else(|| {
            RayexecError::new("expected in-progress pipeline from left side of join")
        })?;

        let left_partitions = left_pipeline.num_partitions();
        let right_partitions = self.in_progress_pipeline_mut()?.num_partitions();

        let physical = Arc::new(PhysicalNestedLoopJoin::new(filter));

        // State shared between left and right.
        let operator_state = Arc::new(OperatorState::NestedLoopJoin(
            NestedLoopJoinOperatorState::new(left_partitions, right_partitions),
        ));

        let left_states = (0..left_partitions)
            .map(|_| {
                PartitionState::NestedLoopJoinBuild(NestedLoopJoinBuildPartitionState::default())
            })
            .collect();

        // Push left states to left pipeline. This pipeline is now "completed".
        // It's acting as an input into the current pipeline.
        left_pipeline.push_operator(physical.clone(), operator_state.clone(), left_states)?;
        self.completed.push(left_pipeline);

        let current_pipeline = self.in_progress_pipeline_mut()?;
        let right_states = (0..right_partitions)
            .map(|partition| {
                PartitionState::NestedLoopJoinProbe(
                    NestedLoopJoinProbePartitionState::new_for_partition(partition),
                )
            })
            .collect();

        // Push right states to the currently in-progress pipeline. Note this
        // pipeline isn't considered "completed" at this point. We'll continue
        // to push onto this pipeline.
        current_pipeline.push_operator(physical, operator_state, right_states)?;

        Ok(())
    }

    fn push_values(
        &mut self,
        conf: &BuildConfig,
        id_gen: &mut PipelineIdGen,
        values: operator::ExpressionList,
    ) -> Result<()> {
        // "Values" is a source of data, and so should be the thing determining
        // the initial partitioning of the pipeline.
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        self.in_progress = Some(Self::new_pipeline_for_values(id_gen.next(), conf, values)?);

        Ok(())
    }

    fn push_empty(&mut self, _conf: &BuildConfig, id_gen: &mut PipelineIdGen) -> Result<()> {
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

        let physical = Arc::new(PhysicalEmpty);
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = vec![PartitionState::Empty(EmptyPartitionState::default())];

        let mut pipeline = Pipeline::new(id_gen.next(), 1);
        pipeline.push_operator(physical, operator_state, partition_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn new_pipeline_for_values(
        pipeline_id: PipelineId,
        conf: &BuildConfig,
        values: operator::ExpressionList,
    ) -> Result<Pipeline> {
        // TODO: This could probably be simplified.

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

        // TODO: This currently only puts the resulting batch in one partition,
        // essentially making the other partitions no-ops.
        //
        // I'm doing this mostly for easily initial implementation, but we have
        // two choices to make this better (if we care).
        //
        // 1. Pre-partition the batch here, and just equally distribute the
        //    batches across the partition pipelines.
        // 2. Add in a partitioning operator above this operator such that
        //    partition happens during execution.

        let mut pipeline = Pipeline::new(pipeline_id, conf.target_partitions);

        let physical = Arc::new(PhysicalValues::new(vec![batch]));
        let operator_state = Arc::new(OperatorState::None);
        let partition_states = physical
            .create_states(conf.target_partitions)
            .into_iter()
            .map(PartitionState::Values)
            .collect();

        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(pipeline)
    }
}

use crate::{
    execution::{
        operators::{
            aggregate::{
                grouping_set::GroupingSets,
                hash_aggregate::{HashAggregateColumnOutput, PhysicalHashAggregate},
            },
            empty::{EmptyPartitionState, PhysicalEmpty},
            filter::FilterOperation,
            join::{
                hash_join::PhysicalHashJoin,
                nl_join::{
                    NestedLoopJoinBuildPartitionState, NestedLoopJoinOperatorState,
                    NestedLoopJoinProbePartitionState, PhysicalNestedLoopJoin,
                },
            },
            limit::PhysicalLimit,
            project::ProjectOperation,
            query_sink::{PhysicalQuerySink, QuerySinkPartitionState},
            repartition::round_robin::{round_robin_states, PhysicalRoundRobinRepartition},
            simple::{SimpleOperator, SimplePartitionState},
            sort::{local_sort::PhysicalLocalSort, merge_sorted::PhysicalMergeSortedInputs},
            values::{PhysicalValues, ValuesPartitionState},
            OperatorState, PartitionState,
        },
        pipeline::{Pipeline, PipelineId},
    },
    expr::{PhysicalAggregateExpression, PhysicalScalarExpression, PhysicalSortExpression},
    planner::operator::{self, LogicalOperator},
};
use rayexec_bullet::{
    array::Array, batch::Batch, bitmap::Bitmap, compute::concat::concat, field::TypeSchema,
};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

use super::{sink::QuerySink, QueryGraph};

/// Configuration used for trigger debug condititions during planning.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryGraphDebugConfig {
    /// Trigger an error if we attempt to plan a nested loop join.
    pub error_on_nested_loop_join: bool,
}

/// Create a query graph from a logical plan.
#[derive(Debug)]
pub struct QueryGraphPlanner {
    conf: BuildConfig,
}

impl QueryGraphPlanner {
    pub fn new(target_partitions: usize, debug: QueryGraphDebugConfig) -> Self {
        QueryGraphPlanner {
            conf: BuildConfig {
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
        sink: QuerySink,
    ) -> Result<QueryGraph> {
        let mut build_state = BuildState::new();
        build_state.walk(&self.conf, plan)?;
        build_state.push_query_sink(&self.conf, sink)?;
        assert!(build_state.in_progress.is_none());

        let pipelines = build_state.completed;

        Ok(QueryGraph { pipelines })
    }
}

#[derive(Debug)]
struct BuildConfig {
    target_partitions: usize,
    debug: QueryGraphDebugConfig,
}

#[derive(Debug)]
struct BuildState {
    /// In-progress pipeline we're building.
    in_progress: Option<Pipeline>,

    /// Completed pipelines.
    completed: Vec<Pipeline>,

    /// Next id to use for new pipelines in the graph.
    next_pipeline_id: PipelineId,
}

impl BuildState {
    fn new() -> Self {
        BuildState {
            in_progress: None,
            completed: Vec::new(),
            next_pipeline_id: PipelineId(0),
        }
    }

    /// Walk a logical plan, creating pipelines along the way.
    ///
    /// Pipeline creation is done in a depth-first approach.
    fn walk(&mut self, conf: &BuildConfig, plan: LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::Projection(proj) => self.push_project(conf, proj),
            LogicalOperator::Filter(filter) => self.push_filter(conf, filter),
            LogicalOperator::ExpressionList(values) => self.push_values(conf, values),
            LogicalOperator::CrossJoin(join) => self.push_cross_join(conf, join),
            LogicalOperator::AnyJoin(join) => self.push_any_join(conf, join),
            LogicalOperator::EqualityJoin(join) => self.push_equality_join(conf, join),
            LogicalOperator::Empty => self.push_empty(conf),
            LogicalOperator::Aggregate(agg) => self.push_aggregate(conf, agg),
            LogicalOperator::Limit(limit) => self.push_limit(conf, limit),
            LogicalOperator::Order(order) => self.push_global_sort(conf, order),
            // LogicalOperator::Scan(scan) => self.plan_scan(scan),
            // LogicalOperator::SetVar(set_var) => self.plan_set_var(set_var),
            // LogicalOperator::ShowVar(show_var) => self.plan_show_var(show_var),
            other => unimplemented!("other: {other:?}"),
        }
    }

    fn next_pipeline_id(&mut self) -> PipelineId {
        let id = self.next_pipeline_id;
        self.next_pipeline_id = PipelineId(id.0 + 1);
        id
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
    fn push_query_sink(&mut self, conf: &BuildConfig, sink: QuerySink) -> Result<()> {
        let current_partitions = self.in_progress_pipeline_mut()?.num_partitions();

        // Push a repartition if the current pipeline has a different number of
        // partitions than the sink we'll be sending results to.
        if sink.num_partitions() != current_partitions {
            self.push_round_robin(conf, sink.num_partitions())?;
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

    fn push_global_sort(&mut self, conf: &BuildConfig, order: operator::Order) -> Result<()> {
        let input_schema = order.input.output_schema(&[])?;
        self.walk(conf, *order.input)?;

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
        let mut pipeline = Pipeline::new(self.next_pipeline_id(), pull_states.len());
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

    fn push_limit(&mut self, conf: &BuildConfig, limit: operator::Limit) -> Result<()> {
        self.walk(conf, *limit.input)?;

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

    fn push_aggregate(&mut self, conf: &BuildConfig, agg: operator::Aggregate) -> Result<()> {
        let input_schema = agg.input.output_schema(&[])?;
        self.walk(conf, *agg.input)?;

        let pipeline = self.in_progress_pipeline_mut()?;

        let mut agg_exprs = Vec::new();
        let mut projection = Vec::new();
        for expr in agg.exprs.into_iter() {
            match expr {
                operator::LogicalExpression::ColumnRef(col) => {
                    let col = col.try_as_uncorrelated()?;
                    projection.push(HashAggregateColumnOutput::GroupingColumn(col));
                }
                other => {
                    let agg_expr = PhysicalAggregateExpression::try_from_logical_expression(
                        other,
                        &input_schema,
                    )?;
                    agg_exprs.push(agg_expr);
                    projection.push(HashAggregateColumnOutput::AggregateResult(
                        agg_exprs.len() - 1,
                    ));
                }
            }
        }

        // Compute the grouping sets based on the grouping expression. It's
        // expected that this plan only has uncorrelated column references as
        // expressions.
        let grouping_sets = match agg.grouping_expr {
            operator::GroupingExpr::None => {
                // TODO: We'd actually use a different (ungrouped) operator if
                // not provided any grouping sets.
                //
                // This just works because all hashes are initialized to zero,
                // and so everything does map to the same thing. Def not
                // something we should rely on.
                GroupingSets::try_new(Vec::new(), vec![Bitmap::default()])?
            }
            operator::GroupingExpr::GroupBy(cols_exprs) => {
                let cols = cols_exprs
                    .into_iter()
                    .map(|expr| expr.try_into_column_ref()?.try_as_uncorrelated())
                    .collect::<Result<Vec<_>>>()?;
                let null_masks = vec![Bitmap::new_with_val(false, cols.len())];
                GroupingSets::try_new(cols, null_masks)?
            }
            operator::GroupingExpr::Rollup(cols_exprs) => {
                let cols = cols_exprs
                    .into_iter()
                    .map(|expr| expr.try_into_column_ref()?.try_as_uncorrelated())
                    .collect::<Result<Vec<_>>>()?;

                // Generate all null masks.
                //
                // E.g. for rollup on 4 columns:
                // [
                //   0000,
                //   0001,
                //   0011,
                //   0111,
                //   1111,
                // ]
                let mut null_masks = Vec::with_capacity(cols.len() + 1);
                for num_null_cols in 0..cols.len() {
                    let iter = std::iter::repeat(false)
                        .take(cols.len() - num_null_cols)
                        .chain(std::iter::repeat(true).take(num_null_cols));
                    let null_mask = Bitmap::from_iter(iter);
                    null_masks.push(null_mask);
                }

                // Append null mask with all columns marked as null (the final
                // rollup).
                null_masks.push(Bitmap::all_true(cols.len()));

                GroupingSets::try_new(cols, null_masks)?
            }
            operator::GroupingExpr::Cube(_) => {
                unimplemented!("https://github.com/GlareDB/rayexec/issues/38")
            }
        };

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
            projection,
        )?;

        let operator = Arc::new(operator);
        let operator_state = Arc::new(OperatorState::HashAggregate(operator_state));
        let partition_states = partition_states
            .into_iter()
            .map(PartitionState::HashAggregate)
            .collect();

        pipeline.push_operator(operator, operator_state, partition_states)?;

        Ok(())
    }

    /// Pushes a round robin repartition onto the pipeline.
    ///
    /// This will mark the current pipeline completed, and start a new pipeline
    /// using the repartition as its source.
    fn push_round_robin(&mut self, _conf: &BuildConfig, target_partitions: usize) -> Result<()> {
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
        let mut pipeline = Pipeline::new(self.next_pipeline_id(), target_partitions);
        pipeline.push_operator(physical, operator_state, pull_states)?;

        self.in_progress = Some(pipeline);

        Ok(())
    }

    fn push_project(&mut self, conf: &BuildConfig, project: operator::Projection) -> Result<()> {
        let input_schema = project.input.output_schema(&[])?;
        self.walk(conf, *project.input)?;

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

    fn push_filter(&mut self, conf: &BuildConfig, filter: operator::Filter) -> Result<()> {
        let input_schema = filter.input.output_schema(&[])?;
        self.walk(conf, *filter.input)?;

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
        join: operator::EqualityJoin,
    ) -> Result<()> {
        // Build up all inputs on the right (probe) side. This is going to
        // continue with the the current pipeline.
        self.walk(conf, *join.right)?;

        // Build up the left (build) side in a separate pipeline. This will feed
        // into the currently pipeline at the join operator.
        let mut left_state = BuildState::new();
        left_state.walk(conf, *join.left)?;

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

    fn push_any_join(&mut self, conf: &BuildConfig, join: operator::AnyJoin) -> Result<()> {
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

        self.push_nl_join(conf, *join.left, *join.right, Some(filter))
    }

    fn push_cross_join(&mut self, conf: &BuildConfig, join: operator::CrossJoin) -> Result<()> {
        self.push_nl_join(conf, *join.left, *join.right, None)
    }

    /// Push a nest loop join.
    ///
    /// This will create a complete pipeline for the left side of the join
    /// (build), right right side (probe) will be pushed onto the current
    /// pipeline.
    fn push_nl_join(
        &mut self,
        conf: &BuildConfig,
        left: operator::LogicalOperator,
        right: operator::LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
    ) -> Result<()> {
        if conf.debug.error_on_nested_loop_join {
            return Err(RayexecError::new("Debug trigger: nested loop join"));
        }

        // Continue to build up all the inputs into the right side.
        self.walk(conf, right)?;

        // Create a completely independent pipeline (or pipelines) for left
        // side.
        let mut left_state = BuildState::new();
        left_state.walk(conf, left)?;

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

    fn push_values(&mut self, conf: &BuildConfig, values: operator::ExpressionList) -> Result<()> {
        // "Values" is a source of data, and so should be the thing determining
        // the initial partitioning of the pipeline.
        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        self.in_progress = Some(Self::new_pipeline_for_values(
            self.next_pipeline_id(),
            conf,
            values,
        )?);

        Ok(())
    }

    fn push_empty(&mut self, _conf: &BuildConfig) -> Result<()> {
        // "Empty" is a source of data by virtue of emitting a batch
        // consistenting of no columns and 1 row.
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

        let mut pipeline = Pipeline::new(self.next_pipeline_id(), 1);
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

        let physical = Arc::new(PhysicalValues);
        let operator_state = Arc::new(OperatorState::None);
        let mut partition_states = vec![PartitionState::Values(
            ValuesPartitionState::with_batches(vec![batch]),
        )];
        // Extend out partition states with empty states.
        partition_states.resize_with(conf.target_partitions, || {
            PartitionState::Values(ValuesPartitionState::empty())
        });

        pipeline.push_operator(physical, operator_state, partition_states)?;

        Ok(pipeline)
    }
}

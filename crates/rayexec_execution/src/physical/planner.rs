use super::{
    chain::OperatorChain,
    plans::{
        empty_source::EmptySource, projection::PhysicalProjection, set_var::PhysicalSetVar,
        show_var::PhysicalShowVar, PhysicalOperator,
    },
    Pipeline, Sink, Source,
};
use crate::{
    engine::vars::SessionVars,
    expr::PhysicalScalarExpression,
    functions::table::Pushdown,
    physical::plans::{
        filter::PhysicalFilter, hash_join::PhysicalPartitionedHashJoin,
        hash_repartition::PhysicalHashRepartition, limit::PhysicalLimit,
        nested_loop_join::PhysicalNestedLoopJoin, values::PhysicalValues,
    },
    planner::operator::{self, LogicalOperator},
    types::batch::DataBatch,
};
use arrow_array::{Array, ArrayRef};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// Produce phyisical plans from logical plans.
#[derive(Debug)]
pub struct PhysicalPlanner {
    debug: DebugConfig,
}

/// Configuration used for trigger debug condititions during planning.
#[derive(Debug, Clone, Copy)]
struct DebugConfig {
    /// Trigger an error if we attempt to plan a nested loop join.
    error_on_nested_loop_join: bool,
}

impl PhysicalPlanner {
    /// Create a new physical planner using the configured session vars.
    pub fn try_new_from_vars(vars: &SessionVars) -> Result<Self> {
        Ok(PhysicalPlanner {
            debug: DebugConfig {
                error_on_nested_loop_join: vars
                    .get_var("debug_error_on_nested_loop_join")?
                    .value
                    .try_as_bool()?,
            },
        })
    }

    /// Create a physical plan from a logical plan.
    pub fn create_plan(&self, plan: LogicalOperator, dest: Box<dyn Sink>) -> Result<Pipeline> {
        let builder = PipelineBuilder::new(dest, self.debug);
        let pipeline = builder.build_pipeline(plan)?;

        Ok(pipeline)
    }
}

/// Build up operator chains for a pipeline depth-first.
#[derive(Debug)]
struct PipelineBuilder {
    /// Intermediate sink we're working with.
    sink: Option<Box<dyn Sink>>,

    /// Intermediate operators.
    operators: Vec<Box<dyn PhysicalOperator>>,

    /// Intermediate source we're working with.
    source: Option<Box<dyn Source>>,

    /// Built operator chains.
    completed_chains: Vec<OperatorChain>,

    debug: DebugConfig,
}

impl PipelineBuilder {
    /// Create a new builder for a pipeline that outputs the final result to
    /// `dest`.
    fn new(dest: Box<dyn Sink>, debug: DebugConfig) -> Self {
        PipelineBuilder {
            sink: Some(dest),
            operators: Vec::new(),
            source: None,
            completed_chains: Vec::new(),
            debug,
        }
    }

    /// Builds a plan from a logical operator.
    fn build_pipeline(mut self, plan: LogicalOperator) -> Result<Pipeline> {
        self.walk_plan(plan)?;
        self.create_complete_chain()?;

        let chains = self.completed_chains.into_iter().map(Arc::new).collect();

        Ok(Pipeline { chains })
    }

    /// Creates a completed chain from the current build state.
    ///
    /// Errors if source or sink are None.
    fn create_complete_chain(&mut self) -> Result<()> {
        let source = match self.source.take() {
            Some(source) => source,
            None => return Err(RayexecError::new("Expected source to be Some")),
        };
        let sink = match self.sink.take() {
            Some(source) => source,
            None => return Err(RayexecError::new("Expected sink to be Some")),
        };

        // TODO: Handle case where source and sink have different number of
        // partitions.
        let chain = OperatorChain::try_new(source, sink, std::mem::take(&mut self.operators))?;

        self.completed_chains.push(chain);

        Ok(())
    }

    /// Recursively walks the provided plan, creating physical operators along
    /// the the way and adding them to the pipeline.
    fn walk_plan(&mut self, plan: LogicalOperator) -> Result<()> {
        match plan {
            LogicalOperator::Projection(proj) => self.plan_projection(proj),
            LogicalOperator::Filter(filter) => self.plan_filter(filter),
            LogicalOperator::Scan(scan) => self.plan_scan(scan),
            LogicalOperator::ExpressionList(values) => self.plan_values(values),
            LogicalOperator::CrossJoin(join) => self.plan_cross_join(join),
            LogicalOperator::AnyJoin(join) => self.plan_any_join(join),
            LogicalOperator::EqualityJoin(join) => self.plan_equality_join(join),
            LogicalOperator::Limit(limit) => self.plan_limit(limit),
            LogicalOperator::Empty => self.plan_empty(),
            LogicalOperator::SetVar(set_var) => self.plan_set_var(set_var),
            LogicalOperator::ShowVar(show_var) => self.plan_show_var(show_var),
            other => unimplemented!("other: {other:?}"),
        }
    }

    fn plan_show_var(&mut self, show_var: operator::ShowVar) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }
        self.source = Some(Box::new(PhysicalShowVar::new(show_var.var)));
        Ok(())
    }

    fn plan_set_var(&mut self, set_var: operator::SetVar) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }
        self.source = Some(Box::new(PhysicalSetVar::new(set_var.name, set_var.value)));
        Ok(())
    }

    fn plan_limit(&mut self, limit: operator::Limit) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        let mut physical_limit = PhysicalLimit::new(limit.offset, limit.limit, 1);
        let limit_sink = physical_limit.take_sink().expect("limit sink to exist");

        // Build children.
        let mut builder = PipelineBuilder::new(Box::new(limit_sink), self.debug);
        builder.walk_plan(*limit.input)?;
        builder.create_complete_chain()?;
        let mut chains = builder.completed_chains;

        self.completed_chains.append(&mut chains);
        self.source = Some(Box::new(physical_limit));

        Ok(())
    }

    /// Plan a join that can handle arbitrary expressions.
    fn plan_any_join(&mut self, join: operator::AnyJoin) -> Result<()> {
        let filter = PhysicalScalarExpression::try_from_uncorrelated_expr(join.on)?;

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

        self.plan_nested_loop_join(*join.left, *join.right, Some(filter))
    }

    fn plan_equality_join(&mut self, join: operator::EqualityJoin) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        // TODO: Partitions.
        let mut hash_join = PhysicalPartitionedHashJoin::try_new(
            join.left_on.clone(),
            join.right_on.clone(),
            1,
            join.join_type,
        )?;
        let build_sink = hash_join.take_build_sink().expect("build sink to exist");
        let probe_sink = hash_join.take_probe_sink().expect("probe sink to exist");

        // Build left side of join with the build sink as the destination.
        let mut left_completed = {
            // Left side goes into hash repartition.
            let mut repartition = PhysicalHashRepartition::new(1, 1, join.left_on);
            let repartition_sink = repartition.take_sink().expect("repartition sink to exist");

            let mut builder = PipelineBuilder::new(Box::new(repartition_sink), self.debug);
            builder.walk_plan(*join.left)?;
            builder.create_complete_chain()?;
            let mut chains = builder.completed_chains;

            // Add an additional operator chain for repartition -> build sink.
            chains.push(OperatorChain::try_new(
                Box::new(repartition),
                Box::new(build_sink),
                Vec::new(),
            )?);

            chains
        };

        // Build right side of join with the probe sink as the destination.
        let mut right_completed = {
            // Right side goes into hash repartition.
            //
            // TODO: Measure performance of this. We might not want to
            // repartition based on hash (but will still need to hash).
            let mut repartition = PhysicalHashRepartition::new(1, 1, join.right_on);
            let repartition_sink = repartition.take_sink().expect("repartition sink to exist");

            let mut builder = PipelineBuilder::new(Box::new(repartition_sink), self.debug);
            builder.walk_plan(*join.right)?;
            builder.create_complete_chain()?;
            let mut chains = builder.completed_chains;

            // Add an additional operator chain for repartition -> probe sink.
            chains.push(OperatorChain::try_new(
                Box::new(repartition),
                Box::new(probe_sink),
                Vec::new(),
            )?);

            chains
        };

        // Source if this pipeline is now the hash join results.
        self.source = Some(Box::new(hash_join));

        // Append operator chains from the left and right children. Note that
        // order doesn't matter with the chains.
        self.completed_chains.append(&mut left_completed);
        self.completed_chains.append(&mut right_completed);

        Ok(())
    }

    fn plan_cross_join(&mut self, join: operator::CrossJoin) -> Result<()> {
        self.plan_nested_loop_join(*join.left, *join.right, None)
    }

    /// Plan a nested loop join between left and right with the given filter.
    ///
    /// Providing a None for the filter is equivalent to make this a cross join.
    /// Every row will match every other row.
    fn plan_nested_loop_join(
        &mut self,
        left: LogicalOperator,
        right: LogicalOperator,
        filter: Option<PhysicalScalarExpression>,
    ) -> Result<()> {
        if self.debug.error_on_nested_loop_join {
            return Err(RayexecError::new(
                "Triggered debug error on nested loop join",
            ));
        }
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        // TODO: Partitions.
        let mut cross_join = PhysicalNestedLoopJoin::new(1, filter);
        let build_sink = cross_join.take_build_sink().expect("build sink to exist");
        let probe_sink = cross_join.take_probe_sink().expect("probe sink to exist");

        // Build left side of join with the build sink as the destination.
        let mut left_completed = {
            let mut builder = PipelineBuilder::new(Box::new(build_sink), self.debug);
            builder.walk_plan(left)?;
            builder.create_complete_chain()?;
            builder.completed_chains
        };

        // Build right side of join with the probe sink as the destination.
        let mut right_completed = {
            let mut builder = PipelineBuilder::new(Box::new(probe_sink), self.debug);
            builder.walk_plan(right)?;
            builder.create_complete_chain()?;
            builder.completed_chains
        };

        // Source if this pipeline is now the cross join results.
        self.source = Some(Box::new(cross_join));

        // Append operator chains from the left and right children. Note that
        // order doesn't matter with the chains.
        self.completed_chains.append(&mut left_completed);
        self.completed_chains.append(&mut right_completed);

        Ok(())
    }

    fn plan_empty(&mut self) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        let source = EmptySource::new();
        self.source = Some(Box::new(source));

        Ok(())
    }

    fn plan_projection(&mut self, proj: operator::Projection) -> Result<()> {
        // Plan projection.
        let projections = proj
            .exprs
            .into_iter()
            .map(PhysicalScalarExpression::try_from_uncorrelated_expr)
            .collect::<Result<Vec<_>>>()?;
        let operator = PhysicalProjection::try_new(projections)?;

        // Plan child, who's output will be pushed into the projection.
        self.walk_plan(*proj.input)?;

        self.operators.push(Box::new(operator));

        Ok(())
    }

    fn plan_filter(&mut self, filter: operator::Filter) -> Result<()> {
        // Plan filter.
        let predicate = PhysicalScalarExpression::try_from_uncorrelated_expr(filter.predicate)?;
        let operator = PhysicalFilter::try_new(predicate)?;

        // Plan child, who's output will be pushed into the filter.
        self.walk_plan(*filter.input)?;

        self.operators.push(Box::new(operator));

        Ok(())
    }

    fn plan_scan(&mut self, scan: operator::Scan) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        let source = match scan.source {
            operator::ScanItem::TableFunction(f) => {
                f.into_source(Vec::new(), Pushdown::default())? // TODO: Actual projection
            }
        };
        self.source = Some(source);

        Ok(())
    }

    fn plan_values(&mut self, values: operator::ExpressionList) -> Result<()> {
        if self.source.is_some() {
            return Err(RayexecError::new("Expected source to be None"));
        }

        let mut row_arrs: Vec<Vec<ArrayRef>> = Vec::new(); // Row oriented.

        let dummy_batch = DataBatch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in values.rows {
            let exprs = row_exprs
                .into_iter()
                .map(PhysicalScalarExpression::try_from_uncorrelated_expr)
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
            let refs: Vec<&dyn Array> = arrs.iter().map(|a| a.as_ref()).collect();
            let col = arrow::compute::concat(&refs)?;
            cols.push(col);
        }

        let batch = DataBatch::try_new(cols)?;
        let source = PhysicalValues::new(batch);

        self.source = Some(Box::new(source));

        Ok(())
    }
}

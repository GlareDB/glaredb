use super::{
    chain::OperatorChain,
    plans::{empty_source::EmptySource, projection::PhysicalProjection, PhysicalOperator},
    Pipeline, Sink, Source,
};
use crate::{
    expr::PhysicalScalarExpression,
    functions::table::Pushdown,
    physical::plans::{filter::PhysicalFilter, values::PhysicalValues},
    planner::operator::{self, LogicalOperator},
    types::batch::{DataBatch, DataBatchSchema},
};
use arrow_array::{Array, ArrayRef};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// Produce phyisical plans from logical plans.
#[derive(Debug)]
pub struct PhysicalPlanner {}

impl PhysicalPlanner {
    pub fn new() -> Self {
        PhysicalPlanner {}
    }

    /// Create a physical plan from a logical plan.
    pub fn create_plan(&self, plan: LogicalOperator, dest: Box<dyn Sink>) -> Result<Pipeline> {
        let builder = PipelineBuilder::new(dest);
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
}

impl PipelineBuilder {
    /// Create a new builder for a pipeline that outputs the final result to
    /// `dest`.
    fn new(dest: Box<dyn Sink>) -> Self {
        PipelineBuilder {
            sink: Some(dest),
            operators: Vec::new(),
            source: None,
            completed_chains: Vec::new(),
        }
    }

    /// Builds a plan from a logical operator.
    fn build_pipeline(mut self, plan: LogicalOperator) -> Result<Pipeline> {
        self.walk_plan(plan)?;
        self.create_complete_chain()?;

        let chains = self
            .completed_chains
            .into_iter()
            .map(|chain| Arc::new(chain))
            .collect();

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
            LogicalOperator::Empty => self.plan_empty(),
            other => unimplemented!("other: {other:?}"),
        }
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
            .map(|p| PhysicalScalarExpression::try_from_uncorrelated_expr(p))
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
                .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr))
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

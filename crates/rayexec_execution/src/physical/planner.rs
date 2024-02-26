use super::{
    plans::{projection::PhysicalProjection, Sink},
    Destination, LinkedOperator, PhysicalOperator, Pipeline,
};
use crate::{
    expr::{execute::MultiScalarExecutor, Expression, PhysicalExpr},
    functions::table::Pushdown,
    physical::plans::{filter::PhysicalFilter, values::PhysicalValues},
    planner::{
        bind_context::BindContext,
        operator::{self, LogicalOperator},
    },
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
    pub fn create_plan(
        &self,
        plan: LogicalOperator,
        context: &BindContext,
        dest: Box<dyn Sink>,
    ) -> Result<Pipeline> {
        let mut builder = PipelineBuilder::new(dest, context);
        builder.walk_plan(plan, Destination::PipelineOutput)?;

        Ok(builder.pipeline)
    }
}

/// Iteratively builds up a pipeline from a logical plan.
#[derive(Debug)]
struct PipelineBuilder<'a> {
    pipeline: Pipeline,
    context: &'a BindContext,
}

impl<'a> PipelineBuilder<'a> {
    /// Create a new builder for a pipeline that outputs the final result to
    /// `dest`.
    fn new(dest: Box<dyn Sink>, context: &'a BindContext) -> Self {
        let pipeline = Pipeline::new_empty(dest);
        PipelineBuilder { pipeline, context }
    }

    /// Recursively walks the provided plan, creating physical operators along
    /// the the way and adding them to the pipeline.
    fn walk_plan(&mut self, plan: LogicalOperator, output: Destination) -> Result<()> {
        match plan {
            LogicalOperator::Projection(proj) => self.plan_projection(proj, output),
            LogicalOperator::Filter(filter) => self.plan_filter(filter, output),
            LogicalOperator::Scan(scan) => self.plan_scan(scan, output),
            LogicalOperator::Values(values) => self.plan_values(values, output),
            _ => unimplemented!(),
        }
    }

    fn plan_projection(&mut self, proj: operator::Projection, output: Destination) -> Result<()> {
        // Plan projection.
        let operator = PhysicalProjection::try_new(proj.exprs)?;
        let linked = LinkedOperator {
            operator: Arc::new(operator),
            dest: output,
        };

        let idx = self.pipeline.push(linked);
        let dest = Destination::Operator {
            operator: idx,
            child: 0,
        };

        // Plan child, who's output will be pushed into the projection.
        self.walk_plan(*proj.input, dest)?;

        Ok(())
    }

    fn plan_filter(&mut self, filter: operator::Filter, output: Destination) -> Result<()> {
        // Plan filter.
        let input = DataBatchSchema::new(Vec::new()); // TODO
        let operator = PhysicalFilter::try_new(filter.predicate, &input)?;
        let linked = LinkedOperator {
            operator: Arc::new(operator),
            dest: output,
        };

        let idx = self.pipeline.push(linked);
        let dest = Destination::Operator {
            operator: idx,
            child: 0,
        };

        // Plan child, who's output will be pushed into the filter.
        self.walk_plan(*filter.input, dest)?;

        Ok(())
    }

    fn plan_scan(&mut self, scan: operator::Scan, output: Destination) -> Result<()> {
        let table = self.context.get_table(scan.input).ok_or_else(|| {
            RayexecError::new(format!("Missing table for bind index: {:?}", scan.input))
        })?;

        // TODO: If no projection, just do everything.
        let operator = table.create_operator(scan.projection.unwrap().clone(), Pushdown::default());
        let linked = LinkedOperator {
            operator,
            dest: output,
        };

        self.pipeline.operators.push(linked);

        Ok(())
    }

    fn plan_values(&mut self, values: operator::Values, output: Destination) -> Result<()> {
        let mut row_arrs: Vec<Vec<ArrayRef>> = Vec::new(); // Row oriented.

        let dummy_batch = DataBatch::empty_with_num_rows(1);

        // Convert expressions into arrays of one element each.
        for row_exprs in values.rows {
            let executor = MultiScalarExecutor::try_new(row_exprs)?;
            let arrs = executor.eval(&dummy_batch)?;
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
        let operator = PhysicalValues::new(batch);
        let linked = LinkedOperator {
            operator: Arc::new(operator),
            dest: output,
        };

        self.pipeline.operators.push(linked);

        Ok(())
    }
}

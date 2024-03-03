use arrow_array::RecordBatch;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, parser};
use std::sync::Arc;
use tracing::trace;

use crate::{
    functions::table::{self, TableFunction},
    physical::{planner::PhysicalPlanner, scheduler::Scheduler, Pipeline2},
    planner::{plan::PlanContext, Resolver},
    types::batch::DataBatchSchema,
};

use super::materialize::MaterializedBatchStream;

#[derive(Debug, Default, Clone, Copy)]
pub struct DebugResolver;

impl Resolver for DebugResolver {
    // fn resolve_for_table_scan(
    //     &self,
    //     reference: &ast::ObjectReference,
    // ) -> Result<Box<dyn TableFunction>> {
    //     unimplemented!()
    // }

    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>> {
        if reference.0.len() != 1 {
            return Err(RayexecError::new("Expected a single ident"));
        }

        Ok(match reference.0[0].value.as_ref() {
            "dummy" => Box::new(table::dummy::DummyTableFunction),
            "generate_series" => Box::new(table::generate_series::GenerateSeries),
            "read_csv" => Box::new(table::read_csv::ReadCsv),
            other => return Err(RayexecError::new(format!("unknown function: {other}"))),
        })
    }
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: DataBatchSchema,
    pub stream: MaterializedBatchStream,
}

#[derive(Debug)]
pub struct Session {
    scheduler: Scheduler,
}

impl Session {
    pub fn new(scheduler: Scheduler) -> Self {
        Session { scheduler }
    }

    pub fn execute(&self, sql: &str) -> Result<ExecutionResult> {
        let stmts = parser::parse(sql)?;
        if stmts.len() != 1 {
            return Err(RayexecError::new("Expected one statement")); // TODO, allow any number
        }
        let mut stmts = stmts.into_iter();

        let plan_context = PlanContext::new(&DebugResolver);
        let logical = plan_context.plan_statement(stmts.next().unwrap())?;
        trace!(?logical, "logical plan created");

        // let optimizer = Optimizer::new();
        // let logical = optimizer.optimize(&context, logical)?;

        let mut output_stream = MaterializedBatchStream::new();

        let physical_planner = PhysicalPlanner::new();
        let pipeline = physical_planner.create_plan(logical.root, output_stream.take_sink()?)?;
        trace!(?pipeline, "physical plan created");

        self.scheduler.execute(pipeline)?;

        Ok(ExecutionResult {
            output_schema: DataBatchSchema::new(Vec::new()), // TODO
            stream: output_stream,
        })
    }
}

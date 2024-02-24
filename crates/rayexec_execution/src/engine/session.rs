use arrow_array::RecordBatch;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, parser};
use std::sync::Arc;

use crate::{
    engine::materialize::MaterializedBatches,
    functions::table::{self, TableFunction},
    optimizer::Optimizer,
    physical::{planner::PhysicalPlanner, scheduler::Scheduler, Pipeline},
    planner::planner::{Planner, Resolver},
};

#[derive(Debug, Default, Clone, Copy)]
pub struct DebugResolver;

impl Resolver for DebugResolver {
    fn resolve_for_table_scan(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>> {
        unimplemented!()
    }

    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunction>> {
        if reference.0.len() != 1 {
            return Err(RayexecError::new("Expected a single ident"));
        }

        Ok(match reference.0[0].value.as_ref() {
            "dummy" => Box::new(table::dummy::DummyTableFunction),
            other => return Err(RayexecError::new(format!("unknown function: {other}"))),
        })
    }
}

#[derive(Debug)]
pub struct Session {
    scheduler: Scheduler,
}

impl Session {
    pub fn new(scheduler: Scheduler) -> Self {
        Session { scheduler }
    }

    pub fn execute(&self, sql: &str) -> Result<Arc<MaterializedBatches>> {
        let stmts = parser::parse(sql)?;
        if stmts.len() != 1 {
            return Err(RayexecError::new("Expected one statement")); // TODO, allow any number
        }
        let mut stmts = stmts.into_iter();

        let planner = Planner::new(DebugResolver);
        let (logical, context) = planner.plan_statement(stmts.next().unwrap())?;

        let optimizer = Optimizer::new();
        let logical = optimizer.optimize(&context, logical)?;

        let dest = Arc::new(MaterializedBatches::new());

        let physical_planner = PhysicalPlanner::new();
        let pipeline = physical_planner.create_plan(logical, &context, dest.clone())?;

        self.scheduler.execute(pipeline)?;

        Ok(dest)
    }
}

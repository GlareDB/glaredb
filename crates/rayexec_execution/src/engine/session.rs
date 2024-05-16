use crossbeam::channel::TryRecvError;
use hashbrown::HashMap;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, parser};

use tracing::trace;

use crate::{
    engine::result_stream::unpartitioned_result_stream,
    execution::query_graph::{
        planner::{QueryGraphDebugConfig, QueryGraphPlanner},
        sink::QuerySink,
    },
    functions::{
        aggregate::{self, GenericAggregateFunction, ALL_AGGREGATE_FUNCTIONS},
        scalar::{GenericScalarFunction, ALL_SCALAR_FUNCTIONS},
        table::{self, TableFunctionOld},
    },
    optimizer::Optimizer,
    planner::{plan::PlanContext, Resolver},
    scheduler::Scheduler,
};

use super::{
    modify::{Modification, SessionModifier},
    result_stream::ResultStream,
    vars::{SessionVar, SessionVars},
};

#[derive(Debug)]
struct SessionFunctions {
    scalars: HashMap<&'static str, &'static Box<dyn GenericScalarFunction>>,
    aggregates: HashMap<&'static str, &'static Box<dyn GenericAggregateFunction>>,
}

impl SessionFunctions {
    fn new() -> Self {
        // TODO: We wouldn't create this every time. Also these would be placed
        // inside a builtin schema.
        let mut scalars = HashMap::new();
        for func in ALL_SCALAR_FUNCTIONS.iter() {
            scalars.insert(func.name(), func);
            for alias in func.aliases() {
                scalars.insert(alias, func);
            }
        }

        let mut aggregates = HashMap::new();
        for func in ALL_AGGREGATE_FUNCTIONS.iter() {
            aggregates.insert(func.name(), func);
            for alias in func.aliases() {
                aggregates.insert(alias, func);
            }
        }

        SessionFunctions {
            scalars,
            aggregates,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DebugResolver<'a> {
    vars: &'a SessionVars,
    functions: &'a SessionFunctions,
}

impl<'a> Resolver for DebugResolver<'a> {
    fn resolve_scalar_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Option<Box<dyn GenericScalarFunction>> {
        if reference.0.len() != 1 {
            return None;
        }
        let func = self.functions.scalars.get(reference.0[0].value.as_str())?;
        Some((*func).clone())
    }

    fn resolve_aggregate_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Option<Box<dyn GenericAggregateFunction>> {
        if reference.0.len() != 1 {
            return None;
        }
        let func = self
            .functions
            .aggregates
            .get(reference.0[0].value.as_str())?;
        Some((*func).clone())
    }

    fn resolve_table_function(
        &self,
        reference: &ast::ObjectReference,
    ) -> Result<Box<dyn TableFunctionOld>> {
        if reference.0.len() != 1 {
            return Err(RayexecError::new("Expected a single ident"));
        }

        Ok(match reference.0[0].value.as_ref() {
            "dummy" => Box::new(table::dummy::DummyTableFunction),
            "generate_series" => Box::new(table::generate_series::GenerateSeries),
            "read_csv" => Box::new(table::csv::read_csv::ReadCsv),
            "sniff_csv" => Box::new(table::csv::sniff_csv::SniffCsv),
            other => return Err(RayexecError::new(format!("unknown function: {other}"))),
        })
    }

    fn get_session_variable(&self, name: &str) -> Result<SessionVar> {
        self.vars.get_var(name).cloned()
    }
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: ResultStream,
}

#[derive(Debug)]
pub struct Session {
    pub(crate) modifications: SessionModifier,
    pub(crate) vars: SessionVars,
    pub(crate) scheduler: Scheduler,
    pub(crate) functions: SessionFunctions,
}

impl Session {
    pub fn new(scheduler: Scheduler) -> Self {
        Session {
            modifications: SessionModifier::new(),
            scheduler,
            vars: SessionVars::new_local(),
            functions: SessionFunctions::new(),
        }
    }

    pub fn execute(&mut self, sql: &str) -> Result<ExecutionResult> {
        // Only thing that requires mut.
        self.apply_pending_modifications()
            .expect("modications to be infallible");

        let stmts = parser::parse(sql)?;
        if stmts.len() != 1 {
            return Err(RayexecError::new("Expected one statement")); // TODO, allow any number
        }
        let mut stmts = stmts.into_iter();

        let resolver = DebugResolver {
            vars: &self.vars,
            functions: &self.functions,
        };
        let plan_context = PlanContext::new(&resolver);
        let logical = plan_context.plan_statement(stmts.next().unwrap())?;

        let (result_stream, result_sink) = unpartitioned_result_stream();
        let planner = QueryGraphPlanner::new(1, QueryGraphDebugConfig::default());
        let query_graph = planner.create_graph(logical.root, QuerySink::new([result_sink]))?;

        self.scheduler.spawn_query_graph(query_graph);

        Ok(ExecutionResult {
            output_schema: Schema::empty(), // TODO
            stream: result_stream,
        })

        // let optimizer = Optimizer::new();
        // logical.root = optimizer.optimize(logical.root)?;

        // let mut output_stream = MaterializedBatchStream::new();

        // let physical_planner = PhysicalPlanner::try_new_from_vars(&self.vars)?;
        // let pipeline = physical_planner.create_plan(logical.root, output_stream.take_sink()?)?;

        // let context = TaskContext {
        //     modifications: Some(self.modifications.clone_sender()),
        // };

        // self.scheduler.execute(pipeline, context)?;

        // Ok(ExecutionResult {
        //     output_schema: Schema::empty(), // TODO
        //     stream: output_stream,
        // })
    }

    fn apply_pending_modifications(&mut self) -> Result<()> {
        let recv = self.modifications.get_recv();
        loop {
            match recv.try_recv() {
                Ok(modification) => match modification {
                    Modification::UpdateVariable { name, value } => {
                        self.vars.set_var(&name, value)?
                    }
                    _ => unimplemented!(),
                },
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => {
                    return Err(RayexecError::new(
                        "session modification channel disconnected",
                    ))
                }
            }
        }
    }
}

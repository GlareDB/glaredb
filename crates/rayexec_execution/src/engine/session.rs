use std::sync::Arc;

use hashbrown::HashMap;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result};
use rayexec_parser::{parser, statement::RawStatement};

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    engine::result_stream::unpartitioned_result_stream,
    execution::query_graph::{
        planner::{QueryGraphDebugConfig, QueryGraphPlanner},
        sink::QuerySink,
    },
    logical::{
        operator::{LogicalOperator, ResetVar, VariableOrAll},
        sql::{binder::Binder, planner::PlanContext},
    },
    optimizer::Optimizer,
};

use super::{
    result_stream::ResultStream,
    vars::{SessionVars, VarAccessor},
    DataSourceRegistry, EngineRuntime,
};

#[derive(Debug)]
pub struct ExecutionResult {
    pub output_schema: Schema,
    pub stream: ResultStream,
}

#[derive(Debug)]
pub struct Session {
    context: DatabaseContext,
    vars: SessionVars,
    registry: Arc<DataSourceRegistry>,
    runtime: Arc<EngineRuntime>,

    prepared: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
}

impl Session {
    pub fn new(runtime: Arc<EngineRuntime>, registry: Arc<DataSourceRegistry>) -> Self {
        Session {
            context: DatabaseContext::new_with_temp(),
            runtime,
            registry,
            vars: SessionVars::new_local(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// Get execution results from one or more sql queries.
    ///
    /// Analogous to postgres' simple query protocol. Goes through all prepatory
    /// steps for query parsing and planning, returning execution results.
    ///
    /// Execution result streams should be read in order.
    ///
    /// Uses the unnamed ("") keys for prepared statements and portals.
    pub async fn simple(&mut self, sql: &str) -> Result<Vec<ExecutionResult>> {
        let stmts = self.parse(sql)?;
        let mut results = Vec::with_capacity(stmts.len());

        const UNNAMED: &str = "";

        for stmt in stmts {
            self.prepare(UNNAMED, stmt)?;
            self.bind(UNNAMED, UNNAMED)?;
            let result = self.execute(UNNAMED).await?;
            results.push(result);
        }

        Ok(results)
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<RawStatement>> {
        parser::parse(sql)
    }

    pub fn prepare(&mut self, name: impl Into<String>, stmt: RawStatement) -> Result<()> {
        self.prepared
            .insert(name.into(), PreparedStatement { statement: stmt });
        Ok(())
    }

    pub fn bind(&mut self, stmt: &str, portal: impl Into<String>) -> Result<()> {
        let stmt = self.prepared.get(stmt).ok_or_else(|| {
            RayexecError::new(format!("Missing named prepared statement: '{stmt}'"))
        })?;
        self.portals.insert(
            portal.into(),
            Portal {
                statement: stmt.statement.clone(),
            },
        );
        Ok(())
    }

    pub async fn execute(&mut self, portal: &str) -> Result<ExecutionResult> {
        let stmt = self
            .portals
            .get(portal)
            .map(|p| p.statement.clone())
            .ok_or_else(|| RayexecError::new(format!("Missing portal: '{portal}'")))?;

        let tx = CatalogTx::new();
        let (bound_stmt, bind_data) = Binder::new(&tx, &self.context).bind_statement(stmt).await?;
        let mut logical = PlanContext::new(&self.vars, &bind_data).plan_statement(bound_stmt)?;

        let optimizer = Optimizer::new();
        logical.root = optimizer.optimize(logical.root)?;

        let (result_stream, result_sink) = unpartitioned_result_stream();
        let planner = QueryGraphPlanner::new(
            &self.context,
            VarAccessor::new(&self.vars).partitions(),
            QueryGraphDebugConfig::new(&self.vars),
        );
        let query_sink = QuerySink::new([result_sink]);

        let query_graph = match logical.root {
            LogicalOperator::AttachDatabase(attach) => {
                let empty = planner.create_graph(LogicalOperator::Empty, query_sink)?; // Here to avoid lifetime issues.

                // TODO: No clue if we want to do this here. What happens during
                // hybrid exec?
                let datasource = self.registry.get_datasource(&attach.datasource)?;
                let catalog = datasource
                    .create_catalog(&self.runtime, attach.options)
                    .await?;
                self.context.attach_catalog(attach.name, catalog)?;
                empty
            }
            LogicalOperator::DetachDatabase(detach) => {
                let empty = planner.create_graph(LogicalOperator::Empty, query_sink)?; // Here to avoid lifetime issues.
                self.context.detach_catalog(&detach.name)?;
                empty
            }
            LogicalOperator::SetVar(set_var) => {
                // TODO: Do we want this logic to exist here?
                //
                // SET seems fine, but what happens with things like wanting to
                // update the catalog? Possibly an "external resources context"
                // that has clients/etc for everything that the session can look
                // at to update its local state?
                //
                // We could have an implementation for the local session, and a
                // separate implementation used for nodes taking part in
                // distributed execution.
                let val = self
                    .vars
                    .try_cast_scalar_value(&set_var.name, set_var.value)?;
                self.vars.set_var(&set_var.name, val)?;
                planner.create_graph(LogicalOperator::Empty, query_sink)?
            }
            LogicalOperator::ResetVar(ResetVar { var }) => {
                // Same TODO as above.
                match var {
                    VariableOrAll::Variable(v) => self.vars.reset_var(v.name)?,
                    VariableOrAll::All => self.vars.reset_all(),
                }
                planner.create_graph(LogicalOperator::Empty, query_sink)?
            }
            root => planner.create_graph(root, query_sink)?,
        };

        self.runtime
            .scheduler
            .spawn_query_graph(query_graph, result_stream.errors_send_channel());

        Ok(ExecutionResult {
            output_schema: Schema::empty(), // TODO
            stream: result_stream,
        })
    }
}

#[derive(Debug)]
struct PreparedStatement {
    statement: RawStatement,
}

#[derive(Debug)]
struct Portal {
    statement: RawStatement,
}

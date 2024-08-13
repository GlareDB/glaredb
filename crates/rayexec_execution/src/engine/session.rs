use std::sync::Arc;

use hashbrown::HashMap;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_parser::{parser, statement::RawStatement};
use uuid::Uuid;

use crate::{
    database::{catalog::CatalogTx, DatabaseContext},
    execution::{
        executable::planner::{ExecutablePipelinePlanner, ExecutionConfig, PlanLocationState},
        intermediate::planner::{IntermediateConfig, IntermediatePipelinePlanner},
    },
    hybrid::client::HybridClient,
    logical::{
        binder::{BindMode, Binder},
        context::QueryContext,
        operator::{LogicalOperator, VariableOrAll},
        planner::plan_statement::PlanContext,
    },
    optimizer::Optimizer,
    runtime::{PipelineExecutor, Runtime},
};

use super::{
    result::{new_results_sinks, ExecutionResult},
    vars::{SessionVars, VarAccessor},
    DataSourceRegistry,
};

/// A "client" session capable of executing queries from arbitrary sql
/// statements.
///
/// The general flow of execution follows the postgres extended protocol, with
/// the flow being:
///
/// 1. Receive sql statement, parse.
/// 2. Prepare parsed sql statement.
/// 3. Move prepared statement to portal.
/// 4. Executed statement inside portal.
/// 5. Stream results...
///
/// The `simple` method can take care of all of these steps, letting
/// you go from a sql statement to the result stream(s).
///
/// Note that most methods take a mutable reference. This is by design. We don't
/// want to lock any of the internal structures. During parsing or planning.
/// This does mean that synchronization has to happen at a higher level, either
/// by keeping the session directly on a stream (postgres protocol) or wrapping
/// in a mutex.
///
/// If the session is wrapped in a mutex, the lock can be safely dropped once
/// the `ExecutionResult`s are returned. This allows not having to hold the lock
/// during actual execution (pulling on the stream) as execution is completely
/// independent of any state inside the session.
#[derive(Debug)]
pub struct Session<P: PipelineExecutor, R: Runtime> {
    /// Context containg everything in the "database" that's visible to this
    /// session.
    context: DatabaseContext,

    /// Variables for this session.
    vars: SessionVars,

    /// Reference configured data source implementations.
    registry: Arc<DataSourceRegistry>,

    /// Runtime for accessing external resources like the filesystem or http
    /// clients.
    #[allow(dead_code)] // TODO: Might just remove this field.
    runtime: R,

    /// Pipeline executor.
    executor: P,

    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,

    /// Portals for statements ready to be executed.
    portals: HashMap<String, Portal>,

    /// Client for hybrid execution if enabled.
    hybrid_client: Option<Arc<HybridClient<R::HttpClient>>>,
}

impl<P, R> Session<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(
        context: DatabaseContext,
        executor: P,
        runtime: R,
        registry: Arc<DataSourceRegistry>,
    ) -> Self {
        Session {
            context,
            runtime,
            executor,
            registry,
            vars: SessionVars::new_local(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            hybrid_client: None,
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

        let bindmode = if self.hybrid_client.is_some() {
            BindMode::Hybrid
        } else {
            BindMode::Normal
        };

        let (bound_stmt, bind_data) = Binder::new(
            bindmode,
            &tx,
            &self.context,
            self.registry.get_file_handlers(),
        )
        .bind_statement(stmt)
        .await?;

        let (stream, sink, errors) = new_results_sinks();

        let (pipelines, output_schema) = match bindmode {
            BindMode::Hybrid if bind_data.any_unbound() => {
                // Hybrid planning, send to remote to complete planning.

                let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
                let resp = hybrid_client
                    .remote_plan(bound_stmt, bind_data, &self.context)
                    .await?;

                // Begin executing remote side.
                hybrid_client.remote_execute(resp.query_id).await?;

                (resp.pipelines, resp.schema)
            }
            _ => {
                // Normal all-local planning.

                let (mut logical, context) =
                    PlanContext::new(&self.vars, &bind_data).plan_statement(bound_stmt)?;

                let optimizer = Optimizer::new();
                logical.root = optimizer.optimize(logical.root)?;
                let schema = logical.schema()?;

                let query_id = Uuid::new_v4();
                let planner = IntermediatePipelinePlanner::new(
                    IntermediateConfig::from_session_vars(&self.vars),
                    query_id,
                );

                let pipelines = match logical.root {
                    LogicalOperator::AttachDatabase(attach) => {
                        let attach = attach.into_inner();
                        // Here to avoid lifetime issues.
                        let empty =
                            planner.plan_pipelines(LogicalOperator::EMPTY, QueryContext::new())?;

                        // TODO: No clue if we want to do this here. What happens during
                        // hybrid exec?
                        let datasource = self.registry.get_datasource(&attach.datasource)?;
                        let catalog = datasource.create_catalog(attach.options).await?;
                        self.context.attach_catalog(&attach.name, catalog)?;
                        empty
                    }
                    LogicalOperator::DetachDatabase(detach) => {
                        let empty =
                            planner.plan_pipelines(LogicalOperator::EMPTY, QueryContext::new())?; // Here to avoid lifetime issues.
                        self.context.detach_catalog(&detach.as_ref().name)?;
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
                        let set_var = set_var.into_inner();
                        let val = self
                            .vars
                            .try_cast_scalar_value(&set_var.name, set_var.value)?;
                        self.vars.set_var(&set_var.name, val)?;
                        planner.plan_pipelines(LogicalOperator::EMPTY, QueryContext::new())?
                    }
                    LogicalOperator::ResetVar(reset) => {
                        // Same TODO as above.
                        match &reset.as_ref().var {
                            VariableOrAll::Variable(v) => self.vars.reset_var(v.name)?,
                            VariableOrAll::All => self.vars.reset_all(),
                        }
                        planner.plan_pipelines(LogicalOperator::EMPTY, QueryContext::new())?
                    }
                    root => planner.plan_pipelines(root, context)?,
                };

                if !pipelines.remote.is_empty() {
                    return Err(RayexecError::new(
                        "Remote pipelines should not have been planned",
                    ));
                }

                (pipelines.local, schema)
            }
        };

        let mut planner = ExecutablePipelinePlanner::<R>::new(
            &self.context,
            ExecutionConfig {
                target_partitions: VarAccessor::new(&self.vars).partitions(),
            },
            PlanLocationState::Client {
                output_sink: Some(Box::new(sink)),
                hybrid_client: self.hybrid_client.as_ref(),
            },
        );

        let pipelines = planner.plan_from_intermediate(pipelines)?;
        let handle = self.executor.spawn_pipelines(pipelines, Arc::new(errors));

        Ok(ExecutionResult {
            output_schema,
            stream,
            handle,
        })
    }

    pub fn set_hybrid(&mut self, client: HybridClient<R::HttpClient>) {
        self.hybrid_client = Some(Arc::new(client));
    }

    pub fn unset_hybrid(&mut self) {
        self.hybrid_client = None;
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

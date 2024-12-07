use std::sync::Arc;

use hashbrown::HashMap;
use rayexec_bullet::field::{Field, Schema};
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_parser::parser;
use rayexec_parser::statement::RawStatement;
use uuid::Uuid;

use super::profiler::PlanningProfileData;
use super::result::{new_results_sinks, ExecutionResult, ResultErrorSink, ResultStream};
use super::DataSourceRegistry;
use crate::config::vars::SessionVars;
use crate::database::catalog::CatalogTx;
use crate::database::memory_catalog::MemoryCatalog;
use crate::database::{AttachInfo, Database, DatabaseContext};
use crate::execution::executable::pipeline::ExecutablePipeline;
use crate::execution::executable::planner::{ExecutablePipelinePlanner, PlanLocationState};
use crate::execution::intermediate::planner::IntermediatePipelinePlanner;
use crate::execution::intermediate::{IntermediateMaterializationGroup, IntermediatePipelineGroup};
use crate::hybrid::client::HybridClient;
use crate::logical::binder::bind_statement::StatementBinder;
use crate::logical::logical_attach::LogicalAttachDatabase;
use crate::logical::logical_set::VariableOrAll;
use crate::logical::operator::{LogicalOperator, Node};
use crate::logical::planner::plan_statement::StatementPlanner;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::{ResolveMode, ResolvedStatement, Resolver};
use crate::optimizer::Optimizer;
use crate::runtime::time::Timer;
use crate::runtime::{PipelineExecutor, Runtime};

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

#[derive(Debug)]
struct PreparedStatement {
    statement: RawStatement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionMode {
    /// Locally only execution.
    LocalOnly,
    /// Local and remote execution. Should trigger remote execution prior to
    /// executing local pipelines.
    Hybrid,
}

#[derive(Debug)]
struct Portal {
    /// Query id for this query. Used to begin execution on the remote side if
    /// needed.
    query_id: Uuid,
    /// Execution mode of the query.
    ///
    /// This may differ from the resolve mode. For example, hybrid resolving may
    /// result in only local pipelines, which would indicated local-only
    /// execution.
    execution_mode: ExecutionMode,
    /// Pipelines we'll be executing on this node.
    executable_pipelines: Vec<ExecutablePipeline>,
    /// Output schema of the query.
    output_schema: Schema,
    /// Where results will be sent to.
    result_stream: ResultStream,
    /// Where errors will be sent do.
    error_sink: ResultErrorSink,
    /// Profile data we've collected during resolving/binding/planning.
    profile: PlanningProfileData,
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
        let stmts = parser::parse(sql)?;
        let mut results = Vec::with_capacity(stmts.len());

        const UNNAMED: &str = "";

        for stmt in stmts {
            self.prepare(UNNAMED, stmt)?;
            self.bind(UNNAMED, UNNAMED).await?;
            let result = self.execute(UNNAMED).await?;
            results.push(result);
        }

        Ok(results)
    }

    // TODO: Typed parameters at some point.
    pub fn prepare(&mut self, prepared_name: impl Into<String>, stmt: RawStatement) -> Result<()> {
        self.prepared
            .insert(prepared_name.into(), PreparedStatement { statement: stmt });
        Ok(())
    }

    pub async fn bind(
        &mut self,
        prepared_name: &str,
        portal_name: impl Into<String>,
    ) -> Result<()> {
        let stmt = self.prepared.get(prepared_name).ok_or_else(|| {
            RayexecError::new(format!(
                "Missing named prepared statement: '{prepared_name}'"
            ))
        })?;

        let mut profile = PlanningProfileData::default();

        // TODO: Store tx state on session.
        let tx = CatalogTx::new();

        let resolve_mode = if self.hybrid_client.is_some() {
            ResolveMode::Hybrid
        } else {
            ResolveMode::Normal
        };

        let timer = Timer::<R::Instant>::start();
        let (resolved_stmt, resolve_context) = Resolver::new(
            resolve_mode,
            &tx,
            &self.context,
            self.registry.get_file_handlers(),
        )
        .resolve_statement(stmt.statement.clone())
        .await?;
        profile.resolve_step = Some(timer.stop());

        let (execution_mode, query_id, pipelines, materializations, schema) = self
            .plan_intermediate(resolved_stmt, resolve_context, resolve_mode, &mut profile)
            .await?;

        let (stream, sink, errors) = new_results_sinks();

        let mut planner = ExecutablePipelinePlanner::<R>::new(
            &self.context,
            self.vars.executable_plan_config(),
            PlanLocationState::Client {
                output_sink: Some(sink),
                hybrid_client: self.hybrid_client.as_ref(),
            },
        );

        let timer = Timer::<R::Instant>::start();
        let pipelines = planner.plan_from_intermediate(pipelines, materializations)?;
        profile.plan_executable_step = Some(timer.stop());

        self.portals.insert(
            portal_name.into(),
            Portal {
                query_id,
                execution_mode,
                executable_pipelines: pipelines,
                output_schema: schema,
                result_stream: stream,
                error_sink: errors,
                profile,
            },
        );
        Ok(())
    }

    /// Plans the intermediate pipelines from a resolved statement.
    async fn plan_intermediate(
        &mut self,
        stmt: ResolvedStatement,
        resolve_context: ResolveContext,
        resolve_mode: ResolveMode,
        profile: &mut PlanningProfileData,
    ) -> Result<(
        ExecutionMode,
        Uuid,
        IntermediatePipelineGroup,
        IntermediateMaterializationGroup,
        Schema,
    )> {
        match resolve_mode {
            ResolveMode::Hybrid if resolve_context.any_unresolved() => {
                // Hybrid planning, send to remote to complete planning.

                let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
                let resp = hybrid_client
                    .remote_plan(stmt, resolve_context, &self.context)
                    .await?;

                Ok((
                    ExecutionMode::Hybrid,
                    resp.query_id,
                    resp.pipelines,
                    IntermediateMaterializationGroup::default(),
                    resp.schema,
                ))
            }
            _ => {
                // Normal all-local planning.

                let binder = StatementBinder {
                    session_vars: &self.vars,
                    resolve_context: &resolve_context,
                };
                let timer = Timer::<R::Instant>::start();
                let (bound_stmt, mut bind_context) = binder.bind(stmt)?;
                profile.bind_step = Some(timer.stop());

                let timer = Timer::<R::Instant>::start();
                let mut logical = StatementPlanner.plan(&mut bind_context, bound_stmt)?;
                profile.plan_logical_step = Some(timer.stop());

                let mut optimizer = Optimizer::new();
                logical = optimizer.optimize::<R::Instant>(&mut bind_context, logical)?;
                profile.optimizer_step = Some(optimizer.profile_data);

                // If we're an explain, put a copy of the optimized plan on the
                // node.
                if let LogicalOperator::Explain(explain) = &mut logical {
                    let child = explain
                        .children
                        .first()
                        .ok_or_else(|| RayexecError::new("Missing explain child"))?;
                    explain.node.logical_optimized = Some(Box::new(child.clone()));
                }

                let schema = Schema::new(
                    bind_context
                        .iter_tables(bind_context.root_scope_ref())?
                        .flat_map(|t| {
                            t.column_names
                                .iter()
                                .zip(&t.column_types)
                                .map(|(name, datatype)| Field::new(name, datatype.clone(), true))
                        }),
                );

                let query_id = Uuid::new_v4();
                let planner = IntermediatePipelinePlanner::new(
                    self.vars.intermediate_plan_config(),
                    query_id,
                );

                let pipelines = match logical {
                    LogicalOperator::AttachDatabase(attach) => {
                        self.handle_attach_database(attach).await?;
                        planner.plan_pipelines(LogicalOperator::EMPTY, bind_context)?
                    }
                    LogicalOperator::DetachDatabase(detach) => {
                        let empty = planner.plan_pipelines(LogicalOperator::EMPTY, bind_context)?; // Here to avoid lifetime issues.
                        self.context.detach_database(&detach.as_ref().name)?;
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
                        planner.plan_pipelines(LogicalOperator::EMPTY, bind_context)?
                    }
                    LogicalOperator::ResetVar(reset) => {
                        // Same TODO as above.
                        match &reset.as_ref().var {
                            VariableOrAll::Variable(v) => self.vars.reset_var(v.name)?,
                            VariableOrAll::All => self.vars.reset_all(),
                        }
                        planner.plan_pipelines(LogicalOperator::EMPTY, bind_context)?
                    }
                    root => {
                        let timer = Timer::<R::Instant>::start();
                        let pipelines = planner.plan_pipelines(root, bind_context)?;
                        profile.plan_intermediate_step = Some(timer.stop());
                        pipelines
                    }
                };

                if !pipelines.remote.is_empty() {
                    return Err(RayexecError::new(
                        "Remote pipelines should not have been planned",
                    ));
                }

                Ok((
                    ExecutionMode::LocalOnly,
                    query_id,
                    pipelines.local,
                    pipelines.materializations,
                    schema,
                ))
            }
        }
    }

    pub async fn execute(&mut self, portal_name: &str) -> Result<ExecutionResult> {
        let portal = self
            .portals
            .remove(portal_name)
            .ok_or_else(|| RayexecError::new(format!("Missing portal: '{portal_name}'")))?;

        if portal.execution_mode == ExecutionMode::Hybrid {
            // Need to begin execution on the remote side.
            let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
            hybrid_client.remote_execute(portal.query_id).await?;
        }

        let handle = self
            .executor
            .spawn_pipelines(portal.executable_pipelines, Arc::new(portal.error_sink));

        Ok(ExecutionResult {
            planning_profile: portal.profile,
            output_schema: portal.output_schema,
            stream: portal.result_stream,
            handle: handle.into(),
        })
    }

    async fn handle_attach_database(&mut self, attach: Node<LogicalAttachDatabase>) -> Result<()> {
        // TODO: This should always be client local. Is there a case where we
        // want to have that not be the cases? What would the behavior be.
        let attach = attach.into_inner();

        let database = match self.registry.get_datasource(&attach.datasource) {
            Some(datasource) => {
                // We have data source implementation on the client. Try to
                // connect locally.
                let connection = datasource.connect(attach.options.clone()).await?;
                let catalog = Arc::new(MemoryCatalog::default());
                if let Some(catalog_storage) = connection.catalog_storage.as_ref() {
                    catalog_storage.initial_load(&catalog).await?;
                }

                Database {
                    catalog,
                    catalog_storage: connection.catalog_storage,
                    table_storage: Some(connection.table_storage),
                    attach_info: Some(AttachInfo {
                        datasource: attach.datasource.clone(),
                        options: attach.options,
                    }),
                }
            }
            None => {
                // We don't have a data source implementation on the client.
                let _client = match &self.hybrid_client {
                    Some(client) => client,
                    None => {
                        return Err(RayexecError::new(format!(
                        "Hybrid execution not enabled. Cannot verify attaching a '{}' data source",
                        attach.datasource,
                    )))
                    }
                };

                // TODO: Verify connection options using hybrid client.

                // Having no catalog storage will result in resolving always
                // kicking out to hybrid execution.
                Database {
                    catalog: Arc::new(MemoryCatalog::default()),
                    catalog_storage: None,
                    table_storage: None,
                    attach_info: Some(AttachInfo {
                        datasource: attach.datasource.clone(),
                        options: attach.options,
                    }),
                }
            }
        };

        self.context.attach_database(&attach.name, database)?;

        Ok(())
    }

    pub fn set_hybrid(&mut self, client: HybridClient<R::HttpClient>) {
        self.hybrid_client = Some(Arc::new(client));
    }

    pub fn unset_hybrid(&mut self) {
        self.hybrid_client = None;
    }
}

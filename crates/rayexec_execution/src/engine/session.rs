use std::sync::Arc;

use hashbrown::HashMap;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_parser::parser;
use rayexec_parser::statement::RawStatement;
use uuid::Uuid;

use super::profiler::PlanningProfileData;
use super::query_result::{Output, QueryResult};
use super::verifier::QueryVerifier;
use super::DataSourceRegistry;
use crate::arrays::field::{Field, Schema};
use crate::config::execution::OperatorPlanConfig;
use crate::config::session::SessionConfig;
use crate::database::catalog::CatalogTx;
use crate::database::memory_catalog::MemoryCatalog;
use crate::database::{AttachInfo, Database, DatabaseContext};
use crate::execution::executable::pipeline::ExecutablePipeline;
use crate::execution::operators::results::streaming::{PhysicalStreamingResults, ResultStream};
use crate::execution::operators::{ExecutionProperties, PushOperator};
use crate::execution::pipeline::ExecutablePipelineGraph;
use crate::execution::planner::{OperatorPlanner, QueryGraph};
use crate::explain::context_display::ContextDisplayMode;
use crate::explain::explainable::{ExplainConfig, Explainable};
use crate::explain::node::ExplainNode;
use crate::hybrid::client::HybridClient;
use crate::logical::binder::bind_statement::StatementBinder;
use crate::logical::logical_attach::LogicalAttachDatabase;
use crate::logical::logical_set::VariableOrAll;
use crate::logical::operator::{LogicalOperator, Node};
use crate::logical::planner::plan_statement::StatementPlanner;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::{ResolveConfig, ResolveMode, ResolvedStatement, Resolver};
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
    config: SessionConfig,
    /// Reference configured data source implementations.
    registry: Arc<DataSourceRegistry>,
    /// Runtime for accessing external resources like the filesystem or http
    /// clients.
    runtime: R,
    /// Pipeline executor.
    executor: P,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Portals for statements ready to be executed.
    portals: HashMap<String, ExecutablePortal>,
    /// Client for hybrid execution if enabled.
    hybrid_client: Option<Arc<HybridClient<R::HttpClient>>>,
}

#[derive(Debug)]
struct PreparedStatement {
    verifier: Option<QueryVerifier>,
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

/// Intermediate struct hold on to pipelines that still need to be planned for
/// execution.
#[derive(Debug)]
struct IntermediatePortal {
    query_id: Uuid,
    execution_mode: ExecutionMode,
    query_graph: QueryGraph,
    output_schema: Schema,
}

/// Portal containing executable pipelines.
#[derive(Debug)]
struct ExecutablePortal {
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
    executable_graph: ExecutablePipelineGraph,
    /// Output schema of the query.
    output_schema: Schema,
    /// Output result stream.
    result_stream: ResultStream,
    /// Profile data we've collected during resolving/binding/planning.
    profile: PlanningProfileData,
    /// Optional verifier that we're carrying through planning.
    verifier: Option<QueryVerifier>,
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
        let config = SessionConfig::new(&executor, &runtime);

        Session {
            context,
            runtime,
            executor,
            registry,
            config,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            hybrid_client: None,
        }
    }

    pub(crate) fn config_mut(&mut self) -> &mut SessionConfig {
        &mut self.config
    }

    /// Get execution results from one or more sql queries.
    ///
    /// Analogous to postgres' simple query protocol. Goes through all prepatory
    /// steps for query parsing and planning, returning execution results.
    ///
    /// Execution result streams should be read in order.
    ///
    /// Uses the unnamed ("") keys for prepared statements and portals.
    pub async fn simple(&mut self, sql: &str) -> Result<Vec<QueryResult>> {
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
        let verifier = if self.config.verify_optimized_plan {
            Some(QueryVerifier::new(stmt.clone()))
        } else {
            None
        };

        self.prepared.insert(
            prepared_name.into(),
            PreparedStatement {
                statement: stmt,
                verifier,
            },
        );
        Ok(())
    }

    /// Gets a prepared statement by name and generates intermedidate executable
    /// pipelines that get placed into a portal.
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
        let verifier = stmt.verifier.clone();

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
            ResolveConfig {
                enable_function_chaining: self.config.enable_function_chaining,
            },
        )
        .resolve_statement(stmt.statement.clone())
        .await?;
        profile.resolve_step = Some(timer.stop());

        let stream = ResultStream::new();

        let intermediate_portal = self
            .plan_intermediate(
                resolved_stmt,
                resolve_context,
                resolve_mode,
                &mut profile,
                PhysicalStreamingResults::new(stream.sink()),
            )
            .await?;

        let timer = Timer::<R::Instant>::start();
        let pipeline_graph = ExecutablePipelineGraph::plan_from_graph(
            &self.context,
            ExecutionProperties {
                batch_size: self.config.batch_size as usize,
            },
            intermediate_portal.query_graph,
        )?;
        profile.plan_executable_step = Some(timer.stop());

        self.portals.insert(
            portal_name.into(),
            ExecutablePortal {
                query_id: intermediate_portal.query_id,
                execution_mode: intermediate_portal.execution_mode,
                executable_graph: pipeline_graph,
                output_schema: intermediate_portal.output_schema,
                result_stream: stream,
                profile,
                verifier,
            },
        );

        Ok(())
    }

    /// Plans the intermediate pipelines from a resolved statement.
    ///
    /// If the resolve context indicates that not all objects were resolved,
    /// we'll call out to the remote side to complete planning.
    async fn plan_intermediate<O>(
        &mut self,
        stmt: ResolvedStatement,
        resolve_context: ResolveContext,
        resolve_mode: ResolveMode,
        profile: &mut PlanningProfileData,
        sink: O,
    ) -> Result<IntermediatePortal>
    where
        O: PushOperator,
    {
        match resolve_mode {
            ResolveMode::Hybrid if resolve_context.any_unresolved() => {
                // Hybrid planning, send to remote to complete planning.
                let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
                let resp = hybrid_client
                    .remote_plan(stmt, resolve_context, &self.context)
                    .await?;

                unimplemented!()
                // Ok(IntermediatePortal {
                //     query_id: resp.query_id,
                //     execution_mode: ExecutionMode::Hybrid,
                //     intermediate_pipelines: resp.pipelines,
                //     intermediate_materializations: IntermediateMaterializationGroup::default(), // TODO: Need to get these somehow.
                //     output_schema: resp.schema,
                // })
            }
            _ => {
                // Normal all-local planning.

                let binder = StatementBinder {
                    session_config: &self.config,
                    resolve_context: &resolve_context,
                };
                let timer = Timer::<R::Instant>::start();
                let (bound_stmt, mut bind_context) = binder.bind(stmt)?;
                profile.bind_step = Some(timer.stop());

                let timer = Timer::<R::Instant>::start();
                let mut logical = StatementPlanner.plan(&mut bind_context, bound_stmt)?;
                profile.plan_logical_step = Some(timer.stop());

                if self.config.enable_optimizer {
                    let mut optimizer = Optimizer::new();
                    logical = optimizer.optimize::<R::Instant>(&mut bind_context, logical)?;
                    profile.optimizer_step = Some(optimizer.profile_data);
                }

                // If we're an explain, put a copy of the optimized plan on the
                // node.
                if let LogicalOperator::Explain(explain) = &mut logical {
                    let child = explain
                        .children
                        .first()
                        .ok_or_else(|| RayexecError::new("Missing explain child"))?;

                    explain.node.logical_optimized = Some(ExplainNode::new_from_logical_plan(
                        &bind_context,
                        explain.node.verbose,
                        child,
                    ));
                }

                let schema = Schema::new(
                    bind_context
                        .iter_tables_in_scope(bind_context.root_scope_ref())?
                        .flat_map(|t| {
                            t.column_names
                                .iter()
                                .zip(&t.column_types)
                                .map(|(name, datatype)| Field::new(name, datatype.clone(), true))
                        }),
                );

                let query_id = Uuid::new_v4();
                let planner = OperatorPlanner::new(
                    OperatorPlanConfig {
                        allow_nested_loop_join: self.config.allow_nested_loop_join,
                    },
                    query_id,
                );

                let query_graph = match logical {
                    LogicalOperator::AttachDatabase(attach) => {
                        self.handle_attach_database(attach).await?;
                        planner.plan(LogicalOperator::EMPTY, bind_context, sink)?
                    }
                    LogicalOperator::DetachDatabase(detach) => {
                        let empty = planner.plan(LogicalOperator::EMPTY, bind_context, sink)?; // Here to avoid lifetime issues.
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
                        self.config
                            .set_from_scalar(&set_var.node.name, set_var.node.value)?;
                        planner.plan(LogicalOperator::EMPTY, bind_context, sink)?
                    }
                    LogicalOperator::ResetVar(reset) => {
                        // Same TODO as above.
                        match &reset.as_ref().var {
                            VariableOrAll::Variable(v) => {
                                self.config.reset(v, &self.executor, &self.runtime)?
                            }
                            VariableOrAll::All => {
                                self.config.reset_all(&self.executor, &self.runtime)
                            }
                        }
                        planner.plan(LogicalOperator::EMPTY, bind_context, sink)?
                    }
                    root => {
                        let timer = Timer::<R::Instant>::start();
                        let pipelines = planner.plan(root, bind_context, sink)?;
                        profile.plan_intermediate_step = Some(timer.stop());
                        pipelines
                    }
                };

                Ok(IntermediatePortal {
                    query_id,
                    execution_mode: ExecutionMode::LocalOnly,
                    query_graph,
                    output_schema: schema,
                })
            }
        }
    }

    /// Executes the pipelines in the given portal.
    ///
    /// This will go through the final phase of planning (producing executable
    /// pipelines) then spawn those on the executor.
    pub async fn execute(&mut self, portal_name: &str) -> Result<QueryResult> {
        let portal = self
            .portals
            .remove(portal_name)
            .ok_or_else(|| RayexecError::new(format!("Missing portal: '{portal_name}'")))?;

        if portal.execution_mode == ExecutionMode::Hybrid {
            // Need to begin execution on the remote side.
            let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
            hybrid_client.remote_execute(portal.query_id).await?;
        }

        let props = ExecutionProperties {
            batch_size: self.config.batch_size as usize,
        };
        let partitions = self.config.partitions as usize;
        let pipelines = portal
            .executable_graph
            .create_partition_pipelines(props, partitions)?;

        let handle = self
            .executor
            .spawn_pipelines(pipelines, Arc::new(portal.result_stream.error_sink()));

        let output = Output::Stream(portal.result_stream);

        Ok(QueryResult {
            handle,
            output,
            output_schema: portal.output_schema,
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

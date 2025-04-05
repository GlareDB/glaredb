use std::sync::Arc;

use glaredb_error::{DbError, Result, not_implemented};
use glaredb_parser::parser;
use glaredb_parser::statement::RawStatement;
use hashbrown::HashMap;
use uuid::Uuid;

use super::query_result::{
    Output,
    QueryResult,
    StreamOutput,
    VerifyQueryHandle,
    VerifyStreamOutput,
};
use crate::arrays::collection::concurrent::ConcurrentColumnCollection;
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::profile::{PlanningProfile, QueryProfile};
use crate::config::execution::OperatorPlanConfig;
use crate::config::session::{DEFAULT_BATCH_SIZE, SessionConfig};
use crate::execution::operators::results::streaming::{PhysicalStreamingResults, ResultStream};
use crate::execution::operators::{ExecutionProperties, PushOperator};
use crate::execution::pipeline::ExecutablePipelineGraph;
use crate::execution::planner::{OperatorPlanner, PlannedQueryGraph};
use crate::explain::node::ExplainNode;
use crate::logical::binder::bind_statement::StatementBinder;
use crate::logical::logical_set::VariableOrAll;
use crate::logical::operator::LogicalOperator;
use crate::logical::planner::plan_statement::StatementPlanner;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::{ResolveConfig, ResolveMode, ResolvedStatement, Resolver};
use crate::optimizer::Optimizer;
use crate::runtime::pipeline::PipelineRuntime;
use crate::runtime::system::SystemRuntime;
use crate::runtime::time::Timer;

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
pub struct Session<P: PipelineRuntime, R: SystemRuntime> {
    /// Context containing everything in the "database" that's visible to this
    /// session.
    context: DatabaseContext,
    /// Variables for this session.
    config: SessionConfig,
    /// Runtime for accessing external resources like the filesystem or http
    /// clients.
    runtime: R,
    /// Pipeline executor.
    executor: P,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Portals for statements ready to be executed.
    portals: HashMap<String, ExecutablePortal>,
}

#[derive(Debug, Clone)]
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

/// Intermediate struct hold on to pipelines that still need to be planned for
/// execution.
#[derive(Debug)]
struct IntermediatePortal {
    /// ID for the query.
    query_id: Uuid,
    /// Execution mode to use for the query.
    execution_mode: ExecutionMode,
    /// The query graph.
    query_graph: PlannedQueryGraph,
    /// Output schema for the query.
    output_schema: ColumnSchema,
    /// If the query was optimized.
    optimized: bool,
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
    output_schema: ColumnSchema,
    /// Output result stream.
    result_stream: ResultStream,
    /// Profile data we've collected during resolving/binding/planning.
    profile: PlanningProfile,
    /// If the query was optimized.
    optimized: bool,
    /// Optional verification state.
    verification: Option<VerificationState>,
}

/// State for query verification.
#[derive(Debug)]
struct VerificationState {
    /// Output result stream.
    result_stream: ResultStream,
    /// Pipelines we'll be executing on this node.
    executable_graph: ExecutablePipelineGraph,
    /// Output schema of the query, this should be the same as the output schema
    /// of the query that's being verified against this one.
    output_schema: ColumnSchema,
}

impl<P, R> Session<P, R>
where
    P: PipelineRuntime,
    R: SystemRuntime,
{
    pub fn new(context: DatabaseContext, executor: P, runtime: R) -> Self {
        let config = SessionConfig::new(&executor, &runtime);

        Session {
            context,
            runtime,
            executor,
            config,
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
        self.prepared
            .insert(prepared_name.into(), PreparedStatement { statement: stmt });
        Ok(())
    }

    fn get_prepared_by_name(&self, name: &str) -> Result<&PreparedStatement> {
        self.prepared
            .get(name)
            .ok_or_else(|| DbError::new(format!("Missing named prepared statement: '{name}'")))
    }

    /// Gets a prepared statement by name and generates intermedidate executable
    /// pipelines that get placed into a portal.
    pub async fn bind(
        &mut self,
        prepared_name: &str,
        portal_name: impl Into<String>,
    ) -> Result<()> {
        let stmt = self.get_prepared_by_name(prepared_name)?;
        let is_query = matches!(stmt.statement, RawStatement::Query(_));

        let profile = PlanningProfile::default();
        let mut portal = self.bind_prepared(profile, stmt.clone()).await?;

        // If we're verifying the query, go ahead and plan it with optimization
        // disabled, and attach to this portal.
        if self.config.verify_optimized_plan && is_query {
            let orig_optimizer = self.config.enable_optimizer;
            self.config.enable_optimizer = false;

            let stmt = self.get_prepared_by_name(prepared_name)?;

            // Note we don't return the error here, we need to reset the config
            // before doing so.
            //
            // TODO: We should be able to pass in a materializing sink here
            // instead of getting a result stream.
            let result = self
                .bind_prepared(PlanningProfile::default(), stmt.clone())
                .await;
            self.config.enable_optimizer = orig_optimizer;

            let verify_portal = result?;
            debug_assert!(
                !verify_portal.optimized,
                "Verification query unexpectedly optimized"
            );

            portal.verification = Some(VerificationState {
                result_stream: verify_portal.result_stream,
                executable_graph: verify_portal.executable_graph,
                output_schema: verify_portal.output_schema,
            });
        }

        self.portals.insert(portal_name.into(), portal);

        Ok(())
    }

    async fn bind_prepared(
        &mut self,
        mut profile: PlanningProfile,
        stmt: PreparedStatement,
    ) -> Result<ExecutablePortal> {
        let resolve_mode = ResolveMode::Normal;
        let timer = Timer::<R::Instant>::start();
        let (resolved_stmt, resolve_context) = Resolver::new(
            resolve_mode,
            &self.context,
            &self.runtime,
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
            ExecutionProperties {
                batch_size: self.config.batch_size as usize,
            },
            intermediate_portal.query_graph,
        )?;
        profile.plan_executable_step = Some(timer.stop());

        Ok(ExecutablePortal {
            query_id: intermediate_portal.query_id,
            execution_mode: intermediate_portal.execution_mode,
            executable_graph: pipeline_graph,
            output_schema: intermediate_portal.output_schema,
            result_stream: stream,
            profile,
            optimized: intermediate_portal.optimized,
            verification: None,
        })
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
        profile: &mut PlanningProfile,
        sink: O,
    ) -> Result<IntermediatePortal>
    where
        O: PushOperator,
    {
        match resolve_mode {
            ResolveMode::Hybrid if resolve_context.any_unresolved() => {
                not_implemented!("hybrid resolve mode")
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

                let mut optimized = false;
                if self.config.enable_optimizer {
                    let mut optimizer = Optimizer::new();
                    logical = optimizer.optimize::<R::Instant>(&mut bind_context, logical)?;
                    profile.plan_optimize_step = Some(optimizer.profile_data);
                    optimized = true;
                }

                // If we're an explain, put a copy of the optimized plan on the
                // node.
                if let LogicalOperator::Explain(explain) = &mut logical {
                    let child = explain
                        .children
                        .first()
                        .ok_or_else(|| DbError::new("Missing explain child"))?;

                    explain.node.logical_optimized = Some(ExplainNode::new_from_logical_plan(
                        &bind_context,
                        explain.node.verbose,
                        child,
                    ));
                }

                let schema = ColumnSchema::new(
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
                        per_partition_counts: self.config.per_partition_counts,
                    },
                    query_id,
                );

                let query_graph = match logical {
                    LogicalOperator::AttachDatabase(_) => {
                        not_implemented!("Attach database")
                        // self.handle_attach_database(attach).await?;
                        // planner.plan(LogicalOperator::EMPTY, bind_context, sink)?
                    }
                    LogicalOperator::DetachDatabase(_) => {
                        not_implemented!("Detach database")
                        // let empty = planner.plan(LogicalOperator::EMPTY, bind_context, sink)?; // Here to avoid lifetime issues.
                        // self.context.detach_database(&detach.as_ref().name)?;
                        // empty
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
                        planner.plan(
                            LogicalOperator::SINGLE_ROW,
                            &self.context,
                            bind_context,
                            sink,
                        )?
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
                        planner.plan(
                            LogicalOperator::SINGLE_ROW,
                            &self.context,
                            bind_context,
                            sink,
                        )?
                    }
                    root => {
                        let timer = Timer::<R::Instant>::start();
                        let pipelines = planner.plan(root, &self.context, bind_context, sink)?;
                        profile.plan_physical_step = Some(timer.stop());
                        pipelines
                    }
                };

                Ok(IntermediatePortal {
                    query_id,
                    execution_mode: ExecutionMode::LocalOnly,
                    query_graph,
                    output_schema: schema,
                    optimized,
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
            .ok_or_else(|| DbError::new(format!("Missing portal: '{portal_name}'")))?;

        if portal.execution_mode == ExecutionMode::Hybrid {
            not_implemented!("Hybrid exec")
            // // Need to begin execution on the remote side.
            // let hybrid_client = self.hybrid_client.clone().required("hybrid_client")?;
            // hybrid_client.remote_execute(portal.query_id).await?;
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

        let profile = QueryProfile {
            id: portal.query_id,
            plan: Some(portal.profile),
            execution: None,
        };

        let output = match portal.verification {
            Some(verification) => {
                // Set up execution for the verification query.
                if verification.output_schema != portal.output_schema {
                    return Err(DbError::new("Queries have different output schemas")
                        .with_field("left", format!("{:?}", verification.output_schema))
                        .with_field("right", format!("{:?}", portal.output_schema)));
                }

                let left_collection = Arc::new(ConcurrentColumnCollection::new(
                    portal
                        .output_schema
                        .fields
                        .iter()
                        .map(|f| f.datatype.clone()),
                    4,
                    DEFAULT_BATCH_SIZE,
                ));
                let right_collection = Arc::new(ConcurrentColumnCollection::new(
                    portal
                        .output_schema
                        .fields
                        .iter()
                        .map(|f| f.datatype.clone()),
                    4,
                    DEFAULT_BATCH_SIZE,
                ));

                let left_pipelines = verification
                    .executable_graph
                    .create_partition_pipelines(props, partitions)?;

                let left_handle = self.executor.spawn_pipelines(
                    left_pipelines,
                    Arc::new(verification.result_stream.error_sink()),
                );

                Output::VerifyStream(VerifyStreamOutput {
                    left_stream: verification.result_stream,
                    left_collection,

                    right_profile: Some(profile),
                    right_profiles: self.context.profiles().clone(),
                    right_stream: portal.result_stream,
                    right_collection,

                    handle: Arc::new(VerifyQueryHandle {
                        left: left_handle,
                        right: handle,
                    }),
                })
            }
            None => {
                // No verification, just a normal stream.
                Output::Stream(StreamOutput::new(
                    portal.result_stream,
                    profile,
                    handle,
                    self.context.profiles().clone(),
                ))
            }
        };

        Ok(QueryResult {
            query_id: portal.query_id,
            output,
            output_schema: portal.output_schema,
        })
    }
}

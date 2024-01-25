use std::collections::VecDeque;
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::context::local::{LocalSessionContext, Portal, PreparedStatement};
use crate::environment::EnvironmentReader;
use crate::errors::{ExecError, Result};
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;
use crate::planner::physical_plan::{
    get_count_from_batch, get_operation_from_batch, GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA,
    GENERIC_OPERATION_PHYSICAL_SCHEMA,
};
use crate::planner::session_planner::SessionPlanner;
use crate::remote::client::RemoteClient;
use crate::remote::planner::{DDLExtensionPlanner, RemotePhysicalPlanner};

use catalog::mutator::CatalogMutator;
use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{
    execute_stream, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use datafusion_ext::metrics::AggregatedMetrics;
use datafusion_ext::session_metrics::{
    BatchStreamWithMetricSender, ExecutionStatus, QueryMetrics, SessionMetricsHandler,
};
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use distexec::scheduler::{OutputSink, Scheduler};
use distexec::stream::create_coalescing_adapter;
use futures::{Stream, StreamExt};
use once_cell::sync::Lazy;
use pgrepr::format::Format;
use pgrepr::notice::{Notice, NoticeSeverity, SqlState};
use telemetry::Tracker;
use uuid::Uuid;

static EMPTY_EXEC_PLAN: Lazy<Arc<dyn ExecutionPlan>> = Lazy::new(|| {
    Arc::new(EmptyExec::new(
        /* produce_one_row = */ false,
        Arc::new(Schema::empty()),
    ))
});

/// Results from a sql statement execution.
pub enum ExecutionResult {
    /// The stream for the output of a query.
    Query {
        /// Inner results stream from execution.
        stream: SendableRecordBatchStream,
    },
    /// Execution errored.
    Error(DataFusionError),
    /// No batches returned.
    EmptyQuery,
    /// Transaction started.
    Begin,
    /// Transaction committed,
    Commit,
    /// Transaction rolled back.
    Rollback,
    /// Data successfully inserted.
    InsertSuccess { rows_inserted: usize },
    /// Data successfully deleted.
    DeleteSuccess { deleted_rows: usize },
    /// Data successfully updated.
    UpdateSuccess { updated_rows: usize },
    /// Data successfully copied.
    CopySuccess,
    /// Table created.
    CreateTable,
    /// Database created.
    CreateDatabase,
    /// Tunnel created.
    CreateTunnel,
    /// Credentials created.
    CreateCredential,
    /// Credentials created.
    CreateCredentials,
    /// Schema created.
    CreateSchema,
    /// A view was created.
    CreateView,
    /// A table was renamed.
    AlterTable,
    /// A database was renamed.
    AlterDatabase,
    /// A tunnel was altered.
    AlterTunnelRotateKeys,
    /// A client local variable was set.
    Set,
    /// Tables dropped.
    DropTables,
    /// Views dropped.
    DropViews,
    /// Schemas dropped.
    DropSchemas,
    /// Database dropped.
    DropDatabase,
    /// Tunnel is dropped.
    DropTunnel,
    /// Credentials are dropped.
    DropCredentials,
}
// this just makes the `prepare_statement` method a bit more ergonomic.
pub struct PrepareStatementArg {
    stmt: Option<StatementWithExtensions>,
}

impl<'a> TryFrom<&'a str> for PrepareStatementArg {
    type Error = ExecError;
    fn try_from(query: &'a str) -> Result<Self> {
        let mut statements = crate::parser::parse_sql(query)?;
        match statements.len() {
            0 => Err(ExecError::String("No statements in query".to_string())),
            1 => Ok(PrepareStatementArg {
                stmt: statements.pop_front(),
            }),
            _ => Err(ExecError::String(
                "More than one statement in query".to_string(),
            )),
        }
    }
}

impl<'a> TryFrom<&'a String> for PrepareStatementArg {
    type Error = ExecError;
    fn try_from(query: &'a String) -> Result<Self> {
        let s: &str = query;
        s.try_into()
    }
}

impl TryFrom<Option<StatementWithExtensions>> for PrepareStatementArg {
    type Error = ExecError;
    fn try_from(stmt: Option<StatementWithExtensions>) -> Result<Self> {
        Ok(PrepareStatementArg { stmt })
    }
}
impl TryFrom<StatementWithExtensions> for PrepareStatementArg {
    type Error = ExecError;
    fn try_from(stmt: StatementWithExtensions) -> Result<Self> {
        Ok(PrepareStatementArg { stmt: Some(stmt) })
    }
}

impl ExecutionResult {
    /// Create a result from a stream and a physical plan.
    ///
    /// This will look at the first batch in the stream to determine which
    /// result it is.
    pub async fn from_stream(mut stream: SendableRecordBatchStream) -> ExecutionResult {
        let schema = stream.schema();
        // If we don't match either of these schemas, just assume these results
        // are from a normal SELECT query.
        if !(schema.eq(&GENERIC_OPERATION_PHYSICAL_SCHEMA)
            || schema.eq(&GENERIC_OPERATION_AND_COUNT_PHYSICAL_SCHEMA))
        {
            return ExecutionResult::Query { stream };
        }

        let (batch, stream) = match stream.next().await {
            Some(Ok(batch)) => (
                batch.clone(),
                StreamAndFirstResult {
                    stream,
                    first_result: Some(Ok(batch)),
                },
            ),
            Some(Err(e)) => {
                return ExecutionResult::Error(e);
            }
            None => return ExecutionResult::EmptyQuery,
        };

        // Our special batches contain only a single row. If we have more,
        // assume that this is a user query (which would be weird since it
        // matches our special schemas).
        if batch.num_rows() != 1 {
            return ExecutionResult::Query {
                stream: Box::pin(stream),
            };
        }

        // Try to get the execution result type from the batch. Default to
        // `Query` if we don't know how to translate it into a result.
        let op = get_operation_from_batch(&batch).unwrap_or_default();
        let count = get_count_from_batch(&batch);

        ExecutionResult::from_str_and_count(&op, count).unwrap_or(ExecutionResult::Query {
            stream: Box::pin(stream),
        })
    }

    const fn result_type_str(&self) -> &'static str {
        match self {
            ExecutionResult::Error(_) => "error",
            ExecutionResult::Query { .. } => "query",
            ExecutionResult::EmptyQuery => "empty_query",
            ExecutionResult::Begin => "begin",
            ExecutionResult::Commit => "commit",
            ExecutionResult::Rollback => "rollback",
            ExecutionResult::InsertSuccess { .. } => "insert",
            ExecutionResult::DeleteSuccess { .. } => "delete",
            ExecutionResult::UpdateSuccess { .. } => "update",
            ExecutionResult::CopySuccess => "copy",
            ExecutionResult::CreateTable => "create_table",
            ExecutionResult::CreateDatabase => "create_database",
            ExecutionResult::CreateTunnel => "create_tunnel",
            ExecutionResult::CreateCredential => "create_credential",
            ExecutionResult::CreateCredentials => "create_credentials",
            ExecutionResult::CreateSchema => "create_schema",
            ExecutionResult::CreateView => "create_view",
            ExecutionResult::AlterTable => "alter_table",
            ExecutionResult::AlterDatabase => "alter_database",
            ExecutionResult::AlterTunnelRotateKeys => "alter_tunnel_rotate_keys",
            ExecutionResult::Set => "set_local",
            ExecutionResult::DropTables => "drop_tables",
            ExecutionResult::DropViews => "drop_views",
            ExecutionResult::DropSchemas => "drop_schemas",
            ExecutionResult::DropDatabase => "drop_database",
            ExecutionResult::DropTunnel => "drop_tunnel",
            ExecutionResult::DropCredentials => "drop_credentials",
        }
    }

    const fn is_ddl(&self) -> bool {
        matches!(
            self,
            ExecutionResult::CreateTable
                | ExecutionResult::CreateDatabase
                | ExecutionResult::CreateTunnel
                | ExecutionResult::CreateCredential
                | ExecutionResult::CreateCredentials
                | ExecutionResult::CreateSchema
                | ExecutionResult::CreateView
                | ExecutionResult::AlterTable
                | ExecutionResult::AlterDatabase
                | ExecutionResult::AlterTunnelRotateKeys
                | ExecutionResult::DropTables
                | ExecutionResult::DropViews
                | ExecutionResult::DropSchemas
                | ExecutionResult::DropDatabase
                | ExecutionResult::DropTunnel
                | ExecutionResult::DropCredentials
        )
    }

    const fn is_error(&self) -> bool {
        matches!(self, ExecutionResult::Error(_))
    }

    fn from_str_and_count(s: &str, count: Option<u64>) -> Option<ExecutionResult> {
        Some(match s {
            "begin" => ExecutionResult::Begin,
            "commit" => ExecutionResult::Commit,
            "rollback" => ExecutionResult::Rollback,
            "insert" => ExecutionResult::InsertSuccess {
                rows_inserted: count.unwrap_or_default() as usize,
            },
            "delete" => ExecutionResult::DeleteSuccess {
                deleted_rows: count.unwrap_or_default() as usize,
            },
            "update" => ExecutionResult::UpdateSuccess {
                updated_rows: count.unwrap_or_default() as usize,
            },
            "copy" => ExecutionResult::CopySuccess,
            "create_table" => ExecutionResult::CreateTable,
            "create_database" => ExecutionResult::CreateDatabase,
            "create_tunnel" => ExecutionResult::CreateTunnel,
            "create_credential" => ExecutionResult::CreateCredential,
            "create_credentials" => ExecutionResult::CreateCredentials,
            "create_schema" => ExecutionResult::CreateSchema,
            "create_view" => ExecutionResult::CreateView,
            "alter_table" => ExecutionResult::AlterTable,
            "alter_database" => ExecutionResult::AlterDatabase,
            "alter_tunnel_rotate_keys" => ExecutionResult::AlterTunnelRotateKeys,
            "set" => ExecutionResult::Set,
            "drop_tables" => ExecutionResult::DropTables,
            "drop_views" => ExecutionResult::DropViews,
            "drop_schemas" => ExecutionResult::DropSchemas,
            "drop_database" => ExecutionResult::DropDatabase,
            "drop_tunnel" => ExecutionResult::DropTunnel,
            "drop_credentials" => ExecutionResult::DropCredentials,
            _ => return None,
        })
    }
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Error(e) => {
                write!(f, "Execution error: {e}")
            }
            ExecutionResult::Query { .. } => {
                write!(f, "Query")
            }
            ExecutionResult::EmptyQuery => write!(f, "No results"),
            ExecutionResult::Begin => write!(f, "Begin"),
            ExecutionResult::Commit => write!(f, "Commit"),
            ExecutionResult::Rollback => write!(f, "Rollback"),
            ExecutionResult::InsertSuccess { rows_inserted, .. } => {
                if *rows_inserted == 1 {
                    write!(f, "Inserted 1 row")
                } else {
                    write!(f, "Inserted {} rows", rows_inserted)
                }
            }
            ExecutionResult::DeleteSuccess { deleted_rows } => {
                if *deleted_rows == 1 {
                    write!(f, "Deleted 1 row")
                } else {
                    write!(f, "Deleted {} rows", deleted_rows)
                }
            }
            ExecutionResult::UpdateSuccess { updated_rows } => {
                if *updated_rows == 1 {
                    write!(f, "Updated 1 row")
                } else {
                    write!(f, "Updated {} rows", updated_rows)
                }
            }
            ExecutionResult::CopySuccess => write!(f, "Copy success"),
            ExecutionResult::CreateTable => write!(f, "Table created"),
            ExecutionResult::CreateDatabase => write!(f, "Database created"),
            ExecutionResult::CreateTunnel => write!(f, "Tunnel created"),
            ExecutionResult::CreateCredential => write!(f, "Credential created"),
            ExecutionResult::CreateCredentials => write!(f, "Credentials created\nDEPRECATION WARNING. `CREATE CREDENTIALS` is deprecated and will be removed in a future release. Please use `CREATE CREDENTIAL` instead."),
            ExecutionResult::CreateSchema => write!(f, "Schema create"),
            ExecutionResult::CreateView => write!(f, "View created"),
            ExecutionResult::AlterTable => write!(f, "Table altered"),
            ExecutionResult::AlterDatabase => write!(f, "Database altered"),
            ExecutionResult::AlterTunnelRotateKeys => write!(f, "Keys rotated"),
            ExecutionResult::Set => write!(f, "Local variable set"),
            ExecutionResult::DropTables => write!(f, "Table(s) dropped"),
            ExecutionResult::DropViews => write!(f, "View(s) dropped"),
            ExecutionResult::DropSchemas => write!(f, "Schema(s) dropped"),
            ExecutionResult::DropDatabase => write!(f, "Database(s) dropped"),
            ExecutionResult::DropTunnel => write!(f, "Tunnel(s) dropped"),
            ExecutionResult::DropCredentials => write!(f, "Credentials dropped"),
        }
    }
}

/// Simple stream adapter to use after we've inspected the first batch in a
/// stream.
struct StreamAndFirstResult {
    stream: SendableRecordBatchStream,
    first_result: Option<DataFusionResult<RecordBatch>>,
}

impl Stream for StreamAndFirstResult {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(batch) = self.first_result.take() {
            return Poll::Ready(Some(batch));
        }
        self.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for StreamAndFirstResult {
    fn schema(&self) -> Arc<Schema> {
        self.stream.schema()
    }
}

/// A per-client user session.
///
/// This is a thin wrapper around a session context. Having a layer between
/// pgsrv and actual execution against the catalog allows for easy extensibility
/// in the future (e.g. consensus).
pub struct Session {
    pub(crate) ctx: LocalSessionContext,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        tracker: Arc<Tracker>,
        spill_path: Option<PathBuf>,
        task_scheduler: Scheduler,
    ) -> Result<Session> {
        let metrics_handler = SessionMetricsHandler::new(
            vars.user_id(),
            vars.database_id(),
            vars.connection_id(),
            tracker,
        );

        let ctx = LocalSessionContext::new(
            vars,
            catalog,
            catalog_mutator,
            native_tables,
            metrics_handler,
            spill_path,
            task_scheduler,
        )?;

        Ok(Session { ctx })
    }

    pub async fn attach_remote_session(
        &mut self,
        client: RemoteClient,
        test_db_id: Option<Uuid>,
    ) -> Result<()> {
        self.ctx.attach_remote_session(client, test_db_id).await
    }

    pub fn get_session_catalog(&self) -> &SessionCatalog {
        self.ctx.get_session_catalog()
    }

    pub fn register_env_reader(&mut self, env_reader: Box<dyn EnvironmentReader>) {
        self.ctx.register_env_reader(env_reader);
    }

    /// Return the DF session context.
    pub fn df_ctx(&self) -> &datafusion::prelude::SessionContext {
        self.ctx.df_ctx()
    }

    /// Create a physical plan for a given datafusion logical plan.
    pub async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
        op: &OperationInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = self.ctx.df_ctx().state();
        let plan = state.optimize(&plan)?;
        if let Some(client) = self.ctx.exec_client() {
            let planner = RemotePhysicalPlanner {
                database_id: self.ctx.get_database_id(),
                query_text: op.query_text(),
                remote_client: client,
                catalog: self.ctx.get_session_catalog(),
            };
            let plan = planner.create_physical_plan(&plan, &state).await?;
            Ok(plan)
        } else {
            // TODO: Possible to not require a catalog clone here?
            let ddl_planner = DDLExtensionPlanner::new(self.ctx.get_session_catalog().clone());
            let planner =
                DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ddl_planner)]);
            let plan = planner.create_physical_plan(&plan, &state).await?;

            Ok(plan)
        }
    }

    /// Execute a datafusion physical plan.
    pub async fn execute_physical_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.ctx.task_context();
        let stream = if self.ctx.get_session_vars().enable_experimental_scheduler() {
            let scheduler = self.ctx.get_task_scheduler();
            let (sink, stream) =
                create_coalescing_adapter(plan.output_partitioning(), plan.schema());
            let sink = Arc::new(sink);

            let output = OutputSink {
                batches: sink.clone(),
                errors: sink,
            };

            scheduler.schedule(plan, context, output)?;
            Box::pin(stream)
        } else {
            execute_stream(plan, context)?
        };

        Ok(stream)
    }

    pub fn get_session_vars(&self) -> SessionVars {
        self.ctx.get_session_vars().clone()
    }

    /// Prepare a parsed statement for future execution.
    pub async fn prepare_statement<T: TryInto<PrepareStatementArg, Error = ExecError>>(
        &mut self,
        name: String,
        stmt: T,
        params: Vec<i32>, // OIDs
    ) -> Result<()> {
        let stmt: PrepareStatementArg = stmt.try_into()?;

        self.ctx.prepare_statement(name, stmt.stmt, params).await
    }

    /// Like 'prepare_statement', but for a portal.
    pub async fn prepare_portal(&mut self, portal_id: &str, query: &str) -> Result<()> {
        self.prepare_statement(portal_id.to_string(), query, Vec::new())
            .await?;
        let prepared = self.get_prepared_statement(portal_id)?;

        let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
        self.bind_statement(
            portal_id.to_string(),
            portal_id,
            Vec::new(),
            vec![Format::Text; num_fields],
        )?;
        Ok(())
    }

    pub fn get_prepared_statement(&self, name: &str) -> Result<&PreparedStatement> {
        self.ctx.get_prepared_statement(name)
    }

    pub fn get_portal(&self, name: &str) -> Result<&Portal> {
        self.ctx.get_portal(name)
    }

    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.ctx.remove_prepared_statement(name);
    }

    pub fn remove_portal(&mut self, name: &str) {
        self.ctx.remove_portal(name);
    }

    pub fn take_notices(&mut self) -> Vec<Notice> {
        self.ctx.take_notices()
    }

    /// Bind the parameters of a prepared statement to the given values.
    ///
    /// If successful, the bound statement will create a portal which can be
    /// used to execute the statement.
    pub fn bind_statement(
        &mut self,
        portal_name: String,
        stmt_name: &str,
        params: Vec<ScalarValue>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        self.ctx
            .bind_statement(portal_name, stmt_name, params, result_formats)
    }

    /// Execute a logical plan.
    pub async fn execute_logical_plan(
        &mut self,
        plan: LogicalPlan,
        op: &OperationInfo,
    ) -> Result<(Arc<dyn ExecutionPlan>, ExecutionResult)> {
        // Note that transaction support is fake, in that we don't currently do
        // anything and do not provide any transactional semantics.
        //
        // We stub out transaction commands since many tools (even BI ones) will
        // try to open a transaction for some queries.
        match plan {
            LogicalPlan::Noop => Ok((EMPTY_EXEC_PLAN.clone(), ExecutionResult::EmptyQuery)),
            LogicalPlan::Transaction(plan) => {
                // Push a notice to let the user know about our current
                // transaction handling.
                self.ctx.push_notice(Notice{
                    severity: NoticeSeverity::Warning,
                    code: SqlState::FeatureNotSupported,
                    message: "GlareDB does not support proper transactional semantics. Do not rely on transactions for correctness. Transactions are stubbed out to enable compatability with existing Postgres tools.".to_string(),
                });

                Ok((
                    EMPTY_EXEC_PLAN.clone(),
                    match plan {
                        TransactionPlan::Begin => ExecutionResult::Begin,
                        TransactionPlan::Commit => ExecutionResult::Commit,
                        TransactionPlan::Abort => ExecutionResult::Rollback,
                    },
                ))
            }
            LogicalPlan::Datafusion(plan) => {
                let physical = self.create_physical_plan(plan, op).await?;
                let stream = self.execute_physical_plan(physical.clone()).await?;

                let stream = ExecutionResult::from_stream(stream).await;

                // If we're attached to a remote node, and the result indicates
                // the operation was a DDL operation, then fetch the newer
                // catalog from the remote node.
                if let Some(mut client) = self.ctx.exec_client() {
                    // Note that 'is error' check tries to cover the case where
                    // the local client tries to query a table that's been
                    // changed by a second client (e.g. rename). This check aims
                    // to make sure we get the latest catalog so that the user
                    // isn't stuck (the user tries to query using the name table
                    // name, but the catalog is out of date and doesn't know
                    // about it).
                    //
                    // This check is overly broad in that we'll try to get the
                    // catalog on every error. We can look into adding more
                    // detail on the grpc response stream from the remote node
                    // to provide a better hint of what we should be doing on
                    // error.
                    if stream.is_ddl() || stream.is_error() {
                        // TODO: Instead of swapping here, I'd like to if we
                        // could go towards collecting a "diff" of a session
                        // (including new catalog states, variable changes, etc)
                        // and applying at the start of a new query. This would
                        // make "rolling back" in the case of an error pretty
                        // easy -- just drop the diff.
                        let state = client.fetch_catalog().await?;
                        self.ctx
                            .get_session_catalog_mut()
                            .swap_state(Arc::new(state));
                    }
                }

                Ok((physical, stream))
            }
        }
    }

    /// Execute a portal.
    ///
    /// This will handle metrics tracking for query executions.
    // TODO: Handle max rows.
    pub async fn execute_portal(
        &mut self,
        portal_name: &str,
        _max_rows: i32,
    ) -> Result<ExecutionResult> {
        let portal = self.ctx.get_portal(portal_name)?;

        let plan = match &portal.stmt.plan {
            Some(plan) => plan.clone(),
            None => return Ok(ExecutionResult::EmptyQuery),
        };

        let mut op = OperationInfo::default();
        if let Some(stmt) = &portal.stmt.stmt {
            op = op.with_query_text(stmt.to_string());
        }

        // Create "base" metrics.
        let mut metrics = QueryMetrics {
            query_text: op.query_text().to_owned(),
            ..Default::default()
        };

        let stream = match self.execute_logical_plan(plan, &op).await {
            Ok((plan, result)) => match result {
                ExecutionResult::Error(e) => {
                    metrics.execution_status = ExecutionStatus::Fail;
                    metrics.error_message = Some(e.to_string());
                    self.ctx.get_metrics_handler().push_metric(metrics);
                    return Err(e.into());
                }
                result => {
                    metrics.execution_status = ExecutionStatus::Success;
                    metrics.result_type = result.result_type_str();

                    match result {
                        ExecutionResult::Query { stream } => {
                            // Swap out the batch stream with one that will send
                            // metrics at the completions of the stream.
                            ExecutionResult::Query {
                                stream: Box::pin(BatchStreamWithMetricSender::new(
                                    stream,
                                    plan.clone(),
                                    metrics,
                                    self.ctx.get_metrics_handler(),
                                )),
                            }
                        }
                        write_result @ ExecutionResult::InsertSuccess { .. }
                        | write_result @ ExecutionResult::CopySuccess => {
                            // Push the metrics from the plan since the stream
                            // is already processed.
                            let agg_metrics = AggregatedMetrics::new_from_plan(plan.as_ref());
                            metrics.elapsed_compute_ns = Some(agg_metrics.elapsed_compute_ns);
                            metrics.bytes_read = Some(agg_metrics.bytes_read);
                            metrics.bytes_written = agg_metrics.bytes_written;
                            self.ctx.get_metrics_handler().push_metric(metrics);
                            write_result
                        }
                        other => other,
                    }
                }
            },
            Err(e) => {
                metrics.execution_status = ExecutionStatus::Fail;
                metrics.error_message = Some(e.to_string());

                // Ensure we push the metrics for this failed query even though
                // we're returning an error. This allows for querying for and
                // reporting failed executions.
                self.ctx.get_metrics_handler().push_metric(metrics);
                return Err(e);
            }
        };

        Ok(stream)
    }

    /// Helper for converting SQL statement to a logical plan.
    ///
    /// Errors if no statements or more than one statement is provided
    /// in the query.
    pub async fn prql_to_lp(&mut self, query: &str) -> Result<LogicalPlan> {
        let stmt = crate::parser::parse_prql(query)?;

        self.prepare_statements(stmt).await
    }

    pub async fn prepare_statements(
        &mut self,
        mut statements: VecDeque<StatementWithExtensions>,
    ) -> Result<LogicalPlan> {
        const UNNAMED: String = String::new();
        match statements.len() {
            0 => Err(ExecError::String("No statements in query".to_string())),
            1 => {
                let stmt = statements.pop_front().unwrap();
                self.prepare_statement(UNNAMED, stmt, Vec::new()).await?;
                let prepared = self.get_prepared_statement(&UNNAMED)?;
                let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
                self.bind_statement(
                    UNNAMED,
                    &UNNAMED,
                    Vec::new(),
                    vec![Format::Text; num_fields],
                )?;
                let portal = self.ctx.get_portal(&UNNAMED)?.clone();
                Ok(portal.stmt.plan.unwrap())
            }
            _ => Err(ExecError::String(
                "More than one statement in query".to_string(),
            )),
        }
    }

    /// Create a logical plan from a SQL query.
    /// if the query doesn't contain exactly one statement, an error is returned.
    pub async fn create_logical_plan(&mut self, query: &str) -> Result<LogicalPlan> {
        self.ctx.maybe_refresh_state().await?;
        let mut statements = self.parse_query(query)?;
        match statements.len() {
            0 => Err(ExecError::String("No statements in query".to_string())),
            1 => {
                let stmt = statements.pop_front().unwrap();
                let planner = SessionPlanner::new(&self.ctx);
                let plan = planner.plan_ast(stmt).await?;
                Ok(plan)
            }
            _ => Err(ExecError::String(
                "More than one statement in query".to_string(),
            )),
        }
    }

    pub fn parse_query(&self, query: &str) -> Result<VecDeque<StatementWithExtensions>> {
        match self.get_session_vars().dialect() {
            datafusion_ext::vars::Dialect::Sql => crate::parser::parse_sql(query),
            datafusion_ext::vars::Dialect::Prql => crate::parser::parse_prql(query),
        }
    }

    /// Execute a SQL query.
    /// if the query doesn't contain exactly one statement, an error is returned.
    pub async fn execute_sql(&mut self, query: &str) -> Result<SendableRecordBatchStream> {
        let plan = self.create_logical_plan(query).await?;
        let plan = plan.try_into_datafusion_plan()?;
        let plan = self
            .create_physical_plan(plan, &OperationInfo::new().with_query_text(query))
            .await?;
        let stream = self.execute_physical_plan(plan).await?;

        Ok(stream)
    }
}

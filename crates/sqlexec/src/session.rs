use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::metastore::catalog::{CatalogMutator, SessionCatalog};
use crate::planner::context_builder::PartialContextProvider;
use crate::remote::client::RemoteClient;
use crate::remote::planner::{DDLExtensionPlanner, RemotePhysicalPlanner};
use datafusion::common::OwnedTableReference;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use pgrepr::format::Format;
use telemetry::Tracker;
use uuid::Uuid;

use crate::background_jobs::JobRunner;
use crate::context::local::{LocalSessionContext, Portal, PreparedStatement};
use crate::environment::EnvironmentReader;
use crate::errors::Result;
use crate::metrics::{BatchStreamWithMetricSender, ExecutionStatus, QueryMetrics, SessionMetrics};
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;

/// Results from a sql statement execution.
pub enum ExecutionResult {
    /// The stream for the output of a query.
    Query {
        stream: SendableRecordBatchStream,
        /// Plan used to create the stream. Used for getting metrics after the
        /// stream completes.
        ///
        /// TODO: I would like to remove this. Putting the plan on the result
        /// was the easiest way of providing everything needed to construct a
        /// `BatchStreamWithMetricSender` without a bit more refactoring. This
        /// stream requires physical plan and a starting set of execution
        /// metrics, which are created separately.
        plan: Arc<dyn ExecutionPlan>,
    },
    /// Showing a variable.
    // TODO: We don't need to make a stream for this.
    ShowVariable { stream: SendableRecordBatchStream },
    /// No statement provided.
    EmptyQuery,
    /// Transaction started.
    Begin,
    /// Transaction committed,
    Commit,
    /// Transaction rolled abck.
    Rollback,
    /// Data successfully deleted.
    DeleteSuccess { deleted_rows: usize },
    /// Data successfully updated.
    UpdateSuccess { updated_rows: usize },
    /// Data successfully written.
    WriteSuccess,
    /// Data successfully copied.
    CopySuccess,
    /// Table created.
    CreateTable,
    /// Database created.
    CreateDatabase,
    /// Tunnel created.
    CreateTunnel,
    /// Credentials created.
    CreateCredentials,
    /// Schema created.
    CreateSchema,
    /// A view was created.
    CreateView,
    /// A table was renamed.
    AlterTableRename,
    /// A database was renamed.
    AlterDatabaseRename,
    /// A tunnel was altered.
    AlterTunnelRotateKeys,
    /// A client local variable was set.
    SetLocal,
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

impl ExecutionResult {
    const fn result_type_str(&self) -> &'static str {
        match self {
            ExecutionResult::Query { .. } => "query",
            ExecutionResult::ShowVariable { .. } => "show_variable",
            ExecutionResult::EmptyQuery => "empty_query",
            ExecutionResult::Begin => "begin",
            ExecutionResult::Commit => "commit",
            ExecutionResult::Rollback => "rollback",
            ExecutionResult::WriteSuccess => "write_success",
            ExecutionResult::CopySuccess => "copy_success",
            ExecutionResult::DeleteSuccess { .. } => "delete_success",
            ExecutionResult::UpdateSuccess { .. } => "update success",
            ExecutionResult::CreateTable => "create_table",
            ExecutionResult::CreateDatabase => "create_database",
            ExecutionResult::CreateTunnel => "create_tunnel",
            ExecutionResult::CreateCredentials => "create_credentials",
            ExecutionResult::CreateSchema => "create_schema",
            ExecutionResult::CreateView => "create_view",
            ExecutionResult::AlterTableRename => "alter_table_rename",
            ExecutionResult::AlterDatabaseRename => "alter_database_rename",
            ExecutionResult::AlterTunnelRotateKeys => "alter_tunnel_rotate_keys",
            ExecutionResult::SetLocal => "set_local",
            ExecutionResult::DropTables => "drop_tables",
            ExecutionResult::DropViews => "drop_views",
            ExecutionResult::DropSchemas => "drop_schemas",
            ExecutionResult::DropDatabase => "drop_database",
            ExecutionResult::DropTunnel => "drop_tunnel",
            ExecutionResult::DropCredentials => "drop_credentials",
        }
    }
}

impl fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Query { stream, .. } => {
                let fields: Vec<_> = stream
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                write!(f, "Query ({})", fields.join(", "))
            }
            ExecutionResult::ShowVariable { .. } => {
                write!(f, "Show")
            }
            ExecutionResult::EmptyQuery => write!(f, "No results"),
            ExecutionResult::Begin => write!(f, "Begin"),
            ExecutionResult::Commit => write!(f, "Commit"),
            ExecutionResult::Rollback => write!(f, "Rollback"),
            ExecutionResult::WriteSuccess => write!(f, "Write success"),
            ExecutionResult::CopySuccess => write!(f, "Copy success"),
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
            ExecutionResult::CreateTable => write!(f, "Table created"),
            ExecutionResult::CreateDatabase => write!(f, "Database created"),
            ExecutionResult::CreateTunnel => write!(f, "Tunnel created"),
            ExecutionResult::CreateCredentials => write!(f, "Credentials created"),
            ExecutionResult::CreateSchema => write!(f, "Schema create"),
            ExecutionResult::CreateView => write!(f, "View created"),
            ExecutionResult::AlterTableRename => write!(f, "Table renamed"),
            ExecutionResult::AlterDatabaseRename => write!(f, "Database renamed"),
            ExecutionResult::AlterTunnelRotateKeys => write!(f, "Keys rotated"),
            ExecutionResult::SetLocal => write!(f, "Local variable set"),
            ExecutionResult::DropTables => write!(f, "Table(s) dropped"),
            ExecutionResult::DropViews => write!(f, "View(s) dropped"),
            ExecutionResult::DropSchemas => write!(f, "Schema(s) dropped"),
            ExecutionResult::DropDatabase => write!(f, "Database(s) dropped"),
            ExecutionResult::DropTunnel => write!(f, "Tunnel(s) dropped"),
            ExecutionResult::DropCredentials => write!(f, "Credentials dropped"),
        }
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
        background_jobs: JobRunner,
    ) -> Result<Session> {
        let metrics = SessionMetrics::new(
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
            metrics,
            spill_path,
            background_jobs,
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

    pub async fn close(&mut self) -> Result<()> {
        self.ctx.close().await
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = self.ctx.df_ctx().state();
        let plan = state.optimize(&plan)?;
        if let Some(client) = self.ctx.exec_client() {
            let planner = RemotePhysicalPlanner {
                remote_client: client,
                catalog: self.ctx.get_session_catalog(),
            };
            let plan = planner.create_physical_plan(&plan, &state).await?;
            Ok(plan)
        } else {
            let ddl_planner = DDLExtensionPlanner::new(self.ctx.get_session_catalog().version());
            let planner =
                DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ddl_planner)]);
            let plan = planner.create_physical_plan(&plan, &state).await?;
            Ok(plan)
        }
    }

    /// Execute a datafusion physical plan.
    pub fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.ctx.task_context();
        let stream = execute_stream(plan, context)?;
        Ok(stream)
    }

    /// Get the session dispatcher.
    pub async fn dispatch_access(
        &self,
        table_ref: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.ctx.df_ctx().state();
        let mut ctx_provider = PartialContextProvider::new(&self.ctx, &state)?;
        Ok(ctx_provider.table_provider(table_ref).await?)
    }

    pub fn get_session_vars(&self) -> SessionVars {
        self.ctx.get_session_vars().clone()
    }

    /// Prepare a parsed statement for future execution.
    pub async fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        params: Vec<i32>, // OIDs
    ) -> Result<()> {
        // Flush any completed metrics prior to planning. This is mostly
        // beneficial when planning successive calls to the
        // `session_query_history` table since the mem table is created during
        // planning.
        //
        // In all other cases, it's correct to only need to flush immediately
        // prior to execute (which we also do).
        self.ctx.get_metrics_mut().flush_completed();

        self.ctx.prepare_statement(name, stmt, params).await
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

    pub fn is_main_instance(&self) -> bool {
        self.ctx.get_session_vars().remote_session_id().is_none()
    }

    pub async fn execute_inner(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
        // Note that transaction support is fake, in that we don't currently do
        // anything and do not provide any transactional semantics.
        //
        // We stub out transaction commands since many tools (even BI ones) will
        // try to open a transaction for some queries.
        let result = match plan {
            LogicalPlan::Transaction(plan) => match plan {
                TransactionPlan::Begin => ExecutionResult::Begin,
                TransactionPlan::Commit => ExecutionResult::Commit,
                TransactionPlan::Abort => ExecutionResult::Rollback,
            },
            LogicalPlan::Datafusion(plan) => {
                let physical = self.create_physical_plan(plan).await?;
                let stream = self.execute_physical(physical.clone())?;
                ExecutionResult::Query {
                    stream,
                    plan: physical,
                }
            }
        };

        Ok(result)
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
        // Flush any completed metrics.
        self.ctx.get_metrics_mut().flush_completed();

        let portal = self.ctx.get_portal(portal_name)?;
        let plan = match &portal.stmt.plan {
            Some(plan) => plan.clone(),
            None => return Ok(ExecutionResult::EmptyQuery),
        };

        // Create "base" metrics.
        let mut metrics = QueryMetrics::new_for_portal(portal);

        let result = self.execute_inner(plan).await;
        let result = match result {
            Ok(result) => {
                metrics.execution_status = ExecutionStatus::Success;
                metrics.result_type = result.result_type_str();
                result
            }
            Err(e) => {
                metrics.execution_status = ExecutionStatus::Fail;
                metrics.error_message = Some(e.to_string());

                // Ensure we push the metrics for this failed query even though
                // we're returning an error. This allows for querying for and
                // reporting failed executions.
                self.ctx.get_metrics_mut().push_metric(metrics);
                return Err(e);
            }
        };

        // If we're running a query, then swap out the batch stream with one
        // that will send metrics at the completions of the stream.
        match result {
            ExecutionResult::Query { stream, plan } => {
                let sender = self.ctx.get_metrics().get_sender();
                Ok(ExecutionResult::Query {
                    stream: Box::pin(BatchStreamWithMetricSender::new(
                        stream,
                        plan.clone(),
                        metrics,
                        sender,
                    )),
                    plan,
                })
            }
            result => {
                // Query not async (already completed), we're good to go ahead
                // and push this metric.
                self.ctx.get_metrics_mut().push_metric(metrics);
                Ok(result)
            }
        }
    }

    pub async fn sql_to_lp(&mut self, query: &str) -> Result<LogicalPlan> {
        const UNNAMED: String = String::new();

        let mut statements = crate::parser::parse_sql(query)?;
        match statements.len() {
            0 => todo!(),
            1 => {
                let stmt = statements.pop_front().unwrap();
                self.prepare_statement(UNNAMED, Some(stmt), Vec::new())
                    .await?;
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
            _ => todo!(),
        }
    }
}

use crate::context::{Portal, PreparedStatement, SessionContext};
use crate::engine::SessionInfo;
use crate::errors::{ExecError, Result};
use crate::metastore::SupervisorClient;
use crate::metrics::{BatchStreamWithMetricSender, ExecutionStatus, QueryMetrics, SessionMetrics};
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;
use crate::vars::SessionVars;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::{
    execute_stream, memory::MemoryStream, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use metastore::session::SessionCatalog;
use pgrepr::format::Format;
use std::fmt;
use std::sync::Arc;
use telemetry::Tracker;

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
    ShowVariable {
        stream: SendableRecordBatchStream,
    }, // TODO: We don't need to make a stream for this.
    /// No statement provided.
    EmptyQuery,
    /// Transaction started.
    Begin,
    /// Transaction committed,
    Commit,
    /// Transaction rolled abck.
    Rollback,
    /// Data successfully written.
    WriteSuccess,
    /// Table created.
    CreateTable,
    /// Database created.
    CreateDatabase,
    /// Schema created.
    CreateSchema,
    /// A view was created.
    CreateView,
    /// A connection was created.
    CreateConnection,
    AlterTableRename,
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
            ExecutionResult::CreateTable => "create_table",
            ExecutionResult::CreateDatabase => "create_database",
            ExecutionResult::CreateSchema => "create_schema",
            ExecutionResult::CreateView => "create_view",
            ExecutionResult::CreateConnection => "create_connection",
            ExecutionResult::AlterTableRename => "alter_table_rename",
            ExecutionResult::SetLocal => "set_local",
            ExecutionResult::DropTables => "drop_tables",
            ExecutionResult::DropViews => "drop_views",
            ExecutionResult::DropSchemas => "drop_schemas",
            ExecutionResult::DropDatabase => "drop_database",
        }
    }
}

impl fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Query { stream, .. } => {
                write!(f, "query (schema: {:?})", stream.schema())
            }
            ExecutionResult::ShowVariable { stream } => {
                write!(f, "show variable (schema: {:?})", stream.schema())
            }
            ExecutionResult::EmptyQuery => write!(f, "empty query"),
            ExecutionResult::Begin => write!(f, "begin"),
            ExecutionResult::Commit => write!(f, "commit"),
            ExecutionResult::Rollback => write!(f, "rollback"),
            ExecutionResult::WriteSuccess => write!(f, "write success"),
            ExecutionResult::CreateTable => write!(f, "create table"),
            ExecutionResult::CreateDatabase => write!(f, "create database"),
            ExecutionResult::CreateSchema => write!(f, "create schema"),
            ExecutionResult::CreateView => write!(f, "create view"),
            ExecutionResult::CreateConnection => write!(f, "create connection"),
            ExecutionResult::AlterTableRename => write!(f, "alter table rename"),
            ExecutionResult::SetLocal => write!(f, "set local"),
            ExecutionResult::DropTables => write!(f, "drop tables"),
            ExecutionResult::DropViews => write!(f, "drop views"),
            ExecutionResult::DropSchemas => write!(f, "drop schemas"),
            ExecutionResult::DropDatabase => write!(f, "drop database"),
        }
    }
}

/// A per-client user session.
///
/// This is a thin wrapper around a session context. Having a layer between
/// pgsrv and actual execution against the catalog allows for easy extensibility
/// in the future (e.g. consensus).
pub struct Session {
    pub(crate) ctx: SessionContext,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    pub fn new(
        info: Arc<SessionInfo>,
        catalog: SessionCatalog,
        metastore: SupervisorClient,
        tracker: Arc<Tracker>,
    ) -> Result<Session> {
        let metrics = SessionMetrics::new(info.clone(), tracker);
        let ctx = SessionContext::new(info, catalog, metastore, metrics);
        Ok(Session { ctx })
    }

    /// Create a physical plan for a given datafusion logical plan.
    pub(crate) async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.ctx.get_df_state().create_physical_plan(&plan).await?;
        Ok(plan)
    }

    /// Execute a datafusion physical plan.
    pub(crate) fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.ctx.task_context();
        let stream = execute_stream(plan, context)?;
        Ok(stream)
    }

    pub(crate) async fn create_table(&self, _plan: CreateTable) -> Result<()> {
        // Disable creating table temporarily since we actually can't insert
        // anything into tables yet.
        Err(ExecError::UnsupportedFeature("CREATE TABLE ..."))
    }

    pub(crate) async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        self.ctx.create_external_table(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_table_as(&self, _plan: CreateTableAs) -> Result<()> {
        Err(ExecError::UnsupportedFeature("CREATE TABLE ... AS ..."))
    }

    pub(crate) async fn create_schema(&mut self, plan: CreateSchema) -> Result<()> {
        self.ctx.create_schema(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_database(&mut self, plan: CreateExternalDatabase) -> Result<()> {
        self.ctx.create_external_database(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        self.ctx.create_view(plan).await?;
        Ok(())
    }

    pub(crate) async fn alter_table_rename(&mut self, plan: AlterTableRename) -> Result<()> {
        self.ctx.alter_table_rename(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        self.ctx.drop_tables(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_views(&mut self, plan: DropViews) -> Result<()> {
        self.ctx.drop_views(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_schemas(&mut self, plan: DropSchemas) -> Result<()> {
        self.ctx.drop_schemas(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_database(&mut self, plan: DropDatabase) -> Result<()> {
        self.ctx.drop_database(plan).await?;
        Ok(())
    }

    pub(crate) async fn insert(&self, _plan: Insert) -> Result<()> {
        Err(ExecError::UnsupportedFeature("INSERT"))
    }

    pub(crate) fn set_variable(&mut self, plan: SetVariable) -> Result<()> {
        let key = plan.variable.to_string().to_lowercase();
        self.ctx
            .get_session_vars_mut()
            .set(&key, plan.try_into_string()?.as_str())?;
        Ok(())
    }

    pub(crate) fn show_variable(&self, plan: ShowVariable) -> Result<SendableRecordBatchStream> {
        let var = self.ctx.get_session_vars().get(&plan.variable)?;
        let batch = var.record_batch();
        let schema = batch.schema();
        // Creating this stream should never error.
        let stream = MemoryStream::try_new(vec![batch], schema, None).unwrap();
        Ok(Box::pin(stream))
    }

    pub fn get_session_vars(&self) -> &SessionVars {
        self.ctx.get_session_vars()
    }

    pub fn get_session_vars_mut(&mut self) -> &mut SessionVars {
        self.ctx.get_session_vars_mut()
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

    async fn execute_inner(&mut self, plan: LogicalPlan) -> Result<ExecutionResult> {
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
            LogicalPlan::Ddl(DdlPlan::CreateTable(plan)) => {
                self.create_table(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)) => {
                self.create_external_table(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateExternalDatabase(plan)) => {
                self.create_database(plan).await?;
                ExecutionResult::CreateDatabase
            }
            LogicalPlan::Ddl(DdlPlan::CreateTableAs(plan)) => {
                self.create_table_as(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateSchema(plan)) => {
                self.create_schema(plan).await?;
                ExecutionResult::CreateSchema
            }
            LogicalPlan::Ddl(DdlPlan::CreateView(plan)) => {
                self.create_view(plan).await?;
                ExecutionResult::CreateView
            }
            LogicalPlan::Ddl(DdlPlan::AlterTableRaname(plan)) => {
                self.alter_table_rename(plan).await?;
                ExecutionResult::AlterTableRename
            }
            LogicalPlan::Ddl(DdlPlan::DropTables(plan)) => {
                self.drop_tables(plan).await?;
                ExecutionResult::DropTables
            }
            LogicalPlan::Ddl(DdlPlan::DropViews(plan)) => {
                self.drop_views(plan).await?;
                ExecutionResult::DropViews
            }
            LogicalPlan::Ddl(DdlPlan::DropSchemas(plan)) => {
                self.drop_schemas(plan).await?;
                ExecutionResult::DropSchemas
            }
            LogicalPlan::Ddl(DdlPlan::DropDatabase(plan)) => {
                self.drop_database(plan).await?;
                ExecutionResult::DropDatabase
            }
            LogicalPlan::Write(WritePlan::Insert(plan)) => {
                self.insert(plan).await?;
                ExecutionResult::WriteSuccess
            }
            LogicalPlan::Query(plan) => {
                let physical = self.create_physical_plan(plan).await?;
                let stream = self.execute_physical(physical.clone())?;
                ExecutionResult::Query {
                    stream,
                    plan: physical,
                }
            }
            LogicalPlan::Variable(VariablePlan::SetVariable(plan)) => {
                self.set_variable(plan)?;
                ExecutionResult::SetLocal
            }
            LogicalPlan::Variable(VariablePlan::ShowVariable(plan)) => {
                let stream = self.show_variable(plan)?;
                ExecutionResult::ShowVariable { stream }
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
}

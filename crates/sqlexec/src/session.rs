use crate::context::{Portal, PreparedStatement, SessionContext};
use crate::errors::{internal, ExecError, Result};
use crate::logical_plan::*;
use crate::metastore::SupervisorClient;
use crate::parser::StatementWithExtensions;
use crate::vars::SessionVars;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, memory::MemoryStream, EmptyRecordBatchStream,
    ExecutionPlan, SendableRecordBatchStream,
};
use metastore::session::SessionCatalog;
use pgrepr::format::Format;
use serde_json::json;
use std::fmt;
use std::sync::Arc;
use telemetry::Tracker;
use uuid::Uuid;

/// Results from a sql statement execution.
pub enum ExecutionResult {
    /// The stream for the output of a query.
    Query { stream: SendableRecordBatchStream },
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
    /// Schema created.
    CreateSchema,
    /// A view was created.
    CreateView,
    /// A connection was created.
    CreateConnection,
    /// A client local variable was set.
    SetLocal,
    /// Tables dropped.
    DropTables,
    /// Schemas dropped.
    DropSchemas,
}

impl ExecutionResult {
    /// Tag for use when sending telemetry about the execution result.
    const fn telemetry_tag(&self) -> &'static str {
        match self {
            ExecutionResult::Query { .. } => "query",
            ExecutionResult::EmptyQuery => "empty_query",
            ExecutionResult::Begin => "begin",
            ExecutionResult::Commit => "commit",
            ExecutionResult::Rollback => "rollback",
            ExecutionResult::WriteSuccess => "write_success",
            ExecutionResult::CreateTable => "create_table",
            ExecutionResult::CreateSchema => "create_schema",
            ExecutionResult::CreateView => "create_view",
            ExecutionResult::CreateConnection => "create_connection",
            ExecutionResult::SetLocal => "set_local",
            ExecutionResult::DropTables => "drop_tables",
            ExecutionResult::DropSchemas => "drop_schemas",
        }
    }
}

impl fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutionResult::Query { stream } => write!(f, "query (schema: {:?})", stream.schema()),
            ExecutionResult::EmptyQuery => write!(f, "empty query"),
            ExecutionResult::Begin => write!(f, "begin"),
            ExecutionResult::Commit => write!(f, "commit"),
            ExecutionResult::Rollback => write!(f, "rollback"),
            ExecutionResult::WriteSuccess => write!(f, "write success"),
            ExecutionResult::CreateTable => write!(f, "create table"),
            ExecutionResult::CreateSchema => write!(f, "create schema"),
            ExecutionResult::CreateView => write!(f, "create view"),
            ExecutionResult::CreateConnection => write!(f, "create connection"),
            ExecutionResult::SetLocal => write!(f, "set local"),
            ExecutionResult::DropTables => write!(f, "drop tables"),
            ExecutionResult::DropSchemas => write!(f, "drop schemas"),
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
    tracker: Arc<Tracker>,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    pub fn new(
        conn_id: Uuid,
        catalog: SessionCatalog,
        metastore: SupervisorClient,
        tracker: Arc<Tracker>,
    ) -> Result<Session> {
        let ctx = SessionContext::new(conn_id, catalog, metastore);
        Ok(Session { ctx, tracker })
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
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan.execute(0, context)?),
            _ => {
                let plan = CoalescePartitionsExec::new(plan);
                Ok(plan.execute(0, context)?)
            }
        }
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

    pub(crate) async fn create_connection(&mut self, plan: CreateConnection) -> Result<()> {
        self.ctx.create_connection(plan).await?;
        Ok(())
    }

    pub(crate) async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        self.ctx.create_view(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        self.ctx.drop_tables(plan).await?;
        Ok(())
    }

    pub(crate) async fn drop_schemas(&mut self, plan: DropSchemas) -> Result<()> {
        self.ctx.drop_schemas(plan).await?;
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
        param_formats: Vec<Format>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        // We don't currently support parameters. We're already erroring on
        // attempting to prepare statements with parameters, so this is just
        // ensuring that we're not missing anything right now.
        assert_eq!(0, param_formats.len());
        assert_eq!(0, param_values.len());

        self.ctx
            .bind_statement(portal_name, stmt_name, result_formats)
    }

    /// Execute a portal.
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

        let result = match plan {
            LogicalPlan::Ddl(DdlPlan::CreateTable(plan)) => {
                self.create_table(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)) => {
                self.create_external_table(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateTableAs(plan)) => {
                self.create_table_as(plan).await?;
                ExecutionResult::CreateTable
            }
            LogicalPlan::Ddl(DdlPlan::CreateSchema(plan)) => {
                self.create_schema(plan).await?;
                ExecutionResult::CreateSchema
            }
            LogicalPlan::Ddl(DdlPlan::CreateConnection(plan)) => {
                self.create_connection(plan).await?;
                ExecutionResult::CreateConnection
            }
            LogicalPlan::Ddl(DdlPlan::CreateView(plan)) => {
                self.create_view(plan).await?;
                ExecutionResult::CreateView
            }
            LogicalPlan::Ddl(DdlPlan::DropTables(plan)) => {
                self.drop_tables(plan).await?;
                ExecutionResult::DropTables
            }
            LogicalPlan::Ddl(DdlPlan::DropSchemas(plan)) => {
                self.drop_schemas(plan).await?;
                ExecutionResult::DropSchemas
            }
            LogicalPlan::Write(WritePlan::Insert(plan)) => {
                self.insert(plan).await?;
                ExecutionResult::WriteSuccess
            }
            LogicalPlan::Query(plan) => {
                let physical = self.create_physical_plan(plan).await?;
                let stream = self.execute_physical(physical)?;
                ExecutionResult::Query { stream }
            }
            LogicalPlan::Variable(VariablePlan::SetVariable(plan)) => {
                self.set_variable(plan)?;
                ExecutionResult::SetLocal
            }
            LogicalPlan::Variable(VariablePlan::ShowVariable(plan)) => {
                let stream = self.show_variable(plan)?;
                ExecutionResult::Query { stream }
            }
            other => return Err(internal!("unimplemented logical plan: {:?}", other)),
        };

        // TODO: Use user id.
        self.tracker.track(
            "Execution complete",
            Uuid::nil(),
            json!({
                "tag": result.telemetry_tag(),
            }),
        );

        Ok(result)
    }
}

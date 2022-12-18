use crate::context::SessionContext;
use crate::errors::{ExecError, Result};
use crate::executor::ExecutionResult;
use crate::extended::{Portal, PreparedStatement};
use crate::logical_plan::*;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, EmptyRecordBatchStream, ExecutionPlan,
    SendableRecordBatchStream,
};
use jsoncat::catalog::Catalog;
use std::sync::Arc;

/// A per-client user session.
///
/// This type acts as the bridge between datafusion planning/execution and the
/// rest of the system.
pub struct Session {
    pub(crate) ctx: SessionContext,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    pub fn new(catalog: Arc<Catalog>) -> Result<Session> {
        let ctx = SessionContext::new(catalog);
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
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan.execute(0, context)?),
            _ => {
                let plan = CoalescePartitionsExec::new(plan);
                Ok(plan.execute(0, context)?)
            }
        }
    }

    pub(crate) async fn create_table(&self, plan: CreateTable) -> Result<()> {
        self.ctx.create_table(plan)?;
        Ok(())
    }

    pub(crate) async fn create_external_table(&self, _plan: CreateExternalTable) -> Result<()> {
        Err(ExecError::UnsupportedFeature("create external table"))
    }

    pub(crate) async fn create_table_as(&self, _plan: CreateTableAs) -> Result<()> {
        Err(ExecError::UnsupportedFeature("create table as"))
    }

    pub(crate) async fn create_schema(&self, plan: CreateSchema) -> Result<()> {
        self.ctx.create_schema(plan)?;
        Ok(())
    }

    pub async fn drop_tables(&self, plan: DropTables) -> Result<()> {
        self.ctx.drop_tables(plan)?;
        Ok(())
    }

    pub async fn drop_schemas(&self, plan: DropSchemas) -> Result<()> {
        self.ctx.drop_schemas(plan)?;
        Ok(())
    }

    pub(crate) async fn insert(&self, _plan: Insert) -> Result<()> {
        Err(ExecError::UnsupportedFeature("insert"))
    }

    pub(crate) fn set_configuration(&mut self, plan: SetConfiguration) -> Result<()> {
        let key = plan.variable.to_string().to_lowercase();
        match key.as_str() {
            "search_path" => self
                .ctx
                .try_set_search_path(&plan.try_as_single_string()?)?,
            _ => return Err(ExecError::InvalidSetKey(key)),
        }
        Ok(())
    }

    /// Store the prepared statement in the current session.
    /// It will later be readied for execution by using `bind_prepared_statement`.
    pub fn create_prepared_statement(
        &mut self,
        _name: Option<String>,
        _sql: String,
        _params: Vec<i32>,
    ) -> Result<()> {
        Err(ExecError::UnsupportedFeature("prepare"))
    }

    pub fn get_prepared_statement(&self, _name: &Option<String>) -> Option<&PreparedStatement> {
        // Currently called by pgsrv...
        None
    }

    pub fn get_portal(&self, _portal_name: &Option<String>) -> Option<&Portal> {
        // Currently called by pgsrv...
        None
    }

    /// Bind the parameters of a prepared statement to the given values.
    /// If successful, the bound statement will create a portal which can be used to execute the statement.
    pub fn bind_prepared_statement(
        &mut self,
        _portal_name: Option<String>,
        _statement_name: Option<String>,
        _param_formats: Vec<i16>,
        _param_values: Vec<Option<Vec<u8>>>,
        _result_formats: Vec<i16>,
    ) -> Result<()> {
        Err(ExecError::UnsupportedFeature("bind"))
    }

    pub async fn execute_portal(
        &mut self,
        _portal_name: &Option<String>,
        _max_rows: i32,
    ) -> Result<ExecutionResult> {
        Err(ExecError::UnsupportedFeature("execute"))
    }
}

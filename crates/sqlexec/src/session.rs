use crate::errors::{internal, Result};
use crate::executor::ExecutionResult;
use crate::extended::{Portal, PreparedStatement};
use crate::logical_plan::*;
use crate::parameters::{ParameterValue, SessionParameters, SEARCH_PATH_PARAM};
use crate::placeholders::bind_placeholders;
use access::runtime::AccessRuntime;
use catalog::catalog::{DatabaseCatalog, SessionCatalog};
use catalog_types::context::SessionContext;
use catalog_types::interfaces::{
    MutableCatalogProvider, MutableSchemaProvider, MutableTableProvider,
};
use catalog_types::keys::TableId;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::catalog::CatalogList;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::options::ParquetReadOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan as DfLogicalPlan;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, EmptyRecordBatchStream, ExecutionPlan,
    SendableRecordBatchStream,
};
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::planner::{convert_data_type, SqlToRel};
use datafusion::sql::sqlparser::ast::{self, ObjectType};
use datafusion::sql::{ResolvedTableReference, TableReference};
use futures::StreamExt;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tracing::debug;

/// A per-client user session.
///
/// This type acts as the bridge between datafusion planning/execution and the
/// rest of the system.
pub struct Session {
    sess_ctx: Arc<SessionContext>,
    sess_catalog: SessionCatalog,
    parameters: SessionParameters,
    // TODO: Transaction context goes here.

    // prepared statements
    unnamed_statement: Option<PreparedStatement>,
    named_statements: HashMap<String, PreparedStatement>,
    unnamed_portal: Option<Portal>,
    named_portals: HashMap<String, Portal>,
}

impl Session {
    /// Create a new session.
    ///
    /// All system schemas (including `information_schema`) should already be in
    /// the provided catalog.
    pub fn new(catalog: DatabaseCatalog) -> Result<Session> {
        let sess_ctx = Arc::new(SessionContext::new());
        // TODO: We might want to store the catalog itself when we figure out
        // how the transaction/session catalog will work.
        let sess_catalog = catalog.begin(sess_ctx.clone())?;
        Ok(Session {
            sess_ctx,
            sess_catalog,
            parameters: SessionParameters::default(),
            unnamed_statement: None,
            named_statements: HashMap::new(),
            unnamed_portal: None,
            named_portals: HashMap::new(),
        })
    }

    pub(crate) fn plan_sql(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");
        let context = SessionContextProvider { session: self };
        let planner = SqlToRel::new(&context);

        match statement {
            ast::Statement::StartTransaction { .. } => Ok(TransactionPlan::Begin.into()),
            ast::Statement::Commit { .. } => Ok(TransactionPlan::Commit.into()),
            ast::Statement::Rollback { .. } => Ok(TransactionPlan::Abort.into()),

            ast::Statement::Query(query) => {
                let plan = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::Explain {
                analyze,
                verbose,
                statement,
                ..
            } => {
                let plan = planner.explain_statement_to_plan(verbose, analyze, *statement)?;
                Ok(LogicalPlan::Query(plan))
            }

            ast::Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(DdlPlan::CreateSchema(CreateSchema {
                schema_name: schema_name.to_string(),
                if_not_exists,
            })
            .into()),

            // Normal tables.
            ast::Statement::CreateTable {
                external: false,
                if_not_exists,
                name,
                columns,
                query: None,
                ..
            } => {
                let mut arrow_cols = Vec::with_capacity(columns.len());
                for column in columns.into_iter() {
                    let dt = convert_data_type(&column.data_type)?;
                    let field = Field::new(&column.name.value, dt, false);
                    arrow_cols.push(field);
                }

                Ok(DdlPlan::CreateTable(CreateTable {
                    table_name: name.to_string(),
                    columns: arrow_cols,
                    if_not_exists,
                })
                .into())
            }

            // External tables.
            ast::Statement::CreateTable {
                external: true,
                if_not_exists: false,
                name,
                file_format: Some(file_format),
                location: Some(location),
                query: None,
                ..
            } => {
                let file_type: FileType = file_format.try_into()?;
                Ok(DdlPlan::CreateExternalTable(CreateExternalTable {
                    table_name: name.to_string(),
                    location,
                    file_type,
                })
                .into())
            }

            // Tables generated from a source query.
            //
            // CREATE TABLE table2 AS (SELECT * FROM table1);
            ast::Statement::CreateTable {
                external: false,
                name,
                query: Some(query),
                ..
            } => {
                let source = planner.query_to_plan(*query, &mut HashMap::new())?;
                Ok(DdlPlan::CreateTableAs(CreateTableAs {
                    table_name: name.to_string(),
                    source,
                })
                .into())
            }

            ast::Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table_name = table_name.to_string();
                let columns = columns.into_iter().map(|col| col.value).collect();

                let source = planner.query_to_plan(*source, &mut HashMap::new())?;

                Ok(WritePlan::Insert(Insert {
                    table_name,
                    columns,
                    source,
                })
                .into())
            }

            // Drop tables
            ast::Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => {
                let names = names
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect::<Vec<_>>();

                Ok(DdlPlan::DropTable(DropTable { if_exists, names }).into())
            }

            // "SET ...", "SET SESSION ...", "SET LOCAL ..."
            //
            // NOTE: Only session local variables are supported. Transaction
            // local variables behave the same as session local (they're not
            // reset on transaction abort/commit).
            ast::Statement::SetVariable {
                variable, value, ..
            } => Ok(RuntimePlan::SetParameter(SetParameter {
                variable,
                values: value,
            })
            .into()),

            // "SHOW ..."
            ast::Statement::ShowVariable { variable } => {
                Ok(RuntimePlan::ShowParameter(ShowParameter {
                    variable: ast::ObjectName(variable),
                })
                .into())
            }

            stmt => Err(internal!("unsupported sql statement: {:?}", stmt)),
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .sess_ctx
            .get_df_state()
            .create_physical_plan(&plan)
            .await?;
        Ok(plan)
    }

    pub(crate) fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.sess_ctx.task_context();
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
        let resolved = self.resolve_table_reference(plan.table_name.as_str());

        let schema = self
            .sess_catalog
            .user_schema(resolved.schema)
            .await?
            .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;

        schema
            .create_table(&self.sess_ctx, &plan.table_name, &Schema::new(plan.columns))
            .await?;

        Ok(())
    }

    pub(crate) async fn create_external_table(&self, plan: CreateExternalTable) -> Result<()> {
        // This will need an additional system table to track external tables.
        todo!()
    }

    pub(crate) async fn create_table_as(&self, plan: CreateTableAs) -> Result<()> {
        let resolved = self.resolve_table_reference(plan.table_name.as_ref());
        let schema = self
            .sess_catalog
            .user_schema(resolved.schema)
            .await?
            .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;

        // Plan and execute the source. We'll use the first batch from the
        // stream to create the table with the correct arrow schema.
        let physical = self.create_physical_plan(plan.source).await?;
        let mut stream = self.execute_physical(physical)?;

        // Create the table and insert the first batch.
        let table = match stream.next().await {
            Some(result) => {
                let batch = result?;
                let arrow_schema = batch.schema();
                schema
                    .create_table(&self.sess_ctx, &plan.table_name, &arrow_schema)
                    .await?;
                let table = schema
                    .get_mutable_table(&plan.table_name)
                    .await?
                    .ok_or_else(|| {
                        internal!("failed to get table after create: {}", plan.table_name)
                    })?;
                table.insert(&self.sess_ctx, batch).await?;
                table
            }
            None => {
                return Err(internal!(
                    "source stream empty, cannot infer schema from empty stream"
                ))
            }
        };

        // Insert the rest of the stream.
        while let Some(result) = stream.next().await {
            let batch = result?;
            table.insert(&self.sess_ctx, batch).await?;
        }

        Ok(())
    }

    pub(crate) async fn create_schema(&self, plan: CreateSchema) -> Result<()> {
        self.sess_catalog
            .create_schema(&self.sess_ctx, &plan.schema_name)
            .await?;
        Ok(())
    }

    pub(crate) async fn insert(&self, plan: Insert) -> Result<()> {
        let resolved = self.resolve_table_reference(plan.table_name.as_ref());
        let schema = self
            .sess_catalog
            .user_schema(&resolved.schema)
            .await?
            .ok_or_else(|| internal!("missing schema: {:?}", resolved.schema))?;

        let table = schema
            .get_mutable_table(&resolved.table)
            .await?
            .ok_or_else(|| internal!("missing table: {:?}", resolved.table))?;

        let physical = self.create_physical_plan(plan.source).await?;
        let mut stream = self.execute_physical(physical)?;

        while let Some(result) = stream.next().await {
            let batch = result?;
            table.insert(&self.sess_ctx, batch).await?;
        }

        Ok(())
    }

    pub(crate) fn set_parameter(&mut self, plan: SetParameter) -> Result<()> {
        self.parameters.set_parameter(plan.variable, plan.values)
    }

    /// Get the default schema to resolve table references with.
    ///
    /// TODO: This will only check the first schema in the search path. We'll
    /// want a way to search for tables in all schemas defined in the search
    /// path.
    fn default_schema(&self) -> &str {
        match self.parameters.get_parameter(SEARCH_PATH_PARAM) {
            Some(ParameterValue::Strings(schemas)) => match schemas.get(0) {
                Some(schema) => schema,
                None => "todo",
            },
            _ => "todo",
        }
    }

    /// Resolve a table reference.
    ///
    /// NOTE: This will resolve with only the first schema in the search path
    /// (or the default schema).
    fn resolve_table_reference<'a, 'b: 'a, R: Into<TableReference<'a>>>(
        &'b self,
        table: R,
    ) -> ResolvedTableReference<'a> {
        table
            .into()
            .resolve(self.sess_catalog.dbname(), self.default_schema())
    }

    /// Store the prepared statement in the current session.
    /// It will later be readied for execution by using `bind_prepared_statement`.
    pub fn create_prepared_statement(
        &mut self,
        name: Option<String>,
        sql: String,
        params: Vec<i32>,
    ) -> Result<()> {
        match name {
            None => {
                // Store the unnamed prepared statement.
                // This will persist until the session is dropped or another unnamed prepared statement is created
                self.unnamed_statement = Some(PreparedStatement::new(sql, params)?);
            }
            Some(name) => {
                // Named prepared statements must be explicitly closed before being redefined
                match self.named_statements.entry(name) {
                    Entry::Occupied(ent) => {
                        return Err(internal!(
                            "prepared statement already exists: {}",
                            ent.key()
                        ))
                    }
                    Entry::Vacant(ent) => {
                        ent.insert(PreparedStatement::new(sql, params)?);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get_prepared_statement(&self, name: &Option<String>) -> Option<&PreparedStatement> {
        match name {
            None => self.unnamed_statement.as_ref(),
            Some(name) => self.named_statements.get(name),
        }
    }

    pub fn get_portal(&self, portal_name: &Option<String>) -> Option<&Portal> {
        match portal_name {
            None => self.unnamed_portal.as_ref(),
            Some(name) => self.named_portals.get(name),
        }
    }

    /// Bind the parameters of a prepared statement to the given values.
    /// If successful, the bound statement will create a portal which can be used to execute the statement.
    pub fn bind_prepared_statement(
        &mut self,
        portal_name: Option<String>,
        statement_name: Option<String>,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<()> {
        let (statement, types) = {
            let prepared_statement = match statement_name {
                None => self
                    .unnamed_statement
                    .as_mut()
                    .ok_or_else(|| internal!("no unnamed prepared statement"))?,
                Some(name) => self
                    .named_statements
                    .get_mut(&name)
                    .ok_or_else(|| internal!("no prepared statement named: {}", name))?,
            };

            (
                prepared_statement.statement.clone(),
                prepared_statement.param_types.clone(),
            )
        };

        let bound_statement = bind_placeholders(statement, &param_formats, &param_values, &types)?;
        debug!(?bound_statement, "bound statement");
        let plan = self.plan_sql(bound_statement)?;

        match portal_name {
            None => {
                // Store the unnamed portal.
                // This will persist until the session is dropped or another unnamed portal is created
                self.unnamed_portal = Some(Portal::new(
                    plan,
                    param_formats,
                    param_values,
                    result_formats,
                )?);
            }
            Some(name) => {
                // Named portals must be explicitly closed before being redefined
                match self.named_portals.entry(name) {
                    Entry::Occupied(ent) => {
                        return Err(internal!("portal already exists: {}", ent.key()))
                    }
                    Entry::Vacant(_ent) => {
                        todo!("plan named portal");
                        // let plan = self.plan_sql(statement)?;
                        // ent.insert(Portal::new(plan, param_formats, param_values, result_formats)?);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn drop_table(&self, plan: DropTable) -> Result<()> {
        todo!()
    }

    pub async fn execute_portal(
        &mut self,
        portal_name: &Option<String>,
        _max_rows: i32,
    ) -> Result<ExecutionResult> {
        // TODO: respect max_rows
        let portal = match portal_name {
            None => self
                .unnamed_portal
                .as_mut()
                .ok_or_else(|| internal!("no unnamed portal"))?,
            Some(name) => self
                .named_portals
                .get_mut(name)
                .ok_or_else(|| internal!("no portal named: {}", name))?,
        };

        match portal.plan.clone() {
            LogicalPlan::Ddl(DdlPlan::CreateTable(plan)) => {
                self.create_table(plan).await?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::CreateExternalTable(plan)) => {
                self.create_external_table(plan).await?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::CreateTableAs(plan)) => {
                self.create_table_as(plan).await?;
                Ok(ExecutionResult::CreateTable)
            }
            LogicalPlan::Ddl(DdlPlan::DropTable(plan)) => {
                self.drop_table(plan).await?;
                Ok(ExecutionResult::DropTables)
            }
            LogicalPlan::Write(WritePlan::Insert(plan)) => {
                self.insert(plan).await?;
                Ok(ExecutionResult::WriteSuccess)
            }
            LogicalPlan::Query(plan) => {
                let physical = self.create_physical_plan(plan).await?;
                let stream = self.execute_physical(physical)?;
                Ok(ExecutionResult::Query { stream })
            }
            other => Err(internal!("unimplemented logical plan: {:?}", other)),
        }
    }
}

/// A wrapper around our session type to help with schema resolution.
struct SessionContextProvider<'a> {
    session: &'a Session,
}

impl<'a> ContextProvider for SessionContextProvider<'a> {
    fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        let name = self.session.resolve_table_reference(name);
        self.session
            .sess_ctx
            .get_df_state()
            .get_table_provider(name.into())
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.session.sess_ctx.get_df_state().get_function_meta(name)
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.session
            .sess_ctx
            .get_df_state()
            .get_aggregate_meta(name)
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        self.session
            .sess_ctx
            .get_df_state()
            .get_variable_type(variable_names)
    }
}

use crate::catalog::{DatabaseCatalog, DEFAULT_SCHEMA};
use crate::datasource::DeltaTable;
use crate::errors::{internal, Result};
use crate::executor::ExecutionResult;
use crate::extended::{Portal, PreparedStatement};
use crate::logical_plan::*;
use crate::placeholders::bind_placeholders;
use crate::runtime::AccessRuntime;
use access::keys::TableId;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::catalog::CatalogList;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::options::ParquetReadOptions;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_plan::LogicalPlan as DfLogicalPlan;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, EmptyRecordBatchStream, ExecutionPlan,
    SendableRecordBatchStream,
};
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
    /// Datafusion state for query planning and execution.
    state: SessionState,
    /// The concretely typed "GlareDB" catalog.
    catalog: Arc<DatabaseCatalog>,
    access_runtime: Arc<AccessRuntime>,
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
    pub fn new(catalog: Arc<DatabaseCatalog>, runtime: Arc<RuntimeEnv>) -> Session {
        // Note that the values for creating the default catalog/schema and
        // information schema do not matter, as we're not using the datafusion's
        // session context. Initializing the session context is what creates
        // thoses.
        let config = SessionConfig::default()
            .with_default_catalog_and_schema(catalog.name(), DEFAULT_SCHEMA)
            .create_default_catalog_and_schema(false)
            .with_information_schema(false);

        let mut state = SessionState::with_config_rt(config, runtime);
        state.catalog_list = catalog.clone();

        Session {
            state,
            catalog,
            access_runtime: Arc::new(AccessRuntime::default()), // TODO: Pass me in.
            unnamed_statement: None,
            named_statements: HashMap::new(),
            unnamed_portal: None,
            named_portals: HashMap::new(),
        }
    }

    pub(crate) fn plan_sql(&self, statement: ast::Statement) -> Result<LogicalPlan> {
        debug!(%statement, "planning sql statement");
        let planner = SqlToRel::new(&self.state);

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
            // TODO: Actually plan this.
            ast::Statement::SetVariable { .. } | ast::Statement::SetRole { .. } => {
                Ok(LogicalPlan::Runtime)
            }

            stmt => Err(internal!("unsupported sql statement: {:?}", stmt)),
        }
    }

    pub(crate) async fn create_physical_plan(
        &self,
        plan: DfLogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.state.create_physical_plan(&plan).await?;
        Ok(plan)
    }

    pub(crate) fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = Arc::new(TaskContext::from(&self.state));
        match plan.output_partitioning().partition_count() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
            1 => Ok(plan.execute(0, context)?),
            _ => {
                let plan = CoalescePartitionsExec::new(plan);
                Ok(plan.execute(0, context)?)
            }
        }
    }

    pub(crate) fn create_table(&self, plan: CreateTable) -> Result<()> {
        let table_ref: TableReference = plan.table_name.as_str().into();
        let resolved = table_ref.resolve(self.catalog.name(), DEFAULT_SCHEMA);

        let catalog = self
            .catalog
            .catalog(resolved.catalog)
            .ok_or_else(|| internal!("missing catalog: {}", resolved.catalog))?;
        let schema = catalog
            .schema(resolved.schema)
            .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;

        // TODO: If not exists

        let table_schema = Schema::new(plan.columns);
        let table = DeltaTable::new(
            new_table_id(),
            Arc::new(table_schema),
            self.access_runtime.clone(),
        );

        schema.register_table(resolved.table.to_string(), Arc::new(table))?;

        Ok(())
    }

    pub(crate) async fn create_external_table(&self, plan: CreateExternalTable) -> Result<()> {
        let resolved = self.resolve_table_name(&plan.table_name);
        let schema = self.get_schema_for_reference(&resolved)?;

        let target_partitions = self.state.config.target_partitions;
        let opts = match plan.file_type {
            FileType::Parquet => {
                ParquetReadOptions::default().to_listing_options(target_partitions)
            }
        };
        let path = ListingTableUrl::parse(&plan.location)?;
        let file_schema = opts.infer_schema(&self.state, &path).await?;
        let config = ListingTableConfig::new(path)
            .with_listing_options(opts)
            .with_schema(file_schema);

        let table = ListingTable::try_new(config)?;
        schema.register_table(resolved.table.to_string(), Arc::new(table))?;

        Ok(())
    }

    pub(crate) async fn create_table_as(&self, plan: CreateTableAs) -> Result<()> {
        let resolved = self.resolve_table_name(&plan.table_name);
        let schema = self.get_schema_for_reference(&resolved)?;

        // Plan and execute the source. We'll use the first batch from the
        // stream to create the table with the correct schema.
        let physical = self.create_physical_plan(plan.source).await?;
        let mut stream = self.execute_physical(physical)?;

        let table = match stream.next().await {
            Some(result) => {
                let batch = result?;
                let schema = batch.schema();
                let table = DeltaTable::new(new_table_id(), schema, self.access_runtime.clone());
                table.insert_batch(batch)?;
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
            table.insert_batch(batch)?;
        }

        // Finally register the table.
        schema.register_table(resolved.table.to_string(), Arc::new(table))?;

        Ok(())
    }

    pub(crate) async fn insert(&self, plan: Insert) -> Result<()> {
        let resolved = self.resolve_table_name(&plan.table_name);
        let schema = self.get_schema_for_reference(&resolved)?;

        let table = schema
            .table(resolved.table)
            .ok_or_else(|| internal!("missing table: {}", resolved.table))?;

        let table = table
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| internal!("cannot downcast to delta table"))?;

        let physical = self.create_physical_plan(plan.source).await?;
        let mut stream = self.execute_physical(physical)?;

        let schema = stream.schema();
        debug!(?schema, "inserting with schema");

        while let Some(result) = stream.next().await {
            let result = result?;
            table.insert_batch(result)?;
        }

        Ok(())
    }

    fn resolve_table_name<'a>(&'a self, table_name: &'a str) -> ResolvedTableReference<'a> {
        let table_ref: TableReference = table_name.into();
        table_ref.resolve(self.catalog.name(), DEFAULT_SCHEMA)
    }

    /// Get a schema provider given some resolved table reference.
    fn get_schema_for_reference(
        &self,
        resolved: &ResolvedTableReference,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let catalog = self
            .catalog
            .catalog(resolved.catalog)
            .ok_or_else(|| internal!("missing catalog: {}", resolved.catalog))?;
        let schema = catalog
            .schema(resolved.schema)
            .ok_or_else(|| internal!("missing schema: {}", resolved.schema))?;
        Ok(schema)
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
        debug!(?plan, "drop table");
        for name in plan.names {
            let resolved = self.resolve_table_name(&name);
            let schema = self.get_schema_for_reference(&resolved)?;

            schema.deregister_table(resolved.table)?;
        }

        Ok(())
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
                self.create_table(plan)?;
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

// TODO: This will be retrieved from the catalog.
fn new_table_id() -> TableId {
    static TABLE_ID_GEN: AtomicU32 = AtomicU32::new(0);
    TABLE_ID_GEN.fetch_add(1, Ordering::Relaxed)
}

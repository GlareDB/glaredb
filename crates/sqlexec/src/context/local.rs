use crate::background_jobs::storage::{BackgroundJobDeleteTable, BackgroundJobStorageTracker};
use crate::background_jobs::{BgJob, JobRunner};
use crate::environment::EnvironmentReader;
use crate::errors::{internal, ExecError, Result};
use crate::metastore::catalog::{CatalogMutator, SessionCatalog, TempObjects};
use crate::metrics::SessionMetrics;
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;
use crate::planner::session_planner::SessionPlanner;
use crate::remote::client::{RemoteClient, RemoteSessionClient};
use crate::remote::planner::RemoteQueryPlanner;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::SchemaReference;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{
    SessionConfig, SessionContext as DfSessionContext, SessionState, TaskContext,
};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, execute_stream, ExecutionPlan,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion::variable::VarType;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use futures::StreamExt;
use pgrepr::format::Format;
use pgrepr::types::arrow_to_pg_type;
use protogen::metastore::types::service::{self, Mutation};
use protogen::rpcsrv::types::service::{
    InitializeSessionRequest, InitializeSessionRequestFromClient,
};
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG};
use std::collections::HashMap;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;
use tracing::info;
use uuid::Uuid;

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

/// Context for a session used local execution and planning.
///
/// A session may be attached to a remote context in which parts of a query can
/// be executed remotely. When attaching a remote context, the query planner is
/// switched out to one that's able to generate plans to send to remote
/// contexts.
pub struct LocalSessionContext {
    /// The execution client for remote sessions.
    exec_client: Option<RemoteSessionClient>,
    /// Database catalog.
    catalog: SessionCatalog,
    /// Temporary objects dropped at the end of a session.
    temp_objects: TempObjects,
    /// Native tables.
    tables: NativeTableStorage,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Bound portals.
    portals: HashMap<String, Portal>,
    /// Track query metrics for this session.
    metrics: SessionMetrics,
    /// Datafusion session context used for planning and execution.
    df_ctx: DfSessionContext,
    /// Read tables from the environment.
    env_reader: Option<Box<dyn EnvironmentReader>>,
    /// Job runner for background jobs.
    background_jobs: JobRunner,
}

impl LocalSessionContext {
    /// Create a new session context with the given catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        metrics: SessionMetrics,
        spill_path: Option<PathBuf>,
        background_jobs: JobRunner,
    ) -> Result<LocalSessionContext> {
        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(vars);
        let mut conf: SessionConfig = opts.into();
        conf = conf.with_extension(Arc::new(catalog_mutator));
        let state = SessionState::with_config_rt(conf, Arc::new(runtime));

        let df_ctx = DfSessionContext::with_state(state);

        Ok(LocalSessionContext {
            exec_client: None,
            catalog,

            temp_objects: TempObjects::default(),
            tables: native_tables,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            metrics,
            df_ctx,
            env_reader: None,
            background_jobs,
        })
    }

    /// Attach a remote session to this session.
    // TODO: Remove test_db_id
    pub async fn attach_remote_session(
        &mut self,
        mut client: RemoteClient,
        test_db_id: Option<Uuid>,
    ) -> Result<()> {
        let (client, catalog) = client
            .initialize_session(InitializeSessionRequest::Client(
                InitializeSessionRequestFromClient { test_db_id },
            ))
            .await?;

        self.get_session_vars()
            .write()
            .remote_session_id
            .set_raw(Some(client.session_id()), VarType::System)
            .unwrap();
        self.exec_client = Some(client.clone());

        self.catalog = catalog;

        // Replace datafusion context with the remote aware planner.
        let planner = RemoteQueryPlanner::new(client);
        let state = self
            .df_ctx
            .state()
            .with_query_planner(Arc::new(planner))
            // TODO: We _could_ do something fancy where we keep the local catalog
            // around, but that's for another time.
            .with_config_extension(Arc::new(CatalogMutator::empty()));

        self.df_ctx = DfSessionContext::with_state(state);

        Ok(())
    }

    /// Close this session. This is only relevant for sessions connecting to
    /// remote clients.
    pub async fn close(&mut self) -> Result<()> {
        if let Some(mut client) = self.exec_client() {
            client.close_session().await?;
        }
        Ok(())
    }

    pub fn register_env_reader(&mut self, env_reader: Box<dyn EnvironmentReader>) {
        self.env_reader = Some(env_reader);
    }

    pub fn get_env_reader(&self) -> Option<&dyn EnvironmentReader> {
        self.env_reader.as_deref()
    }

    pub fn get_metrics(&self) -> &SessionMetrics {
        &self.metrics
    }

    pub fn get_metrics_mut(&mut self) -> &mut SessionMetrics {
        &mut self.metrics
    }

    pub fn get_native_tables(&self) -> &NativeTableStorage {
        &self.tables
    }

    pub fn get_temp_objects(&self) -> &TempObjects {
        &self.temp_objects
    }

    /// Return the DF session context.
    pub fn df_ctx(&self) -> &DfSessionContext {
        &self.df_ctx
    }

    /// Returns the underlaying exec client for remote session.
    pub fn exec_client(&self) -> Option<RemoteSessionClient> {
        self.exec_client.clone()
    }

    fn catalog_mutator(&self) -> Arc<CatalogMutator> {
        self.df_ctx
            .state()
            .config()
            .get_extension::<CatalogMutator>()
            .unwrap()
    }

    /// Create a temp table.
    pub async fn create_temp_table(&mut self, plan: CreateTempTable) -> Result<()> {
        if self
            .temp_objects
            .resolve_temp_table(&plan.table_name)
            .is_some()
        {
            if plan.if_not_exists {
                return Ok(());
            }
            return Err(ExecError::DuplicateObjectName(plan.table_name));
        }

        let schema = plan.schema.as_ref();
        let schema: Arc<ArrowSchema> = Arc::new(schema.into());

        let data = RecordBatch::new_empty(schema.clone());
        let table = Arc::new(MemTable::try_new(schema, vec![vec![data]])?);
        self.temp_objects
            .put_temp_table(plan.table_name, table.clone());

        // Write to the table if it has a source query
        if let Some(source) = plan.source {
            let insert_plan = Insert {
                source,
                table_provider: table,
            };
            self.insert(insert_plan).await?;
        }

        Ok(())
    }

    pub async fn create_table(&mut self, plan: CreateTable) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.table_name)?;

        // Insert into catalog.
        let state = self
            .catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateTable(service::CreateTable {
                    schema: schema.clone(),
                    name: name.clone(),
                    options: plan.schema.into(),
                    if_not_exists: plan.if_not_exists,
                })],
            )
            .await?;

        // Note that we're not changing out the catalog stored on the context,
        // we're just using it here to get the new table entry easily.
        let new_catalog = SessionCatalog::new(state);
        let ent = new_catalog
            .resolve_native_table(DEFAULT_CATALOG, &schema, &name)
            .ok_or_else(|| ExecError::Internal("Missing table after catalog insert".to_string()))?;

        // Create native table.
        let table = self.tables.create_table(ent).await?;
        info!(loc = %table.storage_location(), "native table created");

        // Write to the table if it has a source query
        if let Some(source) = plan.source {
            let insert_plan = Insert {
                source,
                table_provider: table.into_table_provider(),
            };
            self.insert(insert_plan).await?;
        }

        Ok(())
    }

    pub async fn insert(&mut self, plan: Insert) -> Result<()> {
        let state = self.df_ctx.state();

        let physical = state.create_physical_plan(&plan.source).await?;

        // Ensure physical plan has one output partition.
        let physical: Arc<dyn ExecutionPlan> =
            match physical.output_partitioning().partition_count() {
                1 => physical,
                _ => {
                    // merge into a single partition
                    Arc::new(CoalescePartitionsExec::new(physical))
                }
            };

        let physical = plan.table_provider.insert_into(&state, physical).await?;

        let context = self.task_context();
        let mut stream = execute_stream(physical, context)?;

        // Drain the stream to actually "write" successfully.
        while let Some(res) = stream.next().await {
            let _ = res?;
        }

        // Add the storage tracker job once data is inserted.
        if let Some(client) = &self.catalog_mutator().client {
            let tracker = BackgroundJobStorageTracker::new(self.tables.clone(), client.clone());
            self.background_jobs.add(tracker)?;
        }

        Ok(())
    }

    pub async fn delete(&mut self, plan: Delete) -> Result<usize> {
        let (database, schema, name) = self.resolve_table_ref(plan.table_name)?;

        if let Some(table_entry) = self.catalog.resolve_native_table(&database, &schema, &name) {
            Ok(self
                .tables
                .delete_rows_where(table_entry, plan.where_expr)
                .await?)
        } else {
            Err(ExecError::UnsupportedSQLStatement(
                "Delete for external tables".to_string(),
            ))
        }
    }

    pub async fn update(&mut self, plan: Update) -> Result<usize> {
        let (database, schema, name) = self.resolve_table_ref(plan.table_name)?;

        if let Some(table_entry) = self.catalog.resolve_native_table(&database, &schema, &name) {
            Ok(self
                .tables
                .update_rows_where(table_entry, plan.updates, plan.where_expr)
                .await?)
        } else {
            Err(ExecError::UnsupportedSQLStatement(
                "Update for external tables".to_string(),
            ))
        }
    }

    pub async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.table_name)?;
        // TODO: Check if catalog is valid

        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateExternalTable(
                    service::CreateExternalTable {
                        schema,
                        name,
                        options: plan.table_options,
                        if_not_exists: plan.if_not_exists,
                        tunnel: plan.tunnel,
                    },
                )],
            )
            .await?;
        Ok(())
    }

    /// Create a schema.
    pub async fn create_schema(&mut self, plan: CreateSchema) -> Result<()> {
        let (_, name) = Self::resolve_schema_ref(plan.schema_name);
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateSchema(service::CreateSchema {
                    name,
                    if_not_exists: plan.if_not_exists,
                })],
            )
            .await?;
        Ok(())
    }

    pub async fn create_external_database(&mut self, plan: CreateExternalDatabase) -> Result<()> {
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateExternalDatabase(
                    service::CreateExternalDatabase {
                        name: plan.database_name,
                        if_not_exists: plan.if_not_exists,
                        options: plan.options,
                        tunnel: plan.tunnel,
                    },
                )],
            )
            .await?;
        Ok(())
    }

    pub async fn create_tunnel(&mut self, plan: CreateTunnel) -> Result<()> {
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateTunnel(service::CreateTunnel {
                    name: plan.name,
                    if_not_exists: plan.if_not_exists,
                    options: plan.options,
                })],
            )
            .await?;
        Ok(())
    }

    pub async fn create_credentials(&mut self, plan: CreateCredentials) -> Result<()> {
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateCredentials(service::CreateCredentials {
                    name: plan.name,
                    options: plan.options,
                    comment: plan.comment,
                })],
            )
            .await?;
        Ok(())
    }

    pub async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.view_name)?;
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::CreateView(service::CreateView {
                    schema,
                    name,
                    sql: plan.sql,
                    or_replace: plan.or_replace,
                    columns: plan.columns,
                })],
            )
            .await?;

        Ok(())
    }

    pub async fn alter_table_rename(&mut self, plan: AlterTableRename) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.name)?;
        let (_, _, new_name) = self.resolve_table_ref(plan.new_name)?;
        // TODO: Check that the schema and catalog names are same.
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::AlterTableRename(service::AlterTableRename {
                    schema,
                    name,
                    new_name,
                })],
            )
            .await?;

        Ok(())
    }

    pub async fn alter_database_rename(&mut self, plan: AlterDatabaseRename) -> Result<()> {
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::AlterDatabaseRename(
                    service::AlterDatabaseRename {
                        name: plan.name,
                        new_name: plan.new_name,
                    },
                )],
            )
            .await?;

        Ok(())
    }

    pub async fn alter_tunnel_rotate_keys(&mut self, plan: AlterTunnelRotateKeys) -> Result<()> {
        self.catalog_mutator()
            .mutate(
                self.catalog.version(),
                [Mutation::AlterTunnelRotateKeys(
                    service::AlterTunnelRotateKeys {
                        name: plan.name,
                        if_exists: plan.if_exists,
                        new_ssh_key: plan.new_ssh_key,
                    },
                )],
            )
            .await?;

        Ok(())
    }

    /// Drop one or more tables.
    pub async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        let mut jobs = Vec::with_capacity(plan.names.len());
        let mut temp_table_drops = Vec::with_capacity(plan.names.len());

        for r in plan.names {
            if let Ok(table) = self.resolve_temp_table_ref(r.clone()) {
                // This is a temp table.
                temp_table_drops.push(table);
                continue;
            }

            let (database, schema, name) = self.resolve_table_ref(r)?;

            if let Some(table_entry) = self.catalog.resolve_native_table(&database, &schema, &name)
            {
                let job: Arc<dyn BgJob> =
                    BackgroundJobDeleteTable::new(self.tables.clone(), table_entry.clone());
                jobs.push(job);
            }

            drops.push(Mutation::DropObject(service::DropObject {
                schema,
                name,
                if_exists: plan.if_exists,
            }));
        }
        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        // Drop the session (temp) tables after catalog has mutated successfully
        // since this step is not going to error. Ideally, we should have transactions
        // here, but this works beautifully for now.
        for temp_table in temp_table_drops {
            self.temp_objects.drop_table(&temp_table);
        }

        // Run background jobs _after_ tables get removed from the catalog.
        //
        // Note: If/when we have transactions, background jobs should be stored
        // on the session until transaction commit.
        self.background_jobs.add_many(jobs)?;

        Ok(())
    }

    /// Drop one or more views.
    pub async fn drop_views(&mut self, plan: DropViews) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            let (_, schema, name) = self.resolve_table_ref(name)?;
            drops.push(Mutation::DropObject(service::DropObject {
                schema,
                name,
                if_exists: plan.if_exists,
            }));
        }

        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        Ok(())
    }

    /// Drop one or more schemas.
    pub async fn drop_schemas(&mut self, plan: DropSchemas) -> Result<()> {
        let drops: Vec<_> = plan
            .names
            .into_iter()
            .map(|name| {
                let (_, name) = Self::resolve_schema_ref(name);
                Mutation::DropSchema(service::DropSchema {
                    name,
                    if_exists: plan.if_exists,
                    cascade: plan.cascade,
                })
            })
            .collect();

        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        Ok(())
    }

    /// Drop one or more databases.
    pub async fn drop_database(&mut self, plan: DropDatabase) -> Result<()> {
        let drops: Vec<_> = plan
            .names
            .into_iter()
            .map(|name| {
                Mutation::DropDatabase(service::DropDatabase {
                    name,
                    if_exists: plan.if_exists,
                })
            })
            .collect();

        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        Ok(())
    }

    /// Drop one or more tunnels.
    pub async fn drop_tunnel(&mut self, plan: DropTunnel) -> Result<()> {
        let drops: Vec<_> = plan
            .names
            .into_iter()
            .map(|name| {
                Mutation::DropTunnel(service::DropTunnel {
                    name,
                    if_exists: plan.if_exists,
                })
            })
            .collect();

        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        Ok(())
    }

    /// Drop one or more credentials.
    pub async fn drop_credentials(&mut self, plan: DropCredentials) -> Result<()> {
        let drops: Vec<_> = plan
            .names
            .into_iter()
            .map(|name| {
                Mutation::DropCredentials(service::DropCredentials {
                    name,
                    if_exists: plan.if_exists,
                })
            })
            .collect();

        self.catalog_mutator()
            .mutate(self.catalog.version(), drops)
            .await?;

        Ok(())
    }

    /// Get a reference to the session variables.
    pub fn get_session_vars(&self) -> SessionVars {
        let cfg = self.df_ctx.copied_config();
        // i'm pretty sure it's safe to unwrap.
        // we don't expose any way to initialize the session without setting the session vars.
        let vars = cfg.options().extensions.get::<SessionVars>().unwrap();
        vars.clone()
    }

    /// Get a reference to the session catalog.
    pub fn get_session_catalog(&self) -> &SessionCatalog {
        &self.catalog
    }

    /// Create a prepared statement.
    pub async fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        _params: Vec<i32>, // TODO: We can use these for providing types for parameters.
    ) -> Result<()> {
        // Refresh the cached catalog state if necessary
        let mutator = self.catalog_mutator();
        let client = mutator.get_metastore_client();

        self.catalog
            .maybe_refresh_state(client, self.get_session_vars().force_catalog_refresh())
            .await?;

        // Unnamed (empty string) prepared statements can be overwritten
        // whenever. Named prepared statements must be explicitly removed before
        // being used again.
        if !name.is_empty() && self.prepared.contains_key(&name) {
            return Err(internal!(
                "named prepared statments must be deallocated before reuse, name: {}",
                name
            ));
        }

        let stmt = PreparedStatement::build(stmt, self).await?;
        self.prepared.insert(name, stmt);

        Ok(())
    }

    /// Bind a planned prepared statement to a portal.
    ///
    /// Internally this will create a logical plan for the statement and store
    /// that on the portal.
    // TODO: Accept parameters.
    pub fn bind_statement(
        &mut self,
        portal_name: String,
        stmt_name: &str,
        params: Vec<ScalarValue>,
        result_formats: Vec<Format>,
    ) -> Result<()> {
        // Unnamed portals can be overwritten, named portals need to be
        // explicitly deallocated.
        if !portal_name.is_empty() && self.portals.contains_key(&portal_name) {
            return Err(internal!(
                "named portals must be deallocated before reuse, name: {}",
                portal_name,
            ));
        }

        let mut stmt = match self.prepared.get(stmt_name) {
            Some(prepared) => prepared.clone(),
            None => return Err(ExecError::UnknownPreparedStatement(stmt_name.to_string())),
        };

        // Replace placeholders if necessary.
        if let Some(plan) = &mut stmt.plan {
            plan.replace_placeholders(params)?;
        }

        assert_eq!(
            result_formats.len(),
            stmt.output_fields().map(|f| f.len()).unwrap_or(0)
        );

        let portal = Portal {
            stmt,
            result_formats,
        };
        self.portals.insert(portal_name, portal);

        Ok(())
    }

    /// Get a prepared statement.
    pub fn get_prepared_statement(&self, name: &str) -> Result<&PreparedStatement> {
        self.prepared
            .get(name)
            .ok_or_else(|| ExecError::UnknownPreparedStatement(name.to_string()))
    }

    /// Get a portal.
    pub fn get_portal(&self, name: &str) -> Result<&Portal> {
        self.portals
            .get(name)
            .ok_or_else(|| ExecError::UnknownPortal(name.to_string()))
    }

    /// Remove a prepared statement.
    pub fn remove_prepared_statement(&mut self, name: &str) {
        // TODO: This should technically also remove portals that make use of
        // this prepared statement.
        self.prepared.remove(name);
    }

    /// Remove a portal.
    pub fn remove_portal(&mut self, name: &str) {
        self.portals.remove(name);
    }

    /// Get a datafusion task context to use for physical plan execution.
    pub(crate) fn task_context(&self) -> Arc<TaskContext> {
        self.df_ctx.task_ctx()
    }

    /// Resolve schema reference.
    fn resolve_schema_ref(r: SchemaReference<'_>) -> (String, String) {
        match r {
            SchemaReference::Bare { schema } => (DEFAULT_CATALOG.to_owned(), schema.into_owned()),
            SchemaReference::Full { schema, catalog } => {
                (catalog.into_owned(), schema.into_owned())
            }
        }
    }

    /// Resolve table reference for objects that live inside a schema.
    fn resolve_table_ref(&self, r: TableReference<'_>) -> Result<(String, String, String)> {
        let r = match r {
            TableReference::Bare { table } => {
                let schema = self.first_nonimplicit_schema()?;
                (
                    DEFAULT_CATALOG.to_owned(),
                    schema.to_owned(),
                    table.into_owned(),
                )
            }
            TableReference::Partial { schema, table } => (
                DEFAULT_CATALOG.to_owned(),
                schema.into_owned(),
                table.into_owned(),
            ),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => (
                catalog.into_owned(),
                schema.into_owned(),
                table.into_owned(),
            ),
        };
        Ok(r)
    }

    /// Resolve temp table reference for objects that live in the session.
    fn resolve_temp_table_ref(&self, r: TableReference<'_>) -> Result<String> {
        let table = match r {
            TableReference::Bare { table } => table,
            TableReference::Partial { schema, table } => {
                if schema.as_ref() == CURRENT_SESSION_SCHEMA {
                    table
                } else {
                    return Err(ExecError::InvalidTempTable {
                        reason: format!("can only have '{CURRENT_SESSION_SCHEMA}' schema"),
                    });
                }
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog.as_ref() == DEFAULT_CATALOG && schema.as_ref() == CURRENT_SESSION_SCHEMA
                {
                    table
                } else {
                    return Err(ExecError::InvalidTempTable {
                        reason: format!("can only have '{DEFAULT_CATALOG}' catalog and '{CURRENT_SESSION_SCHEMA}' schema")
                    });
                }
            }
        };

        if self.temp_objects.resolve_temp_table(&table).is_some() {
            Ok(table.into_owned())
        } else {
            Err(ExecError::InvalidTempTable {
                reason: format!("does not exist: {table}"),
            })
        }
    }

    /// Iterate over all values in the search path.
    pub(crate) fn search_paths(&self) -> Vec<String> {
        self.get_session_vars().search_path()
    }

    /// Iterate over the implicit search path. This will have all implicit
    /// schemas prepended to the iterator.
    ///
    /// This should be used when trying to resolve existing items.
    pub(crate) fn implicit_search_paths(&self) -> Vec<String> {
        self.get_session_vars().implicit_search_path()
    }

    /// Get the first non-implicit schema.
    pub(crate) fn first_nonimplicit_schema(&self) -> Result<String> {
        self.get_session_vars()
            .first_nonimplicit_schema()
            .ok_or_else(|| ExecError::EmptySearchPath)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PreparedStatement {
    pub(crate) stmt: Option<StatementWithExtensions>,
    /// The logical plan for the statement. Is `Some` if the statement is
    /// `Some`.
    pub(crate) plan: Option<LogicalPlan>,
    /// Parameter data types.
    pub(crate) parameter_types: Option<HashMap<String, Option<(PgType, DataType)>>>,
    /// The output schema of the statement if it produces an output.
    pub(crate) output_schema: Option<ArrowSchema>,
    /// Output postgres types.
    pub(crate) output_pg_types: Vec<PgType>,
}

impl PreparedStatement {
    /// Create and plan a new prepared statement.
    // TODO: Not sure if we want to delay the planning portion.
    async fn build(
        mut stmt: Option<StatementWithExtensions>,
        ctx: &LocalSessionContext,
    ) -> Result<Self> {
        if let Some(inner) = stmt.take() {
            // Go ahead and plan using the session context.
            let planner = SessionPlanner::new(ctx);
            let plan = planner.plan_ast(inner.clone()).await?;
            let schema = plan.output_schema();
            let pg_types = match &schema {
                Some(s) => {
                    // TODO: Type hints.
                    s.fields
                        .iter()
                        .map(|f| arrow_to_pg_type(f.data_type(), None))
                        .collect()
                }
                None => Vec::new(),
            };

            // Convert inferred arrow types for parameters into their associated
            // pg type.
            let parameter_types: HashMap<_, _> = plan
                .get_parameter_types()?
                .into_iter()
                .map(|(id, arrow_type)| {
                    let typ = arrow_type.map(|typ| (arrow_to_pg_type(&typ, None), typ));
                    (id, typ)
                })
                .collect();

            Ok(PreparedStatement {
                stmt: Some(inner),
                plan: Some(plan),
                parameter_types: Some(parameter_types),
                output_schema: schema,
                output_pg_types: pg_types,
            })
        } else {
            // No statement to plan.
            Ok(PreparedStatement {
                stmt: None,
                plan: None,
                parameter_types: None,
                output_schema: None,
                output_pg_types: Vec::new(),
            })
        }
    }

    /// Returns an iterator over the fields of output schema (if any).
    pub fn output_fields(&self) -> Option<OutputFields<'_>> {
        self.output_schema.as_ref().map(|s| OutputFields {
            len: s.fields.len(),
            arrow_fields: s.fields.into_iter(),
            pg_types: self.output_pg_types.iter(),
            result_formats: None,
        })
    }

    /// Returns the type of the input parameters. Input paramets are keyed as
    /// "$n" starting at "$1".
    pub fn input_paramaters(&self) -> Option<&HashMap<String, Option<(PgType, DataType)>>> {
        self.parameter_types.as_ref()
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Portal {
    /// The associated prepared statement.
    pub(crate) stmt: PreparedStatement,
    /// Requested output formats.
    pub(crate) result_formats: Vec<Format>,
}

impl Portal {
    /// Returns an iterator over the fields of output schema (if any).
    pub fn output_fields(&self) -> Option<OutputFields<'_>> {
        self.stmt.output_fields()
    }
}

/// Iterator over the various fields of output schema.
pub struct OutputFields<'a> {
    len: usize,
    arrow_fields: slice::Iter<'a, Arc<ArrowField>>,
    pg_types: slice::Iter<'a, PgType>,
    result_formats: Option<slice::Iter<'a, Format>>,
}

impl<'a> OutputFields<'a> {
    /// Number of fields in the output.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if there are no fields in output.
    // This method was required by clippy. Not really useful since we already
    // return an `Option` from the `output_fields` method.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Structure to hold information about the output field.
pub struct OutputField<'a> {
    pub name: &'a String,
    pub arrow_type: &'a DataType,
    pub pg_type: &'a PgType,
    pub format: &'a Format,
}

impl<'a> Iterator for OutputFields<'a> {
    type Item = OutputField<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let field = self.arrow_fields.next()?;
        let format = if let Some(formats) = self.result_formats.as_mut() {
            formats
                .next()
                .expect("result_formats should have the same length as fields")
        } else {
            &Format::Text
        };
        Some(Self::Item {
            name: field.name(),
            arrow_type: field.data_type(),
            pg_type: self
                .pg_types
                .next()
                .expect("pg_types should have the same length as fields"),
            format,
        })
    }
}

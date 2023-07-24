use crate::environment::EnvironmentReader;
use crate::errors::{internal, ExecError, Result};
use crate::metastore::SupervisorClient;
use crate::metrics::SessionMetrics;
use crate::parser::{CustomParser, StatementWithExtensions};
use crate::planner::errors::PlanError;
use crate::planner::logical_plan::*;
use crate::planner::session_planner::SessionPlanner;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Column as DfColumn, SchemaReference};
use datafusion::config::{CatalogOptions, ConfigOptions, OptimizerOptions};
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlanBuilder as DfLogicalPlanBuilder};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, execute_stream, ExecutionPlan,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use datasources::object_store::registry::GlareDBRegistry;
use futures::{future::BoxFuture, StreamExt};
use metastore_client::errors::ResolveErrorStrategy;
use metastore_client::session::SessionCatalog;
use metastore_client::types::catalog::{CatalogEntry, EntryType};
use metastore_client::types::options::TableOptions;
use metastore_client::types::service::{self, Mutation};
use pgrepr::format::Format;
use pgrepr::types::arrow_to_pg_type;
use sqlbuiltins::builtins::{CURRENT_SESSION_SCHEMA, DEFAULT_CATALOG, POSTGRES_SCHEMA};
use std::collections::HashMap;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;
use tracing::{debug, info};

/// Implicity schemas that are consulted during object resolution.
///
/// Note that these are not normally shown in the search path.
const IMPLICIT_SCHEMAS: [&str; 2] = [
    POSTGRES_SCHEMA,
    // Objects stored in current session will always have a priority over the
    // schemas in search path.
    CURRENT_SESSION_SCHEMA,
];

/// Context for a session used during execution.
///
/// The context generally does not have to worry about anything external to the
/// database. Its source of truth is the in-memory catalog.
// TODO: Need to make session context less pervasive. Pretty much everything in
// this crate relies on it, make test setup a pain.
pub struct SessionContext {
    /// Database catalog.
    metastore_catalog: SessionCatalog,
    metastore: SupervisorClient,
    /// In-memory (temporary) tables.
    current_session_tables: HashMap<String, Arc<MemTable>>,
    /// Native tables.
    tables: NativeTableStorage,
    /// Session variables.
    vars: SessionVars,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Bound portals.
    portals: HashMap<String, Portal>,
    /// Track query metrics for this session.
    metrics: SessionMetrics,
    /// Datafusion session state used for planning and execution.
    ///
    /// This session state makes a ton of assumptions, try to keep usage of it
    /// to a minimum and ensure interactions with this are well-defined.
    df_state: SessionState,
    /// Read tables from the environment.
    env_reader: Option<Box<dyn EnvironmentReader>>,
}

impl SessionContext {
    /// Create a new session context with the given catalog.
    ///
    /// If `spill_path` is provided, a new disk manager will be created pointing
    /// to that path. Otherwise the manager will be kept as default (request
    /// temp files from the OS).
    ///
    /// If `info.memory_limit_bytes` is non-zero, a new memory pool will be
    /// created with the max set to this value.
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        metastore: SupervisorClient,
        native_tables: NativeTableStorage,
        metrics: SessionMetrics,
        spill_path: Option<PathBuf>,
    ) -> SessionContext {
        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let mut catalog_opts = CatalogOptions::default();
        catalog_opts.create_default_catalog_and_schema = false;
        catalog_opts.information_schema = false;
        let mut optimizer_opts = OptimizerOptions::default();
        optimizer_opts.prefer_hash_join = true;
        let mut config_opts = ConfigOptions::new();
        config_opts.catalog = catalog_opts;
        config_opts.optimizer = optimizer_opts;
        let config: SessionConfig = config_opts.into();

        // Create a new datafusion runtime env with disk manager and memory pool
        // if needed.
        let entries = catalog
            .iter_entries()
            .filter(|e| e.entry_type() == EntryType::Table && !e.builtin);

        let mut conf = RuntimeConfig::default();
        conf =
            conf.with_object_store_registry(Arc::new(GlareDBRegistry::try_new(entries).unwrap()));
        if let Some(spill_path) = spill_path {
            conf = conf.with_disk_manager(DiskManagerConfig::NewSpecified(vec![spill_path]));
        }
        if let &Some(mem_limit) = vars.memory_limit_bytes.value() {
            // TODO: Make this actually have optional semantics.
            if mem_limit > 0 {
                conf = conf.with_memory_pool(Arc::new(GreedyMemoryPool::new(mem_limit)));
            }
        }
        let runtime = Arc::new(RuntimeEnv::new(conf).unwrap());

        let state = SessionState::with_config_rt(config, runtime);

        // Note that we do not replace the default catalog list on the state. We
        // should never be referencing it during planning or execution.
        //
        // Ideally we can reduce the need to rely on datafusion's session state
        // as much as possible. It makes way too many assumptions.

        SessionContext {
            metastore_catalog: catalog,
            metastore,
            current_session_tables: HashMap::new(),
            tables: native_tables,
            vars,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            metrics,
            df_state: state,
            env_reader: None,
        }
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

    pub fn get_datasource_count(&mut self) -> usize {
        self.metastore_catalog
            .iter_entries()
            .filter(|ent| {
                ent.entry.get_meta().external
                    && (ent.entry.get_meta().entry_type == EntryType::Database
                        || ent.entry.get_meta().entry_type == EntryType::Table)
            })
            .count()
    }

    pub fn get_tunnel_count(&mut self) -> usize {
        self.metastore_catalog
            .iter_entries()
            .filter(|ent| ent.entry.get_meta().entry_type == EntryType::Tunnel)
            .count()
    }

    pub fn get_credentials_count(&mut self) -> usize {
        self.metastore_catalog
            .iter_entries()
            .filter(|ent| ent.entry.get_meta().entry_type == EntryType::Credentials)
            .count()
    }

    /// Resolve temp table.
    pub fn resolve_temp_table(&self, table_name: &str) -> Option<Arc<MemTable>> {
        self.current_session_tables.get(table_name).cloned()
    }

    /// Create a temp table.
    pub async fn create_temp_table(&mut self, plan: CreateTempTable) -> Result<()> {
        if self.current_session_tables.contains_key(&plan.table_name) {
            if plan.if_not_exists {
                return Ok(());
            }
            return Err(ExecError::DuplicateObjectName(plan.table_name));
        }

        let schema = Arc::new(ArrowSchema::new(plan.columns));
        let data = RecordBatch::new_empty(Arc::clone(&schema));
        let table = Arc::new(MemTable::try_new(schema, vec![vec![data]])?);
        self.current_session_tables
            .insert(plan.table_name, Arc::clone(&table));

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
        self.mutate_catalog([Mutation::CreateTable(service::CreateTable {
            schema: schema.clone(),
            name: name.clone(),
            options: plan.table_options,
            if_not_exists: plan.if_not_exists,
        })])
        .await?;

        // Mutations should update the local catalog state, so resolve the
        // entry.
        let ent = match self
            .metastore_catalog
            .resolve_entry(DEFAULT_CATALOG, &schema, &name)
        {
            Some(CatalogEntry::Table(table)) => match &table.options {
                TableOptions::Internal(_) => table,
                other => {
                    return Err(ExecError::Internal(format!(
                        "Unexpected set of table options: {:?}",
                        other
                    )))
                }
            },
            Some(other) => {
                return Err(ExecError::Internal(format!(
                    "Unexpected catalog entry type: {:?}",
                    other
                )))
            }
            None => {
                return Err(ExecError::Internal(
                    "Missing table after catalog insert".to_string(),
                ));
            }
        };

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
        let physical = self
            .get_df_state()
            .create_physical_plan(&plan.source)
            .await?;

        // Ensure physical plan has one output partition.
        let physical: Arc<dyn ExecutionPlan> =
            match physical.output_partitioning().partition_count() {
                1 => physical,
                _ => {
                    // merge into a single partition
                    Arc::new(CoalescePartitionsExec::new(physical))
                }
            };

        let physical = plan
            .table_provider
            .insert_into(self.get_df_state(), physical)
            .await?;

        let context = self.task_context();
        let mut stream = execute_stream(physical, context)?;

        // Drain the stream to actually "write" successfully.
        while let Some(res) = stream.next().await {
            let _ = res?;
        }
        Ok(())
    }

    /// List temporary tables.
    pub fn list_temp_tables(&self) -> impl Iterator<Item = &str> {
        self.current_session_tables.keys().map(|k| k.as_str())
    }

    pub async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        if let &Some(limit) = self.vars.max_datasource_count.value() {
            if self.get_datasource_count() >= limit {
                return Err(ExecError::MaxObjectCount {
                    typ: "datasources",
                    max: limit,
                    current: self.get_datasource_count(),
                });
            }
        }

        let (_, schema, name) = self.resolve_table_ref(plan.table_name)?;
        // TODO: Check if catalog is valid

        self.mutate_catalog([Mutation::CreateExternalTable(
            service::CreateExternalTable {
                schema,
                name,
                options: plan.table_options,
                if_not_exists: plan.if_not_exists,
                tunnel: plan.tunnel,
            },
        )])
        .await?;
        Ok(())
    }

    /// Create a schema.
    pub async fn create_schema(&mut self, plan: CreateSchema) -> Result<()> {
        let (_, name) = Self::resolve_schema_ref(plan.schema_name);
        // TODO: if_not_exists
        self.mutate_catalog([Mutation::CreateSchema(service::CreateSchema { name })])
            .await?;
        Ok(())
    }

    pub async fn create_external_database(&mut self, plan: CreateExternalDatabase) -> Result<()> {
        if let &Some(limit) = self.vars.max_datasource_count.value() {
            if self.get_datasource_count() >= limit {
                return Err(ExecError::MaxObjectCount {
                    typ: "datasources",
                    max: limit,
                    current: self.get_datasource_count(),
                });
            }
        }

        self.mutate_catalog([Mutation::CreateExternalDatabase(
            service::CreateExternalDatabase {
                name: plan.database_name,
                if_not_exists: plan.if_not_exists,
                options: plan.options,
                tunnel: plan.tunnel,
            },
        )])
        .await?;
        Ok(())
    }

    pub async fn create_tunnel(&mut self, plan: CreateTunnel) -> Result<()> {
        if let &Some(limit) = self.vars.max_tunnel_count.value() {
            if self.get_tunnel_count() >= limit {
                return Err(ExecError::MaxObjectCount {
                    typ: "tunnels",
                    max: limit,
                    current: self.get_tunnel_count(),
                });
            }
        }

        self.mutate_catalog([Mutation::CreateTunnel(service::CreateTunnel {
            name: plan.name,
            if_not_exists: plan.if_not_exists,
            options: plan.options,
        })])
        .await?;
        Ok(())
    }

    pub async fn create_credentials(&mut self, plan: CreateCredentials) -> Result<()> {
        if let &Some(limit) = self.vars.max_credentials_count.value() {
            if self.get_credentials_count() >= limit {
                return Err(ExecError::MaxObjectCount {
                    typ: "credentials",
                    max: limit,
                    current: self.get_tunnel_count(),
                });
            }
        }

        self.mutate_catalog([Mutation::CreateCredentials(service::CreateCredentials {
            name: plan.name,
            options: plan.options,
            comment: plan.comment,
        })])
        .await?;
        Ok(())
    }

    pub async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.view_name)?;
        self.mutate_catalog([Mutation::CreateView(service::CreateView {
            schema,
            name,
            sql: plan.sql,
            or_replace: plan.or_replace,
            columns: plan.columns,
        })])
        .await?;

        Ok(())
    }

    pub async fn alter_table_rename(&mut self, plan: AlterTableRename) -> Result<()> {
        let (_, schema, name) = self.resolve_table_ref(plan.name)?;
        let (_, _, new_name) = self.resolve_table_ref(plan.new_name)?;
        // TODO: Check that the schema and catalog names are same.
        self.mutate_catalog([Mutation::AlterTableRename(service::AlterTableRename {
            schema,
            name,
            new_name,
        })])
        .await?;

        Ok(())
    }

    pub async fn alter_database_rename(&mut self, plan: AlterDatabaseRename) -> Result<()> {
        self.mutate_catalog([Mutation::AlterDatabaseRename(
            service::AlterDatabaseRename {
                name: plan.name,
                new_name: plan.new_name,
            },
        )])
        .await?;

        Ok(())
    }

    pub async fn alter_tunnel_rotate_keys(&mut self, plan: AlterTunnelRotateKeys) -> Result<()> {
        self.mutate_catalog([Mutation::AlterTunnelRotateKeys(
            service::AlterTunnelRotateKeys {
                name: plan.name,
                if_exists: plan.if_exists,
                new_ssh_key: plan.new_ssh_key,
            },
        )])
        .await?;

        Ok(())
    }

    /// Drop one or more tables.
    pub async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        for r in plan.names {
            let (_, schema, name) = self.resolve_table_ref(r)?;
            drops.push(Mutation::DropObject(service::DropObject {
                schema,
                name,
                if_exists: plan.if_exists,
            }));
        }

        self.mutate_catalog(drops).await?;

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

        self.mutate_catalog(drops).await?;

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

        self.mutate_catalog(drops).await?;

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

        self.mutate_catalog(drops).await?;

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

        self.mutate_catalog(drops).await?;

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

        self.mutate_catalog(drops).await?;

        Ok(())
    }

    /// Get a reference to the session variables.
    pub fn get_session_vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Get a mutable reference to the session variables.
    pub fn get_session_vars_mut(&mut self) -> &mut SessionVars {
        &mut self.vars
    }

    /// Get a reference to the session catalog.
    pub fn get_session_catalog(&self) -> &SessionCatalog {
        &self.metastore_catalog
    }

    /// Create a prepared statement.
    pub async fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        _params: Vec<i32>, // TODO: We can use these for providing types for parameters.
    ) -> Result<()> {
        // Refresh the cached catalog state if necessary
        if *self.get_session_vars().force_catalog_refresh.value() {
            debug!("refreshed cached catalog state as per force_catalog_refresh");
            self.metastore.refresh_cached_state().await?;
        }

        // Swap out cached catalog if a newer one was fetched.
        //
        // Note that when we have transactions, we should move this to only
        // swapping states between transactions.
        if self.metastore.version_hint() != self.metastore_catalog.version() {
            debug!(version = %self.metastore_catalog.version(), "swapping catalog state for session");
            let new_state = self.metastore.get_cached_state().await?;
            self.metastore_catalog.swap_state(new_state);
        }

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
        Arc::new(TaskContext::from(&self.df_state))
    }

    /// Get a datafusion session state.
    pub(crate) fn get_df_state(&self) -> &SessionState {
        &self.df_state
    }

    /// Plan the body of a view.
    pub(crate) fn late_view_plan<'a, 'b: 'a>(
        &'a self,
        sql: &'b str,
        columns: &'b [String],
    ) -> BoxFuture<Result<datafusion::logical_expr::LogicalPlan, PlanError>> {
        // TODO: Instead of doing late planning, we should instead try to insert
        // the contents of the view into the parent query prior to any planning.
        //
        // The boxed future is a quick fix to enable recursive async planning.
        Box::pin(async move {
            let mut statements = CustomParser::parse_sql(sql)?;
            if statements.len() != 1 {
                return Err(PlanError::ExpectedExactlyOneStatement(
                    statements.into_iter().collect(),
                ));
            }

            let planner = SessionPlanner::new(self);

            let plan = planner.plan_ast(statements.pop_front().unwrap()).await?;
            let mut df_plan = plan.try_into_datafusion_plan()?;

            // Wrap logical plan in projection if the view was defined with
            // column aliases.
            if !columns.is_empty() {
                let fields = df_plan.schema().fields().clone();
                df_plan = DfLogicalPlanBuilder::from(df_plan)
                    .project(fields.iter().zip(columns.iter()).map(|(field, alias)| {
                        DfExpr::Column(DfColumn::new_unqualified(field.name())).alias(alias)
                    }))?
                    .build()?;
            }

            Ok(df_plan)
        })
    }

    /// Attempt to apply mutations to the catalog.
    ///
    /// This will retry mutations if we were working with an out of date
    /// catalog.
    async fn mutate_catalog(
        &mut self,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<()> {
        // Note that when we have transactions, these shouldn't be sent until
        // commit.
        let mutations: Vec<_> = mutations.into_iter().collect();

        let state = match self
            .metastore
            .try_mutate(self.metastore_catalog.version(), mutations.clone())
            .await
        {
            Ok(state) => state,
            Err(ExecError::MetastoreTonic {
                strategy: ResolveErrorStrategy::FetchCatalogAndRetry,
                message,
            }) => {
                // Go ahead and refetch the catalog and retry the mutation.
                //
                // Note that this relies on metastore _always_ being stricter
                // when validating mutations. What this means is that retrying
                // here should be semantically equivalent to manually refreshing
                // the catalog and rerunning and replanning the query.
                debug!(error_message = message, "retrying mutations");

                self.metastore.refresh_cached_state().await?;
                let state = self.metastore.get_cached_state().await?;
                let version = state.version;
                self.metastore_catalog.swap_state(state);

                self.metastore.try_mutate(version, mutations).await?
            }
            Err(e) => return Err(e),
        };
        self.metastore_catalog.swap_state(state);
        Ok(())
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

    /// Iterate over all values in the search path.
    pub(crate) fn search_path_iter(&self) -> impl Iterator<Item = &str> {
        self.vars.search_path.value().iter().map(|s| s.as_str())
    }

    /// Iterate over the implicit search path. This will have all implicit
    /// schemas prepended to the iterator.
    ///
    /// This should be used when trying to resolve existing items.
    pub(crate) fn implicit_search_path_iter(&self) -> impl Iterator<Item = &str> {
        IMPLICIT_SCHEMAS
            .into_iter()
            .chain(self.vars.search_path.value().iter().map(|s| s.as_str()))
    }

    /// Get the first non-implicit schema.
    fn first_nonimplicit_schema(&self) -> Result<&str> {
        self.search_path_iter()
            .next()
            .ok_or(ExecError::EmptySearchPath)
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
        ctx: &SessionContext,
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

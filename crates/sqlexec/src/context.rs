use crate::engine::SessionInfo;
use crate::errors::{internal, ExecError, Result};
use crate::metastore::SupervisorClient;
use crate::metrics::SessionMetrics;
use crate::parser::{CustomParser, StatementWithExtensions};
use crate::planner::errors::PlanError;
use crate::planner::logical_plan::*;
use crate::planner::session_planner::SessionPlanner;
use crate::vars::SessionVars;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::config::{CatalogOptions, ConfigOptions};
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datasource_common::ssh::SshTunnelAccess;
use futures::future::BoxFuture;
use metastore::builtins::DEFAULT_CATALOG;
use metastore::builtins::POSTGRES_SCHEMA;
use metastore::session::SessionCatalog;
use metastore::types::catalog::{self, ColumnDefinition, ConnectionEntry, ConnectionOptions};
use metastore::types::service::{self, Mutation};
use pgrepr::format::Format;
use pgrepr::types::arrow_to_pg_type;
use std::borrow::Cow;
use std::collections::HashMap;
use std::slice;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;
use tracing::debug;

/// Implicity schemas that are consulted during object resolution.
///
/// Note that these are not normally shown in the search path.
const IMPLICIT_SCHEMAS: [&str; 1] = [POSTGRES_SCHEMA];

/// Context for a session used during execution.
///
/// The context generally does not have to worry about anything external to the
/// database. Its source of truth is the in-memory catalog.
// TODO: Need to make session context less pervasive. Pretty much everything in
// this crate relies on it, make test setup a pain.
pub struct SessionContext {
    info: Arc<SessionInfo>,
    /// Database catalog.
    metastore_catalog: SessionCatalog,
    metastore: SupervisorClient,
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
}

impl SessionContext {
    /// Create a new session context with the given catalog.
    pub fn new(
        info: Arc<SessionInfo>,
        catalog: SessionCatalog,
        metastore: SupervisorClient,
        metrics: SessionMetrics,
    ) -> SessionContext {
        // TODO: Pass in datafusion runtime env.

        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let mut catalog_opts = CatalogOptions::default();
        catalog_opts.create_default_catalog_and_schema = false;
        catalog_opts.information_schema = false;
        let mut config_opts = ConfigOptions::new();
        config_opts.catalog = catalog_opts;
        let config: SessionConfig = config_opts.into();

        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);

        // Note that we do not replace the default catalog list on the state. We
        // should never be referencing it during planning or execution.
        //
        // Ideally we can reduce the need to rely on datafusion's session state
        // as much as possible. It makes way too many assumptions.

        SessionContext {
            info,
            metastore_catalog: catalog,
            metastore,
            vars: SessionVars::default(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            metrics,
            df_state: state,
        }
    }

    pub fn get_info(&self) -> &SessionInfo {
        self.info.as_ref()
    }

    pub fn get_metrics(&self) -> &SessionMetrics {
        &self.metrics
    }

    pub fn get_metrics_mut(&mut self) -> &mut SessionMetrics {
        &mut self.metrics
    }

    /// Create a table.
    pub fn create_table(&self, _plan: CreateTable) -> Result<()> {
        Err(ExecError::UnsupportedFeature("CREATE TABLE"))
    }

    pub async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        let (_, schema, name) = self.resolve_object_reference(plan.table_name.into())?;
        let columns = plan
            .columns
            .into_iter()
            .map(|field| ColumnDefinition {
                name: field.name().to_owned(),
                nullable: field.is_nullable(),
                arrow_type: field.data_type().to_owned(),
            })
            .collect();
        self.mutate_catalog([Mutation::CreateExternalTable(
            service::CreateExternalTable {
                schema,
                name,
                connection_id: plan.connection_id,
                options: plan.table_options,
                if_not_exists: plan.if_not_exists,
                columns,
            },
        )])
        .await?;
        Ok(())
    }

    /// Create a schema.
    pub async fn create_schema(&mut self, plan: CreateSchema) -> Result<()> {
        self.mutate_catalog([Mutation::CreateSchema(service::CreateSchema {
            name: plan.schema_name,
        })])
        .await?;
        Ok(())
    }

    pub async fn create_database(&mut self, plan: CreateDatabase) -> Result<()> {
        self.mutate_catalog([Mutation::CreateDatabase(service::CreateDatabase {
            name: plan.database_name,
            if_not_exists: plan.if_not_exists,
            options: plan.options,
        })])
        .await?;
        Ok(())
    }

    pub async fn create_connection(&mut self, plan: CreateConnection) -> Result<()> {
        let (_, schema, name) = self.resolve_object_reference(plan.connection_name.into())?;
        self.mutate_catalog([Mutation::CreateConnection(service::CreateConnection {
            schema,
            name,
            options: plan.options,
            if_not_exists: plan.if_not_exists,
        })])
        .await?;

        Ok(())
    }

    pub async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        let (_, schema, name) = self.resolve_object_reference(plan.view_name.into())?;
        self.mutate_catalog([Mutation::CreateView(service::CreateView {
            schema,
            name,
            sql: plan.sql,
        })])
        .await?;

        Ok(())
    }

    /// Drop one or more tables.
    pub async fn drop_tables(&mut self, plan: DropTables) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            let (_, schema, name) = self.resolve_object_reference(name.into())?;
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
            let (_, schema, name) = self.resolve_object_reference(name.into())?;
            drops.push(Mutation::DropObject(service::DropObject {
                schema,
                name,
                if_exists: plan.if_exists,
            }));
        }

        self.mutate_catalog(drops).await?;

        Ok(())
    }

    /// Drop one or more connections.
    pub async fn drop_connections(&mut self, plan: DropConnections) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            let (_, schema, name) = self.resolve_object_reference(name.into())?;
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
        let mut drops = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            drops.push(Mutation::DropSchema(service::DropSchema {
                name,
                if_exists: plan.if_exists,
            }));
        }

        self.mutate_catalog(drops).await?;

        Ok(())
    }

    pub async fn drop_database(&mut self, plan: DropDatabase) -> Result<()> {
        self.mutate_catalog([Mutation::DropDatabase(service::DropDatabase {
            name: plan.name,
            if_exists: plan.if_exists,
        })])
        .await?;
        Ok(())
    }

    /// Get a connection entry from the catalog by name.
    pub fn get_connection_by_name(&self, name: &str) -> Result<&ConnectionEntry> {
        let (_, schema, name) = self.resolve_object_reference(name.into())?;
        let ent = self
            .metastore_catalog
            .resolve_entry(DEFAULT_CATALOG, &schema, &name)
            .ok_or(ExecError::MissingConnectionByName { schema, name })?;

        match ent {
            catalog::CatalogEntry::Connection(ent) => Ok(ent),
            other => Err(ExecError::UnexpectedEntryType {
                got: other.entry_type(),
                want: catalog::EntryType::Connection,
            }),
        }
    }

    /// Get a connection entry from the catalog by id.
    pub fn get_connection_by_oid(&self, oid: u32) -> Result<&ConnectionEntry> {
        let ent = self
            .metastore_catalog
            .get_by_oid(oid)
            .ok_or(ExecError::MissingConnectionByOid { oid })?;

        match ent {
            catalog::CatalogEntry::Connection(ent) => Ok(ent),
            other => Err(ExecError::UnexpectedEntryType {
                got: other.entry_type(),
                want: catalog::EntryType::Connection,
            }),
        }
    }

    /// Get an ssh tunnel access info from the catalog, along with its id.
    ///
    /// Errors if a connection doesn't exist with the given name, or if the
    /// connection isn't an ssh connection.
    pub fn get_ssh_tunnel_access_by_name(
        &self,
        name: &str,
    ) -> Result<(u32, SshTunnelAccess), ExecError> {
        let conn = self.get_connection_by_name(name)?;
        let ssh_opts = conn
            .try_get_ssh_options()
            .ok_or(ExecError::InvalidConnectionType {
                expected: ConnectionOptions::SSH,
                got: conn.options.as_str(),
            })?;

        let access = ssh_opts.try_into()?;
        Ok((conn.meta.id, access))
    }

    /// Get an ssh tunnel access info from the catalog by oid..
    ///
    /// Errors if a connection doesn't exist with the given name, or if the
    /// connection isn't an ssh connection.
    pub fn get_ssh_tunnel_access_by_oid(&self, oid: u32) -> Result<SshTunnelAccess, ExecError> {
        let conn = self.get_connection_by_oid(oid)?;
        let ssh_opts = conn
            .try_get_ssh_options()
            .ok_or(ExecError::InvalidConnectionType {
                expected: ConnectionOptions::SSH,
                got: conn.options.as_str(),
            })?;

        let access = ssh_opts.try_into()?;
        Ok(access)
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
            let df_plan = plan.try_into_datafusion_plan()?;

            Ok(df_plan)
        })
    }

    /// Attempt to apply mutations to the catalog.
    async fn mutate_catalog(
        &mut self,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<()> {
        // Note that when we have transactions, these shouldn't be sent until
        // commit.
        let state = self
            .metastore
            .try_mutate(
                self.metastore_catalog.version(),
                mutations.into_iter().collect(),
            )
            .await?;
        self.metastore_catalog.swap_state(state);
        Ok(())
    }

    /// Resolves a reference for an object that existing inside a schema.
    fn resolve_object_reference(&self, name: Cow<'_, str>) -> Result<(String, String, String)> {
        let reference = TableReference::from(name.as_ref());
        match reference {
            TableReference::Bare { .. } => {
                let schema = self.first_nonimplicit_schema()?.to_string();
                Ok((DEFAULT_CATALOG.to_string(), schema, name.to_string()))
            }
            TableReference::Partial { schema, table } => Ok((
                DEFAULT_CATALOG.to_string(),
                schema.to_string(),
                table.to_string(),
            )),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => Ok((catalog.to_string(), schema.to_string(), table.to_string())),
        }
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
            arrow_fields: s.fields.iter(),
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
    arrow_fields: slice::Iter<'a, ArrowField>,
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

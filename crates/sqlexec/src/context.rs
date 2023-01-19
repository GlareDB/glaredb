use crate::dispatch::SessionDispatcher;
use crate::errors::{internal, ExecError, Result};
use crate::functions::BuiltinScalarFunction;
use crate::logical_plan::*;
use crate::metastore::SupervisorClient;
use crate::parser::{CustomParser, StatementWithExtensions};
use crate::planner::SessionPlanner;
use crate::vars::SessionVars;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{SessionConfig, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use metastore::session::SessionCatalog;
use metastore::types::catalog::{self, ConnectionEntry};
use metastore::types::service::{self, Mutation};
use pgrepr::format::Format;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

/// Context for a session used during execution.
///
/// The context generally does not have to worry about anything external to the
/// database. Its source of truth is the in-memory catalog.
pub struct SessionContext {
    conn_id: Uuid,
    /// Database catalog.
    metastore_catalog: SessionCatalog,
    metastore: SupervisorClient,
    /// Session variables.
    vars: SessionVars,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Bound portals.
    portals: HashMap<String, Portal>,
    /// Datafusion session state used for planning and execution.
    ///
    /// This session state makes a ton of assumptions, try to keep usage of it
    /// to a minimum and ensure interactions with this are well-defined.
    df_state: SessionState,
}

impl SessionContext {
    /// Create a new session context with the given catalog.
    pub fn new(
        conn_id: Uuid,
        catalog: SessionCatalog,
        metastore: SupervisorClient,
    ) -> SessionContext {
        // TODO: Pass in datafusion runtime env.

        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let config = SessionConfig::default()
            .create_default_catalog_and_schema(false)
            .with_information_schema(false);

        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);

        // Note that we do not replace the default catalog list on the state. We
        // should never be referencing it during planning or execution.
        //
        // Ideally we can reduce the need to rely on datafusion's session state
        // as much as possible. It makes way too many assumptions.

        SessionContext {
            conn_id,
            metastore_catalog: catalog,
            metastore,
            vars: SessionVars::default(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            df_state: state,
        }
    }

    /// Get the connection id for this session.
    pub fn conn_id(&self) -> Uuid {
        self.conn_id
    }

    /// Create a table.
    pub fn create_table(&self, _plan: CreateTable) -> Result<()> {
        Err(ExecError::UnsupportedFeature("CREATE TABLE"))
    }

    pub async fn create_external_table(&mut self, plan: CreateExternalTable) -> Result<()> {
        let (schema, name) = self.resolve_object_reference(plan.table_name)?;
        self.mutate_catalog([Mutation::CreateExternalTable(
            service::CreateExternalTable {
                schema,
                name,
                connection_id: plan.connection_id,
                options: plan.table_options,
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

    pub async fn create_connection(&mut self, plan: CreateConnection) -> Result<()> {
        let (schema, name) = self.resolve_object_reference(plan.connection_name)?;
        self.mutate_catalog([Mutation::CreateConnection(service::CreateConnection {
            schema,
            name,
            options: plan.options,
        })])
        .await?;

        Ok(())
    }

    pub async fn create_view(&mut self, plan: CreateView) -> Result<()> {
        let (schema, name) = self.resolve_object_reference(plan.view_name)?;
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
            let (schema, name) = self.resolve_object_reference(name)?;
            drops.push(Mutation::DropObject(service::DropObject { schema, name }));
        }

        self.mutate_catalog(drops).await?;

        Ok(())
    }

    /// Drop one or more schemas.
    pub async fn drop_schemas(&mut self, plan: DropSchemas) -> Result<()> {
        let mut drops = Vec::with_capacity(plan.names.len());
        for name in plan.names {
            drops.push(Mutation::DropSchema(service::DropSchema { name }));
        }

        self.mutate_catalog(drops).await?;

        Ok(())
    }

    /// Get a connection entry from the catalog.
    pub fn get_connection(&self, name: String) -> Result<&ConnectionEntry> {
        let (schema, name) = self.resolve_object_reference(name)?;
        let ent = self
            .metastore_catalog
            .resolve_entry(&schema, &name)
            .ok_or(ExecError::MissingConnection { schema, name })?;

        match ent {
            catalog::CatalogEntry::Connection(ent) => Ok(ent),
            other => Err(ExecError::UnexpectedEntryType {
                got: other.entry_type(),
                want: catalog::EntryType::Connection,
            }),
        }
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
        params: Vec<i32>,
    ) -> Result<()> {
        // Swap out cached catalog if a newer one was fetched.
        //
        // Note that when we have transactions, we should move this to only
        // swapping states between transactions.
        if self.metastore.version_hint() != self.metastore_catalog.version() {
            debug!(version = %self.metastore_catalog.version(), "swapping catalog state for session");
            let new_state = self.metastore.get_cached_state().await?;
            self.metastore_catalog.swap_state(new_state);
        }

        if !params.is_empty() {
            return Err(ExecError::UnsupportedFeature(
                "prepared statements with parameters",
            ));
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

        let stmt = PreparedStatement::new(stmt, self)?;
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

        let stmt = match self.prepared.get(stmt_name) {
            Some(prepared) => prepared.clone(),
            None => return Err(ExecError::UnknownPreparedStatement(stmt_name.to_string())),
        };

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
    pub(crate) fn late_view_plan(
        &self,
        sql: &str,
    ) -> Result<datafusion::logical_expr::LogicalPlan, ExecError> {
        let mut statements = CustomParser::parse_sql(sql)?;
        if statements.len() != 1 {
            return Err(ExecError::ExpectedExactlyOneStatement(
                statements.into_iter().collect(),
            ));
        }

        let planner = SessionPlanner::new(self);
        let plan = planner.plan_ast(statements.pop_front().unwrap())?;
        let df_plan = plan.try_into_datafusion_plan()?;

        Ok(df_plan)
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
    fn resolve_object_reference(&self, name: String) -> Result<(String, String)> {
        let reference = TableReference::from(name.as_str());
        match reference {
            TableReference::Bare { .. } => {
                let schema = self.first_schema()?.to_string();
                Ok((schema, name))
            }
            TableReference::Partial { schema, table }
            | TableReference::Full { schema, table, .. } => {
                Ok((schema.to_string(), table.to_string()))
            }
        }
    }

    /// Iterate over all values in the search path.
    fn search_path_iter(&self) -> impl Iterator<Item = &str> {
        self.vars.search_path.value().iter().map(|s| s.as_str())
    }

    fn first_schema(&self) -> Result<&str> {
        self.search_path_iter()
            .next()
            .ok_or(ExecError::EmptySearchPath)
    }
}

/// Adapter for datafusion planning.
///
/// NOTE: While `ContextProvider` is for _logical_ planning, DataFusion will
/// actually try to downcast the `TableSource` to a `TableProvider` during
/// physical planning. This only works with `DefaultTableSource` which is what
/// this adapter uses.
pub struct ContextProviderAdapter<'a> {
    pub context: &'a SessionContext,
}

impl<'a> ContextProvider for ContextProviderAdapter<'a> {
    fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        let dispatcher = SessionDispatcher::new(self.context);
        match name {
            TableReference::Bare { table } => {
                for schema in self.context.search_path_iter() {
                    match dispatcher.dispatch_access(schema, table) {
                        Ok(table) => return Ok(Arc::new(DefaultTableSource::new(table))),
                        Err(e) if e.should_try_next_schema() => (), // Continue to next schema in search path.
                        Err(e) => {
                            return Err(DataFusionError::Plan(format!(
                                "failed to dispatch bare table: {}",
                                e
                            )))
                        }
                    }
                }
                Err(DataFusionError::Plan(format!(
                    "failed to resolve bare table: {}",
                    table
                )))
            }
            TableReference::Full { schema, table, .. }
            | TableReference::Partial { schema, table } => {
                let table = dispatcher.dispatch_access(schema, table).map_err(|e| {
                    DataFusionError::Plan(format!("failed dispatch for qualified table: {}", e))
                })?;
                Ok(Arc::new(DefaultTableSource::new(table)))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        BuiltinScalarFunction::try_from_name(name)
            .map(|f| Arc::new(f.build_scalar_udf(self.context)))
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_config_option(&self, _variable: &str) -> Option<ScalarValue> {
        None
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PreparedStatement {
    pub(crate) stmt: Option<StatementWithExtensions>,
    /// The logical plan for the statement. Is `Some` if the statement is
    /// `Some`.
    pub(crate) plan: Option<LogicalPlan>,
    /// The output schema of the statement if it produces an output.
    pub(crate) output_schema: Option<ArrowSchema>,
}

impl PreparedStatement {
    /// Create and plan a new prepared statement.
    // TODO: Not sure if we want to delay the planning portion.
    fn new(mut stmt: Option<StatementWithExtensions>, ctx: &SessionContext) -> Result<Self> {
        if let Some(inner) = stmt.take() {
            // Go ahead and plan using the session context.
            let planner = SessionPlanner::new(ctx);
            let plan = planner.plan_ast(inner.clone())?;
            let schema = plan.output_schema();

            Ok(PreparedStatement {
                stmt: Some(inner),
                plan: Some(plan),
                output_schema: schema,
            })
        } else {
            // No statement to plan.
            Ok(PreparedStatement {
                stmt: None,
                plan: None,
                output_schema: None,
            })
        }
    }

    /// Get the ouput schema for this statement.
    pub fn output_schema(&self) -> Option<&ArrowSchema> {
        self.output_schema.as_ref()
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
    /// Get the output schema for this portal.
    pub fn output_schema(&self) -> Option<&ArrowSchema> {
        self.stmt.output_schema()
    }
}

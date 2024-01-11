use crate::distexec::scheduler::Scheduler;
use crate::environment::EnvironmentReader;
use crate::errors::{internal, ExecError, Result};
use crate::parser::StatementWithExtensions;
use crate::planner::logical_plan::*;
use crate::planner::session_planner::SessionPlanner;
use crate::remote::client::{RemoteClient, RemoteSessionClient};
use catalog::mutator::CatalogMutator;
use catalog::session_catalog::SessionCatalog;
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::common::SchemaReference;
use datafusion::execution::context::{
    SessionConfig, SessionContext as DfSessionContext, SessionState, TaskContext,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_ext::session_metrics::SessionMetricsHandler;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use pgrepr::format::Format;
use pgrepr::types::arrow_to_pg_type;

use datafusion::variable::VarType;
use protogen::rpcsrv::types::service::{
    InitializeSessionRequest, InitializeSessionRequestFromClient,
};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use std::collections::HashMap;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;
use tokio_postgres::types::Type as PgType;

use datafusion_ext::runtime::group_pull_up::RuntimeGroupPullUp;
use uuid::Uuid;

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

/// Context for a session used local execution and planning.
///
/// A session may be attached to a remote context in which parts of a query can
/// be executed remotely. When attaching a remote context, the query planner is
/// switched out to one that's able to generate plans to send to remote
/// contexts.
pub struct LocalSessionContext {
    /// The id of the database
    database_id: Uuid,
    /// The execution client for remote sessions.
    exec_client: Option<RemoteSessionClient>,
    /// Database catalog.
    catalog: SessionCatalog,
    /// Native tables.
    tables: NativeTableStorage,
    /// Prepared statements.
    prepared: HashMap<String, PreparedStatement>,
    /// Bound portals.
    portals: HashMap<String, Portal>,
    /// Handler to push metrics into tracker.
    metrics_handler: SessionMetricsHandler,
    /// Datafusion session context used for planning and execution.
    df_ctx: DfSessionContext,
    /// Read tables from the environment.
    env_reader: Option<Box<dyn EnvironmentReader>>,
    /// Task scheduler.
    task_scheduler: Scheduler,
}

impl LocalSessionContext {
    /// Create a new session context with the given catalog.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        metrics_handler: SessionMetricsHandler,
        spill_path: Option<PathBuf>,
        task_scheduler: Scheduler,
    ) -> Result<LocalSessionContext> {
        let database_id = vars.database_id();
        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(&vars);

        let mut conf: SessionConfig = opts.into();

        // TODO: Can we remove the temp catalog here? It's pretty disgusting,
        // but it's needed for the create temp table execution plan.
        conf = conf
            .with_extension(Arc::new(catalog_mutator))
            .with_extension(Arc::new(native_tables.clone()))
            .with_extension(Arc::new(catalog.get_temp_catalog().clone()))
            .with_extension(Arc::new(task_scheduler.clone()));

        let state = SessionState::new_with_config_rt(conf, Arc::new(runtime))
            .add_physical_optimizer_rule(Arc::new(RuntimeGroupPullUp {}));

        let df_ctx = DfSessionContext::new_with_state(state);
        df_ctx.register_variable(datafusion::variable::VarType::UserDefined, Arc::new(vars));

        Ok(LocalSessionContext {
            database_id,
            exec_client: None,
            catalog,
            tables: native_tables,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            metrics_handler,
            df_ctx,
            env_reader: None,
            task_scheduler,
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

        // I gave up and just decided to create a new state. Datafusion isn't
        // amenable to mutating an existing context.
        //
        // The main difference here is creating a new catalog mutator without a
        // metastore client to "detach" ourselves from the metastore we're
        // connected to (likely running in-memory).
        let vars = self
            .get_session_vars()
            .with_database_id(client.database_id(), VarType::System);
        let runtime = self.df_ctx.runtime_env();
        let opts = new_datafusion_session_config_opts(&vars);
        let mut conf: SessionConfig = opts.into();
        // TODO: Just like above, the temp catalog here is kinda gross.
        conf = conf
            .with_extension(Arc::new(CatalogMutator::empty()))
            .with_extension(Arc::new(self.get_native_tables().clone()))
            .with_extension(Arc::new(catalog.get_temp_catalog().clone()));

        let state = SessionState::new_with_config_rt(conf, runtime)
            .add_physical_optimizer_rule(Arc::new(RuntimeGroupPullUp {}));

        let df_ctx = DfSessionContext::new_with_state(state);
        df_ctx.register_variable(datafusion::variable::VarType::UserDefined, Arc::new(vars));

        self.exec_client = Some(client.clone());
        self.df_ctx = df_ctx;
        self.catalog = catalog;

        Ok(())
    }

    pub fn register_env_reader(&mut self, env_reader: Box<dyn EnvironmentReader>) {
        self.env_reader = Some(env_reader);
    }

    pub fn get_env_reader(&self) -> Option<&dyn EnvironmentReader> {
        self.env_reader.as_deref()
    }

    pub fn get_metrics_handler(&self) -> SessionMetricsHandler {
        self.metrics_handler.clone()
    }

    pub fn get_database_id(&self) -> Uuid {
        self.database_id
    }

    pub fn get_native_tables(&self) -> &NativeTableStorage {
        &self.tables
    }

    pub fn get_task_scheduler(&self) -> Scheduler {
        self.task_scheduler.clone()
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

    /// Get a mutable reference to the session catalog.
    pub fn get_session_catalog_mut(&mut self) -> &mut SessionCatalog {
        &mut self.catalog
    }

    pub async fn maybe_refresh_state(&mut self) -> Result<()> {
        let mutator = self.catalog_mutator();
        let client = mutator.get_metastore_client();
        self.catalog
            .maybe_refresh_state(client, self.get_session_vars().force_catalog_refresh())
            .await
            .map_err(ExecError::from)
    }

    /// Create a prepared statement.
    pub async fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        _params: Vec<i32>, // TODO: We can use these for providing types for parameters.
    ) -> Result<()> {
        // Refresh the cached catalog state if necessary
        self.maybe_refresh_state().await?;

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
    pub fn resolve_schema_ref(&self, r: SchemaReference<'_>) -> OwnedFullSchemaReference {
        match r {
            SchemaReference::Bare { schema } => FullSchemaReference {
                database: DEFAULT_CATALOG.into(),
                schema: schema.into_owned().into(),
            },
            SchemaReference::Full { catalog, schema } => FullSchemaReference {
                database: catalog.into_owned().into(),
                schema: schema.into_owned().into(),
            },
        }
    }

    pub fn resolve_table_ref(&self, r: TableReference<'_>) -> Result<OwnedFullObjectReference> {
        let r = match r {
            TableReference::Bare { table } => {
                let schema = self.first_nonimplicit_schema()?;
                FullObjectReference {
                    database: DEFAULT_CATALOG.into(),
                    schema: schema.into(),
                    name: table.into_owned().into(),
                }
            }
            TableReference::Partial { schema, table } => FullObjectReference {
                database: DEFAULT_CATALOG.into(),
                schema: schema.into_owned().into(),
                name: table.into_owned().into(),
            },
            TableReference::Full {
                catalog,
                schema,
                table,
            } => FullObjectReference {
                database: catalog.into_owned().into(),
                schema: schema.into_owned().into(),
                name: table.into_owned().into(),
            },
        };
        Ok(r)
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
        self.stmt.output_fields().map(|mut output_fields| {
            output_fields.result_formats = Some(self.result_formats.as_slice().iter());
            output_fields
        })
    }
    pub fn logical_plan(&self) -> Option<&LogicalPlan> {
        self.stmt.plan.as_ref()
    }
    pub fn input_paramaters(&self) -> Option<&HashMap<String, Option<(PgType, DataType)>>> {
        self.stmt.input_paramaters()
    }
    pub fn output_schema(&self) -> Option<&ArrowSchema> {
        self.stmt.output_schema.as_ref()
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

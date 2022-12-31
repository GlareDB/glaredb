use crate::errors::{internal, ExecError, Result};
use crate::functions::BuiltinScalarFunction;
use crate::logical_plan::*;
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
use dfutil::convert::from_df_field;
use jsoncat::access::AccessMethod;
use jsoncat::catalog::{Catalog, DropEntry, EntryType};
use jsoncat::dispatch::{CatalogDispatcher, LatePlanner};
use jsoncat::entry::schema::SchemaEntry;
use jsoncat::entry::table::{ColumnDefinition, TableEntry};
use jsoncat::transaction::StubCatalogContext;
use pgrepr::format::Format;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Context for a session used during execution.
///
/// The context generally does not have to worry about anything external to the
/// database. Its source of truth is the in-memory catalog.
pub struct SessionContext {
    id: Uuid,
    /// Database catalog.
    catalog: Arc<Catalog>,
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
    pub fn new(catalog: Arc<Catalog>, id: Uuid) -> SessionContext {
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
            id,
            catalog,
            vars: SessionVars::default(),
            prepared: HashMap::new(),
            portals: HashMap::new(),
            df_state: state,
        }
    }

    /// Get the id for this session.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Create a table.
    pub fn create_table(&self, plan: CreateTable) -> Result<()> {
        let (schema, name) = self.resolve_table_reference(plan.table_name)?;
        let ent = TableEntry {
            schema,
            name,
            access: AccessMethod::Unknown,
            columns: plan
                .columns
                .into_iter()
                .map(|f| ColumnDefinition::from(&from_df_field(f)))
                .collect(),
        };

        self.catalog.create_table(&StubCatalogContext, ent)?;
        Ok(())
    }

    pub fn create_external_table(&self, plan: CreateExternalTable) -> Result<()> {
        let (schema, name) = self.resolve_table_reference(plan.table_name)?;
        let ent = TableEntry {
            schema,
            name,
            access: plan.access,
            columns: Vec::new(),
        };

        self.catalog.create_table(&StubCatalogContext, ent)?;
        Ok(())
    }

    /// Create a schema.
    pub fn create_schema(&self, plan: CreateSchema) -> Result<()> {
        let ent = SchemaEntry {
            schema: plan.schema_name,
            internal: false,
        };

        self.catalog.create_schema(&StubCatalogContext, ent)?;
        Ok(())
    }

    /// Drop one or more tables.
    pub fn drop_tables(&self, plan: DropTables) -> Result<()> {
        for name in plan.names {
            let (schema, name) = self.resolve_table_reference(name)?;
            let ent = DropEntry {
                typ: EntryType::Table,
                schema,
                name,
            };
            self.catalog.drop_entry(&StubCatalogContext, ent)?;
        }
        Ok(())
    }

    /// Drop one or more schemas.
    pub fn drop_schemas(&self, plan: DropSchemas) -> Result<()> {
        for name in plan.names {
            let ent = DropEntry {
                typ: EntryType::Schema,
                schema: name.clone(),
                name,
            };
            self.catalog.drop_entry(&StubCatalogContext, ent)?;
        }
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

    /// Get a reference to the catalog.
    pub fn get_catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Create a prepared statement.
    pub fn prepare_statement(
        &mut self,
        name: String,
        stmt: Option<StatementWithExtensions>,
        params: Vec<i32>,
    ) -> Result<()> {
        if params.len() > 0 {
            return Err(ExecError::UnsupportedFeature(
                "prepared statements with parameters",
            ));
        }

        // Unnamed (empty string) prepared statements can be overwritten
        // whenever. Named prepared statements must be explicitly removed before
        // being used again.
        if name != "" && self.prepared.contains_key(&name) {
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
        if portal_name != "" && self.portals.contains_key(&portal_name) {
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

    /// Get a portal.
    pub fn get_portal(&self, name: &str) -> Result<&Portal> {
        self.portals
            .get(name)
            .ok_or_else(|| ExecError::UnknownPortal(name.to_string()))
    }

    /// Get a prepared statement.
    pub fn get_prepared_statement(&self, name: &str) -> Result<&PreparedStatement> {
        self.prepared
            .get(name)
            .ok_or_else(|| ExecError::UnknownPreparedStatement(name.to_string()))
    }

    /// Get a datafusion task context to use for physical plan execution.
    pub(crate) fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(&self.df_state))
    }

    /// Get a datafusion session state.
    pub(crate) fn get_df_state(&self) -> &SessionState {
        &self.df_state
    }

    /// Resolves a table reference for creating tables and views.
    fn resolve_table_reference(&self, name: String) -> Result<(String, String)> {
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

impl LatePlanner for SessionContext {
    type Error = ExecError;

    fn plan_sql(&self, sql: &str) -> Result<datafusion::logical_expr::LogicalPlan, ExecError> {
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
}

/// Adapter for datafusion planning.
pub struct ContextProviderAdapter<'a> {
    pub context: &'a SessionContext,
}

impl<'a> ContextProvider for ContextProviderAdapter<'a> {
    fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        // NOTE: While `ContextProvider` is for _logical_ planning, DataFusion
        // will actually try to downcast the `TableSource` to a `TableProvider`
        // during physical planning. This only works with `DefaultTableSource`.

        let dispatcher = CatalogDispatcher::new(&StubCatalogContext, &self.context.catalog);
        match name {
            TableReference::Bare { table } => {
                for schema in self.context.search_path_iter() {
                    let opt = dispatcher
                        .dispatch_access(self.context, schema, table)
                        .map_err(|e| DataFusionError::Plan(format!("failed dispatch: {}", e)))?;
                    if let Some(provider) = opt {
                        return Ok(Arc::new(DefaultTableSource::new(provider)));
                    }
                }
                Err(DataFusionError::Plan(format!(
                    "failed to resolve bare table: {}",
                    table
                )))
            }
            TableReference::Full { schema, table, .. }
            | TableReference::Partial { schema, table } => {
                let opt = dispatcher
                    .dispatch_access(self.context, schema, table)
                    .map_err(|e| DataFusionError::Plan(format!("failed dispatch: {}", e)))?;
                if let Some(provider) = opt {
                    return Ok(Arc::new(DefaultTableSource::new(provider)));
                }

                Err(DataFusionError::Plan(format!(
                    "failed to resolve qualified table; schema: {}, table: {}",
                    schema, table
                )))
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

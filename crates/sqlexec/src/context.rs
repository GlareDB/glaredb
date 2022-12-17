use crate::errors::{ExecError, Result};
use crate::executor::SqlParser;
use crate::functions::BuiltinScalarFunction;
use crate::logical_plan::*;
use crate::planner::SessionPlanner;
use crate::searchpath::SearchPath;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
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
use jsoncat::transaction::{Context, StubCatalogContext};
use std::sync::Arc;

/// Context for a session used during execution.
pub struct SessionContext {
    /// Database catalog.
    catalog: Arc<Catalog>,
    /// This session's search path. Used to resolve database objects.
    search_path: SearchPath,
    /// Datafusion session state used for planning and execution.
    ///
    /// This session state makes a ton of assumptions, try to keep usage of it
    /// to a minimum and ensure interactions with this are well-defined.
    df_state: SessionState,
}

impl SessionContext {
    pub fn new(catalog: Arc<Catalog>) -> SessionContext {
        // TODO: Pass in datafusion runtime env.

        // NOTE: We handle catalog/schema defaults and information schemas
        // ourselves.
        let config = SessionConfig::default()
            .create_default_catalog_and_schema(false)
            .with_information_schema(false);

        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::with_config_rt(config, runtime);

        // // TODO: We don't need to create the builtin functions everytime.
        // let funcs = vec![version_func()];
        // for func in funcs {
        //     state.scalar_functions.insert(func.name.clone(), func);
        // }

        // Note that we do not replace the default catalog list on the state. We
        // should never be referencing it during planning or execution.
        //
        // Ideally we can reduce the need to rely on datafusion's session state
        // as much as possible. It makes way too many assumptions.

        SessionContext {
            catalog,
            search_path: SearchPath::new(),
            df_state: state,
        }
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

    /// Set the search path.
    pub fn try_set_search_path(&mut self, text: &str) -> Result<()> {
        self.search_path
            .try_set(&StubCatalogContext, &self.catalog, text)
    }

    /// Get a reference to the catalog.
    pub fn get_catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub(crate) fn get_search_path(&self) -> &SearchPath {
        &self.search_path
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

    fn first_schema(&self) -> Result<&str> {
        self.search_path
            .iter()
            .next()
            .ok_or(ExecError::EmptySearchPath)
    }
}

impl LatePlanner for SessionContext {
    type Error = ExecError;

    fn plan_sql(&self, sql: &str) -> Result<datafusion::logical_expr::LogicalPlan, ExecError> {
        let mut statements = SqlParser::parse(sql)?;
        if statements.len() != 1 {
            return Err(ExecError::ExpectedExactlyOneStatement(statements));
        }

        let planner = SessionPlanner::new(self);
        let plan = planner.plan_ast(statements.pop().unwrap())?;
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
                for schema in self.context.search_path.iter() {
                    let opt = dispatcher
                        .dispatch_access(self.context, schema, table)
                        .map_err(|e| DataFusionError::Plan(format!("failed dispatch: {}", e)))?;
                    if let Some(provider) = opt {
                        return Ok(Arc::new(DefaultTableSource::new(provider)));
                    }
                }
                Err(DataFusionError::Plan(format!(
                    "failed to resolve bare table: {}, {}",
                    table, self.context.search_path
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
                    "failed to resolve qualified table; schema: {}, table: {}, {}",
                    schema, table, self.context.search_path
                )))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        BuiltinScalarFunction::try_from_name(name)
            .map(|f| Arc::new(f.build_scalar_udf(self.context)))
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_config_option(&self, variable: &str) -> Option<ScalarValue> {
        None
    }
}

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
use jsoncat::adapter::CatalogProviderAdapter;
use jsoncat::catalog::Catalog;
use jsoncat::transaction::Context;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StubCatalogContext;

impl Context for StubCatalogContext {}

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

    /// Get a datafusion task context to use for physical plan execution.
    pub fn task_context(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(&self.df_state))
    }

    /// Get a datafusion session state.
    pub fn get_df_state(&self) -> &SessionState {
        &self.df_state
    }
}

/// Adapter for datafusion planning.
pub struct ContextProviderAdapter<'a> {
    pub context: &'a SessionContext,
}

impl<'a> ContextProvider for ContextProviderAdapter<'a> {
    fn get_table_provider(&self, name: TableReference) -> DataFusionResult<Arc<dyn TableSource>> {
        let adapter = CatalogProviderAdapter::new(StubCatalogContext, self.context.catalog.clone());
        match name {
            TableReference::Bare { table } => {
                for schema in self.context.search_path.iter() {
                    if let Some(schema) = adapter.schema(schema) {
                        if let Some(table) = schema.table(table) {
                            return Ok(Arc::new(DefaultTableSource::new(table)));
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
                let schema = adapter.schema(schema).ok_or_else(|| {
                    DataFusionError::Plan(format!("failed to resolve schema: {}", schema))
                })?;
                let table = schema.table(table).ok_or_else(|| {
                    DataFusionError::Plan(format!("failed to resolve table: {}", table))
                })?;
                Ok(Arc::new(DefaultTableSource::new(table)))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        None
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

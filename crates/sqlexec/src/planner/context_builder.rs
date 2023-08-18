use crate::context::SessionContext;
use crate::errors::ExecError;
use crate::functions::BuiltinScalarFunction;
use crate::functions::PgFunctionBuilder;
use crate::planner::dispatch::SessionDispatcher;
use crate::planner::errors::PlanError;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::OwnedTableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::AggregateUDF;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::TableSource;
use datafusion::sql::TableReference;
use datafusion_ext::functions::TableFunc;
use datafusion_ext::functions::TableFuncContextProvider;
use datafusion_ext::planner::AsyncContextProvider;
use datafusion_ext::vars::SessionVars;
use protogen::metastore::types::catalog::{CatalogEntry, CredentialsEntry, DatabaseEntry};
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

/// Partial context provider with table providers required to fulfill a single
/// query.
///
/// NOTE: While `ContextProvider` is for _logical_ planning, DataFusion will
/// actually try to downcast the `TableSource` to a `TableProvider` during
/// physical planning. This only works with `DefaultTableSource` which is what
/// this adapter uses.
pub struct PartialContextProvider<'a> {
    providers: HashMap<OwnedTableReference, Arc<dyn TableProvider>>,
    state: &'a SessionState,
    ctx: &'a SessionContext,
}

impl<'a> PartialContextProvider<'a> {
    pub fn new(ctx: &'a SessionContext, state: &'a SessionState) -> Result<Self, PlanError> {
        Ok(Self {
            providers: HashMap::new(),
            state,
            ctx,
        })
    }

    /// Find a table provider the given reference, taking into account the
    /// session's search path.
    async fn table_for_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        if let Some(mut client) = self.ctx.exec_client() {
            let provider = client
                .dispatch_access(reference.to_owned_reference())
                .await?;
            return Ok(Arc::new(provider));
        }

        let dispatcher = SessionDispatcher::new(self.ctx);
        match &reference {
            TableReference::Bare { table } => {
                for schema in self.ctx.implicit_search_paths() {
                    // TODO
                    match dispatcher
                        .dispatch_access(DEFAULT_CATALOG, &schema, table)
                        .await
                    {
                        Ok(table) => return Ok(table),
                        Err(e) if e.should_try_next_schema() => (), // Continue to next schema in search path.
                        Err(e) => {
                            return Err(PlanError::FailedToCreateTableProvider {
                                reference: reference.to_string(),
                                e,
                            });
                        }
                    }
                }

                // Try to read from the environment if we fail to find the table
                // in the search path.
                //
                // TODO: We'll want to figure out how we want to handle
                // shadowing/precedence.
                if let Some(reader) = self.ctx.get_env_reader() {
                    if let Some(table) = reader
                        .resolve_table(table)
                        .map_err(|e| ExecError::EnvironmentTableRead(e))?
                    {
                        return Ok(table);
                    }
                }

                Err(PlanError::FailedToFindTableForReference {
                    reference: reference.to_string(),
                })
            }
            TableReference::Partial { schema, table } => {
                // TODO
                let table = dispatcher
                    .dispatch_access(DEFAULT_CATALOG, schema, table)
                    .await?;
                Ok(table)
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                let table = dispatcher.dispatch_access(catalog, schema, table).await?;
                Ok(table)
            }
        }
    }

    async fn resolve_entry(
        &self,
        schema: impl AsRef<str>,
        name: &str,
    ) -> Option<Arc<dyn TableFunc>> {
        let catalog = self.ctx.get_session_catalog();

        match catalog
            .resolve_entry(DEFAULT_CATALOG, schema.as_ref(), name)
            .await
        {
            Some(CatalogEntry::Function(func)) => {
                if func.meta.builtin {
                    if let Some(func_impl) = BUILTIN_TABLE_FUNCS.find_function(&func.meta.name) {
                        Some(func_impl)
                    } else {
                        // This can happen if we're in the middle of
                        // a deploy and builtin functions were
                        // added/removed.
                        error!(name = %func.meta.name, "missing builtin function impl");
                        None
                    }
                } else {
                    // We only have builtin functions right now.
                    None
                }
            }
            _ => None,
        }
    }
    /// Find a table function with the given reference, taking into account the
    /// session's search path.
    async fn table_function_for_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Option<Arc<dyn TableFunc>> {
        match &reference {
            TableReference::Bare { table } => {
                for schema in self.ctx.implicit_search_paths() {
                    if let Some(func_impl) = self.resolve_entry(&schema, table).await {
                        return Some(func_impl);
                    }
                }
                None
            }
            TableReference::Partial { schema, table } => self.resolve_entry(schema, table).await,
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog == DEFAULT_CATALOG {
                    self.resolve_entry(schema, table).await
                } else {
                    None
                }
            }
        }
    }

    /// Get the table provider from the table reference.
    pub async fn table_provider(
        &mut self,
        name: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        let provider = match self.providers.get(&name) {
            Some(provider) => provider.clone(),
            None => {
                let provider = self
                    .table_for_reference(TableReference::from(&name))
                    .await?;
                self.providers.insert(name, provider.clone());
                provider
            }
        };

        Ok(provider)
    }
}

#[async_trait]
impl<'a> AsyncContextProvider for PartialContextProvider<'a> {
    type TableFuncContextProvider = TableFnCtxProvider<'a>;

    async fn get_table_provider(
        &mut self,
        name: TableReference<'_>,
    ) -> DataFusionResult<Arc<dyn TableSource>> {
        let provider = self
            .table_provider(name.to_owned_reference())
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!("Unable to fetch table provider for '{name}': {e}"))
            })?;
        Ok(Arc::new(DefaultTableSource::new(provider)))
    }

    async fn get_function_meta(&mut self, name: &str) -> Option<Arc<ScalarUDF>> {
        // TODO: We can build these async too.
        match BuiltinScalarFunction::try_from_name(name)
            .map(|f| Arc::new(f.build_scalar_udf(self.ctx)))
        {
            Some(func) => Some(func),
            None => PgFunctionBuilder::try_from_name(self.ctx, name, true),
        }
    }

    async fn get_variable_type(&mut self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    async fn get_aggregate_meta(&mut self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    async fn get_table_func(&self, name: TableReference<'_>) -> Option<Arc<dyn TableFunc>> {
        self.table_function_for_reference(name).await
    }

    fn table_fn_ctx_provider(&self) -> Self::TableFuncContextProvider {
        Self::TableFuncContextProvider {
            ctx: self.ctx,
            state: self.state,
        }
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
}

pub struct TableFnCtxProvider<'a> {
    ctx: &'a SessionContext,
    state: &'a SessionState,
}

#[async_trait]
impl<'a> TableFuncContextProvider for TableFnCtxProvider<'a> {
    async fn get_database_entry(&self, name: &str) -> Option<DatabaseEntry> {
        self.ctx.get_session_catalog().resolve_database(name).await
    }

    async fn get_credentials_entry(&self, name: &str) -> Option<CredentialsEntry> {
        self.ctx
            .get_session_catalog()
            .resolve_credentials(name)
            .await
    }

    fn get_session_vars(&self) -> SessionVars {
        self.ctx.get_session_vars()
    }

    fn get_session_state(&self) -> &SessionState {
        self.state
    }
}

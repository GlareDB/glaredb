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
use datafusion::logical_expr::AggregateUDF;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::TableSource;
use datafusion::sql::TableReference;
use datafusion_planner::planner::AsyncContextProvider;
use metastore::builtins::DEFAULT_CATALOG;
use metastoreproto::types::catalog::CatalogEntry;
use metastoreproto::types::catalog::DatabaseEntry;
use sqlbuiltins::functions::TableFunc;
use sqlbuiltins::functions::TableFuncContextProvider;
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
    ctx: &'a SessionContext,
}

impl<'a> PartialContextProvider<'a> {
    pub fn new(ctx: &'a SessionContext) -> Result<Self, PlanError> {
        Ok(Self {
            providers: HashMap::new(),
            ctx,
        })
    }

    /// Find a table provider the given reference, taking into account the
    /// session's search path.
    async fn table_for_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        let dispatcher = SessionDispatcher::new(self.ctx);
        match &reference {
            TableReference::Bare { table } => {
                for schema in self.ctx.implicit_search_path_iter() {
                    // TODO
                    match dispatcher
                        .dispatch_access(DEFAULT_CATALOG, schema, table)
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

    /// Find a table function with the given reference, taking into account the
    /// session's search path.
    fn table_function_for_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Option<Arc<dyn TableFunc>> {
        let catalog = self.ctx.get_session_catalog();

        let resolve_func = |schema, name| {
            match catalog.resolve_entry(DEFAULT_CATALOG, schema, name) {
                Some(CatalogEntry::Function(func)) => {
                    if func.meta.builtin {
                        if let Some(func_impl) = BUILTIN_TABLE_FUNCS.find_function(&func.meta.name)
                        {
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
        };

        match &reference {
            TableReference::Bare { table } => {
                for schema in self.ctx.implicit_search_path_iter() {
                    if let Some(func_impl) = resolve_func(schema, table) {
                        return Some(func_impl);
                    }
                }
                None
            }
            TableReference::Partial { schema, table } => resolve_func(schema, table),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog == DEFAULT_CATALOG {
                    resolve_func(schema, table)
                } else {
                    None
                }
            }
        }
    }
    /// Get the table provider if available in the cache.
    pub fn table_provider(&self, name: TableReference<'_>) -> Option<Arc<dyn TableProvider>> {
        self.providers.get(&name).cloned()
    }
}

#[async_trait]
impl<'a> AsyncContextProvider for PartialContextProvider<'a> {
    type TableFuncContextProvider = TableFnCtxProvider<'a>;

    async fn get_table_provider(
        &mut self,
        name: TableReference<'_>,
    ) -> DataFusionResult<Arc<dyn TableSource>> {
        let name = name.to_owned_reference();

        let provider = match self.providers.get(&name) {
            Some(provider) => provider.clone(),
            None => {
                let provider = self
                    .table_for_reference(TableReference::from(&name))
                    .await
                    .map_err(|e| {
                        DataFusionError::Plan(format!(
                            "Unable to fetch table provider for '{name}': {e}"
                        ))
                    })?;
                self.providers.insert(name, provider.clone());
                provider
            }
        };

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

    fn get_table_func(&mut self, name: TableReference<'_>) -> Option<Arc<dyn TableFunc>> {
        self.table_function_for_reference(name)
    }

    fn table_fn_ctx_provider(&self) -> Self::TableFuncContextProvider {
        Self::TableFuncContextProvider { ctx: self.ctx }
    }

    fn options(&self) -> &ConfigOptions {
        self.ctx.get_df_state().config_options()
    }
}

pub struct TableFnCtxProvider<'a> {
    ctx: &'a SessionContext,
}

impl<'a> TableFuncContextProvider for TableFnCtxProvider<'a> {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry> {
        self.ctx.get_session_catalog().resolve_database(name)
    }
}

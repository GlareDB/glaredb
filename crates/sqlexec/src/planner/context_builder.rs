use crate::context::SessionContext;
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
use std::collections::HashMap;
use std::sync::Arc;

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

    // Find a table provider the given reference, taking into account the
    // session's search path.
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
}

#[async_trait]
impl<'a> AsyncContextProvider for PartialContextProvider<'a> {
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

    fn options(&self) -> &ConfigOptions {
        self.ctx.get_df_state().config_options()
    }
}

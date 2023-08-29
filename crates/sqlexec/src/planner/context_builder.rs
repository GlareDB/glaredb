use crate::context::local::LocalSessionContext;
use crate::dispatch::Dispatcher;
use crate::errors::ExecError;
use crate::functions::BuiltinScalarFunction;
use crate::functions::PgFunctionBuilder;
use crate::planner::errors::PlanError;
use crate::resolve::EntryResolver;
use crate::resolve::ResolvedEntry;
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
use datafusion_ext::local_hint::LocalTableHint;
use datafusion_ext::planner::AsyncContextProvider;
use datafusion_ext::vars::SessionVars;
use protogen::metastore::types::catalog::{CatalogEntry, CredentialsEntry, DatabaseEntry};
use protogen::metastore::types::options::TableOptions;
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlbuiltins::builtins::DEFAULT_CATALOG;
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

/// Partial context provider with table providers required to fulfill a single
/// query.
///
/// NOTE: While `PartialContextProvider` is for _logical_ planning, DataFusion
/// will actually try to downcast the `TableSource` to a `TableProvider` during
/// physical planning. This only works with `DefaultTableSource` which is what
/// this adapter uses.
pub struct PartialContextProvider<'a> {
    /// Providers we've seen so far.
    providers: HashMap<OwnedTableReference, Arc<dyn TableProvider>>,
    /// Datafusion session state.
    state: &'a SessionState,
    /// Glaredb session context.
    ctx: &'a LocalSessionContext,
    /// Entry resolver to use to resolve tables and other objects.
    resolver: EntryResolver<'a>,
}

impl<'a> PartialContextProvider<'a> {
    pub fn new(ctx: &'a LocalSessionContext, state: &'a SessionState) -> Result<Self, PlanError> {
        let resolver = EntryResolver::from_context(ctx);
        Ok(Self {
            providers: HashMap::new(),
            state,
            ctx,
            resolver,
        })
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

    /// Find a table provider the given reference, taking into account the
    /// session's search path.
    async fn table_for_reference(
        &mut self,
        reference: TableReference<'_>,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        // Try to read from the environment first.
        //
        // TODO: Determine if this is a behavior we want. This was move to the
        // top to preempt reading from a remote session.
        if let TableReference::Bare { table } = &reference {
            if let Some(reader) = self.ctx.get_env_reader() {
                if let Some(table) = reader
                    .resolve_table(table)
                    .map_err(|e| ExecError::EnvironmentTableRead(e))?
                {
                    // Hint that the table being scanned from the environment
                    // should be scanned client-side.
                    return Ok(Arc::new(LocalTableHint(table)));
                }
            }
        }

        let ent = self
            .resolver
            .resolve_entry_from_reference(reference.clone())?;

        // If we have a remote session configured, do some selective dispatching
        // remotely.
        if let Some(mut client) = self.ctx.exec_client() {
            match &ent {
                // References to external databases should always resolve
                // remotely.
                ResolvedEntry::NeedsExternalResolution {
                    db_ent,
                    schema,
                    name,
                } => {
                    let prov = client
                        .dispatch_access(ResolvedTableReference::External {
                            database: db_ent.meta.name.clone(),
                            schema: schema.clone().into_owned(),
                            name: name.clone().into_owned(),
                        })
                        .await?;
                    return Ok(Arc::new(prov));
                }

                // Only resolve native and external tables remotely.
                //
                // Temp and system tables should be handled locally.
                ResolvedEntry::Entry(ent) => {
                    // Anything that should be resolved locally should not go
                    // to server for resolution.
                    let should_resolve_local = match &ent {
                        CatalogEntry::Table(t) => {
                            matches!(&t.options, TableOptions::Debug(_) | TableOptions::Local(_))
                        }
                        _ => false,
                    };
                    let meta = ent.get_meta();
                    let should_resolve_local = meta.is_temp || meta.builtin || should_resolve_local;
                    if !should_resolve_local {
                        let prov = client
                            .dispatch_access(ResolvedTableReference::Internal {
                                table_oid: meta.id,
                            })
                            .await?;
                        return Ok(Arc::new(prov));
                    }
                }
            }
        }

        let dispatcher = Dispatcher::new(
            self.ctx.get_session_catalog(),
            self.ctx.get_native_tables(),
            self.ctx.get_metrics(),
            &self.resolver.temp_objects,
            self.ctx,
            self.ctx.df_ctx(),
            self.ctx.get_session_vars().is_cloud_instance(),
        );

        let provider = match ent {
            ResolvedEntry::Entry(ent) => dispatcher.dispatch(ent).await?,
            ResolvedEntry::NeedsExternalResolution {
                db_ent,
                schema,
                name,
            } => dispatcher.dispatch_external(db_ent, &schema, &name).await?,
        };

        // TODO: See <https://github.com/GlareDB/glaredb/issues/1657#issuecomment-1696219544>
        let provider = if self.ctx.exec_client().is_some() {
            Arc::new(LocalTableHint(provider))
        } else {
            provider
        };

        Ok(provider)
    }

    /// Find a table function with the given reference, taking into account the
    /// session's search path.
    fn table_function_for_reference(
        &self,
        reference: TableReference<'_>,
    ) -> Option<Arc<dyn TableFunc>> {
        // TODO: Refactor this to resolve properly.

        let catalog = self.ctx.get_session_catalog();

        let resolve_func = |schema: String, name| {
            match catalog.resolve_entry(DEFAULT_CATALOG, &schema, name) {
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
                for schema in self.ctx.implicit_search_paths() {
                    if let Some(func_impl) = resolve_func(schema, table) {
                        return Some(func_impl);
                    }
                }
                None
            }
            TableReference::Partial { schema, table } => resolve_func(schema.to_string(), table),
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                if catalog == DEFAULT_CATALOG {
                    resolve_func(schema.to_string(), table)
                } else {
                    None
                }
            }
        }
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

    fn get_table_func(&mut self, name: TableReference<'_>) -> Option<Arc<dyn TableFunc>> {
        self.table_function_for_reference(name)
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
    ctx: &'a LocalSessionContext,
    state: &'a SessionState,
}

impl<'a> TableFuncContextProvider for TableFnCtxProvider<'a> {
    fn get_database_entry(&self, name: &str) -> Option<&DatabaseEntry> {
        self.ctx.get_session_catalog().resolve_database(name)
    }

    fn get_credentials_entry(&self, name: &str) -> Option<&CredentialsEntry> {
        self.ctx.get_session_catalog().resolve_credentials(name)
    }

    fn get_session_vars(&self) -> SessionVars {
        self.ctx.get_session_vars()
    }

    fn get_session_state(&self) -> &SessionState {
        self.state
    }
}

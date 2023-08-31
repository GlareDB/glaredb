use crate::context::local::LocalSessionContext;
use crate::dispatch::DispatchError;
use crate::dispatch::Dispatcher;
use crate::errors::ExecError;
use crate::functions::BuiltinScalarFunction;
use crate::functions::PgFunctionBuilder;
use crate::planner::errors::PlanError;
use crate::remote::client::RemoteSessionClient;
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
use datafusion_ext::functions::FuncParamValue;
use datafusion_ext::local_hint::LocalTableHint;
use datafusion_ext::planner::AsyncContextProvider;

use protogen::metastore::types::catalog::CatalogEntry;
use protogen::metastore::types::catalog::DatabaseEntry;
use protogen::metastore::types::catalog::FunctionEntry;
use protogen::metastore::types::catalog::RuntimePreference;
use protogen::metastore::types::options::TableOptions;
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlbuiltins::functions::BUILTIN_TABLE_FUNCS;
use std::collections::HashMap;
use std::sync::Arc;

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

    fn new_dispatcher(&self) -> Dispatcher {
        Dispatcher::new(
            self.ctx.get_session_catalog(),
            self.ctx.get_native_tables(),
            self.ctx.get_metrics(),
            &self.resolver.temp_objects,
            self.ctx,
            self.ctx.df_ctx(),
            self.ctx.get_session_vars().is_cloud_instance(),
        )
    }

    async fn dispatch_function_local(
        &self,
        func: &FunctionEntry,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, DispatchError> {
        println!("dispatching function local");
        Ok(Arc::new(LocalTableHint(
            self.new_dispatcher()
                .dispatch_function(func, args, opts)
                .await?,
        )))
    }

    async fn dispatch_function_remote(
        &self,
        func: &FunctionEntry,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
        client: &mut RemoteSessionClient,
    ) -> Result<Arc<dyn TableProvider>, ExecError> {
        client
            .dispatch_access(
                ResolvedTableReference::Internal {
                    table_oid: func.meta.id,
                },
                Some(args),
                Some(opts),
            )
            .await
    }

    async fn dispatch_catalog_entry_local(
        &self,
        ent: &CatalogEntry,
    ) -> Result<Arc<dyn TableProvider>, DispatchError> {
        Ok(Arc::new(LocalTableHint(
            self.new_dispatcher().dispatch(ent.clone()).await?,
        )))
    }

    async fn dispatch_external_entry_local(
        &self,
        db_ent: &DatabaseEntry,
        schema: &str,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>, DispatchError> {
        Ok(Arc::new(LocalTableHint(
            self.new_dispatcher()
                .dispatch_external(db_ent, schema, name)
                .await?,
        )))
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
                    .resolve_reference(TableReference::from(&name), None, None)
                    .await?;
                self.providers.insert(name, provider.clone());
                provider
            }
        };

        Ok(provider)
    }

    /// Find a table provider the given reference, taking into account the
    /// session's search path.
    async fn resolve_reference(
        &mut self,
        reference: TableReference<'_>,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
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
        use ResolvedEntry::*;
        let provider = match (ent, self.ctx.exec_client()) {
            // TODO: Views will likely need to be handled a bit
            // differently in the future.
            //
            // Given the following view:
            //
            // `create view v1 as select local_table cross join remote_table`
            //
            // And the following query:
            //
            // `select * from v1`
            //
            // Will mean that the client will be streaming 2 sets of
            // record batches, not just one, as would be optimal.
            //
            // 1. The client needs to upload the results of
            // `local_table`.
            // 2. The client will then receive the results of the
            // cross join, but then needs upload that so that the
            // rest of the query can continue to execute remotely.
            (Entry(ent @ CatalogEntry::View(_)), _) => {
                Arc::new(LocalTableHint(self.new_dispatcher().dispatch(ent).await?))
            }

            // --- LOCAL RESOLUTION ---
            // (function , no remote client)
            (Entry(CatalogEntry::Function(ref f)), None) => {
                let args = args.unwrap_or_default();
                let opts = opts.unwrap_or_default();

                self.dispatch_function_local(f, args, opts).await?
            }

            // (native entry, no remote client)
            (Entry(ent), None) => self.dispatch_catalog_entry_local(&ent).await?,
            // (external entry, no remote client)
            (
                NeedsExternalResolution {
                    db_ent,
                    schema,
                    name,
                },
                None,
            ) => {
                self.dispatch_external_entry_local(db_ent, &schema, &name)
                    .await?
            }

            // --- REMOTE RESOLUTION ---
            // (local entry, remote client)
            (Entry(ref ent @ CatalogEntry::Table(ref t)), Some(client)) => {
                self.handle_table_entry_dispatch(ent, t, client, args, opts)
                    .await?
            }

            (Entry(CatalogEntry::Function(ref f)), Some(client)) => {
                self.handle_function_dispatch(f, args, opts, client).await?
            }

            // (native entry, remote client)
            (Entry(ent), Some(client)) => {
                self.handle_catalog_entry_dispatch(ent, client, args, opts)
                    .await?
            }
            // (external entry, remote client)
            (
                NeedsExternalResolution {
                    db_ent,
                    schema,
                    name,
                },
                Some(mut client),
            ) => {
                client
                    .dispatch_access(
                        ResolvedTableReference::External {
                            database: db_ent.meta.name.clone(),
                            schema: schema.clone().into_owned(),
                            name: name.clone().into_owned(),
                        },
                        args,
                        opts,
                    )
                    .await?
            }
        };

        Ok(provider)
    }

    async fn handle_catalog_entry_dispatch(
        &mut self,
        ent: CatalogEntry,
        mut client: RemoteSessionClient,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        let meta = ent.get_meta();
        let should_resolve_local = meta.is_temp || meta.builtin;
        Ok(if should_resolve_local {
            self.dispatch_catalog_entry_local(&ent).await?
        } else {
            client
                .dispatch_access(
                    ResolvedTableReference::Internal { table_oid: meta.id },
                    args,
                    opts,
                )
                .await?
        })
    }

    async fn handle_function_dispatch(
        &mut self,
        func: &protogen::metastore::types::catalog::FunctionEntry,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
        mut client: RemoteSessionClient,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        if args.is_none() && opts.is_none() {
            return Err(PlanError::Internal(
                "function should have args or opts at this point".to_string(),
            ));
        }
        let args = args.unwrap_or_default();
        let opts = opts.unwrap_or_default();
        Ok(match func.runtime_preference {
            RuntimePreference::Local => self.dispatch_function_local(func, args, opts).await?,
            RuntimePreference::Remote => {
                self.dispatch_function_remote(func, args, opts, &mut client)
                    .await?
            }
            RuntimePreference::Inherit => {
                if self.remote_context_available() {
                    self.dispatch_function_remote(func, args, opts, &mut client)
                        .await?
                } else {
                    self.dispatch_function_local(func, args, opts).await?
                }
            }
            RuntimePreference::Unspecified => {
                let resolve_func = if func.meta.builtin {
                    BUILTIN_TABLE_FUNCS
                        .find_function(&func.meta.name)
                        .expect("function should always exist for builtins")
                } else {
                    return Err(PlanError::Internal(
                        "only builtin functions supported at this timef".to_string(),
                    ));
                };

                let actual_runtime = resolve_func
                    .detect_runtime(&args)
                    .map_err(DispatchError::ExtensionError)?;

                match actual_runtime {
                    RuntimePreference::Local => {
                        self.dispatch_function_local(func, args, opts).await?
                    }
                    RuntimePreference::Remote => {
                        client
                            .dispatch_access(
                                ResolvedTableReference::Internal {
                                    table_oid: func.meta.id,
                                },
                                Some(args),
                                Some(opts),
                            )
                            .await?
                    }
                    _ => panic!(
                        "function should have a specified runtime at this point. This is a bug."
                    ),
                }
            }
        })
    }

    async fn handle_table_entry_dispatch(
        &mut self,
        ent: &CatalogEntry,
        t: &protogen::metastore::types::catalog::TableEntry,
        mut client: RemoteSessionClient,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<Arc<dyn TableProvider>, PlanError> {
        let meta = ent.get_meta();
        let should_resolve_local = meta.is_temp
            || meta.builtin
            || matches!(&t.options, TableOptions::Debug(_) | TableOptions::Local(_));
        Ok(if !should_resolve_local {
            client
                .dispatch_access(
                    ResolvedTableReference::Internal { table_oid: meta.id },
                    args,
                    opts,
                )
                .await?
        } else {
            self.dispatch_catalog_entry_local(ent).await?
        })
    }
    fn remote_context_available(&self) -> bool {
        self.ctx.exec_client().is_some()
    }
}

#[async_trait]
impl<'a> AsyncContextProvider for PartialContextProvider<'a> {
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

    async fn get_table_func(
        &mut self,
        name: TableReference<'_>,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> DataFusionResult<Arc<dyn TableSource>> {
        self.resolve_reference(name.to_owned_reference(), Some(args), Some(opts))
            .await
            .map(|p| Arc::new(DefaultTableSource::new(p)) as _)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
}

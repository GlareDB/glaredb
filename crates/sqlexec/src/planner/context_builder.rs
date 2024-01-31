use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::OwnedTableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{AggregateUDF, TableSource, WindowUDF};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use datafusion::variable::VarProvider;
use datafusion_ext::functions::FuncParamValue;
use datafusion_ext::planner::AsyncContextProvider;
use datafusion_ext::runtime::table_provider::RuntimeAwareTableProvider;
use datafusion_ext::vars::CredentialsVarProvider;
use protogen::metastore::types::catalog::{CatalogEntry, RuntimePreference};
use protogen::metastore::types::options::TableOptions;
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlbuiltins::functions::FUNCTION_REGISTRY;

use crate::context::local::LocalSessionContext;
use crate::dispatch::{DispatchError, Dispatcher};
use crate::errors::ExecError;
use crate::planner::errors::PlanError;
use crate::resolve::{EntryResolver, ResolvedEntry};

/// Partial context provider with table providers required to fulfill a single
/// query.
///
/// NOTE: While `PartialContextProvider` is for _logical_ planning, DataFusion
/// will actually try to downcast the `TableSource` to a `TableProvider` during
/// physical planning. This only works with `DefaultTableSource` which is what
/// this adapter uses.
pub struct PartialContextProvider<'a> {
    /// Providers we've seen so far.
    providers: HashMap<OwnedTableReference, RuntimeAwareTableProvider>,
    /// Datafusion session state.
    state: &'a SessionState,
    /// Glaredb session context.
    ctx: &'a LocalSessionContext,
    /// Entry resolver to use to resolve tables and other objects.
    resolver: EntryResolver<'a>,
    runtime_preference: RuntimePreference,
}

impl<'a> PartialContextProvider<'a> {
    pub fn new(ctx: &'a LocalSessionContext, state: &'a SessionState) -> Result<Self, PlanError> {
        let resolver = EntryResolver::from_context(ctx);
        Ok(Self {
            providers: HashMap::new(),
            state,
            ctx,
            resolver,
            runtime_preference: RuntimePreference::Unspecified,
        })
    }

    fn new_dispatcher(&self) -> Dispatcher {
        Dispatcher::new(
            self.ctx.get_session_catalog(),
            self.ctx.get_native_tables(),
            self.ctx,
            self.ctx.df_ctx(),
            self.ctx.get_session_vars().is_cloud_instance(), // TODO: This locks, remove the locks
        )
    }

    /// Get the table provider from the table reference.
    pub async fn table_provider(
        &mut self,
        name: OwnedTableReference,
    ) -> Result<RuntimeAwareTableProvider, PlanError> {
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
    ///
    /// This will attempt to resolve either a table, or a table returning
    /// function.
    ///
    /// When the session is configured for hybrid exec, the returned table
    /// providers will have their runtime preferences set to where the table
    /// should be scanned (remote or local).
    async fn resolve_reference(
        &mut self,
        reference: TableReference<'_>,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<RuntimeAwareTableProvider, PlanError> {
        // Try to read from the environment first.
        //
        // TODO: Determine if this is a behavior we want. This was move to the
        // top to preempt reading from a remote session.
        if let TableReference::Bare { table } = &reference {
            if let Some(reader) = self.ctx.get_env_reader() {
                if let Some(table) = reader
                    .resolve_table(table)
                    .map_err(ExecError::EnvironmentTableRead)?
                {
                    // Hint that the table being scanned from the environment
                    // should be scanned client-side.
                    return Ok(RuntimeAwareTableProvider::new(
                        RuntimePreference::Local,
                        table,
                    ));
                }
            }
        }

        let ent = self.resolver.resolve_entry_from_reference(reference)?;

        let client = self.ctx.exec_client();
        let provider = match ent {
            // Views
            // Rely on further planning to determine how to handle views.
            ResolvedEntry::Entry(CatalogEntry::View(view)) => RuntimeAwareTableProvider::new(
                RuntimePreference::Unspecified,
                self.new_dispatcher().dispatch_view(&view).await?,
            ),

            // Functions
            ResolvedEntry::Entry(CatalogEntry::Function(func)) => {
                let args = args.unwrap_or_default();
                let opts = opts.unwrap_or_default();

                let table_func = match FUNCTION_REGISTRY.get_table_func(&func.meta.name) {
                    Some(func) => func,
                    None => {
                        return Err(PlanError::String(format!(
                            "'{}' cannot be used in the FROM clause of a query.",
                            func.meta.name
                        )))
                    }
                };

                match client {
                    Some(mut client) => {
                        let actual_runtime = table_func
                            .detect_runtime(&args, self.runtime_preference)
                            .map_err(DispatchError::ExtensionError)?;

                        match actual_runtime {
                            RuntimePreference::Local => RuntimeAwareTableProvider::new(
                                RuntimePreference::Local,
                                self.new_dispatcher()
                                    .dispatch_table_function(&func, args, opts)
                                    .await?,
                            ),
                            RuntimePreference::Remote => RuntimeAwareTableProvider::new(
                                RuntimePreference::Remote,
                                client
                                    .dispatch_access(
                                        ResolvedTableReference::Internal {
                                            table_oid: func.meta.id,
                                        },
                                        Some(args),
                                        Some(opts),
                                    )
                                    .await?,
                            ),
                            _ => return Err(PlanError::Internal(
                                "function's actual runtime should always be one of remote or local"
                                    .to_string(),
                            )),
                        }
                    }
                    None => RuntimeAwareTableProvider::new(
                        RuntimePreference::Local,
                        self.new_dispatcher()
                            .dispatch_table_function(&func, args, opts)
                            .await?,
                    ),
                }
            }

            // Tables
            ResolvedEntry::Entry(CatalogEntry::Table(table)) => match client {
                Some(mut client) => {
                    // TODO: This "run local" check will fail for builtin tables
                    // that actually write out to storage.
                    let run_local = table.meta.is_temp
                        || table.meta.builtin
                        || matches!(
                            &table.options,
                            TableOptions::Debug(_) | TableOptions::Local(_)
                        );

                    if run_local {
                        RuntimeAwareTableProvider::new(
                            RuntimePreference::Local,
                            self.new_dispatcher().dispatch_table(&table).await?,
                        )
                    } else {
                        RuntimeAwareTableProvider::new(
                            RuntimePreference::Remote,
                            client
                                .dispatch_access(
                                    ResolvedTableReference::Internal {
                                        table_oid: table.meta.id,
                                    },
                                    args,
                                    opts,
                                )
                                .await?,
                        )
                    }
                }
                None => RuntimeAwareTableProvider::new(
                    RuntimePreference::Local,
                    self.new_dispatcher().dispatch_table(&table).await?,
                ),
            },

            // Everything else.
            ResolvedEntry::Entry(ent) => {
                return Err(PlanError::String(format!(
                    "Invalid entry type for converting to a table: {}",
                    ent.entry_type()
                )))
            }

            // Need to hit an external system
            ResolvedEntry::NeedsExternalResolution {
                db_ent,
                schema,
                name,
            } => match client {
                Some(mut client) => RuntimeAwareTableProvider::new(
                    RuntimePreference::Remote,
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
                        .await?,
                ),
                None => RuntimeAwareTableProvider::new(
                    RuntimePreference::Local,
                    self.new_dispatcher()
                        .dispatch_external(db_ent, schema, name)
                        .await?,
                ),
            },
        };

        Ok(provider)
    }
}

#[async_trait]
impl<'a> AsyncContextProvider for PartialContextProvider<'a> {
    async fn get_table_source(
        &mut self,
        name: TableReference<'_>,
    ) -> DataFusionResult<Arc<dyn TableSource>> {
        let provider = self
            .table_provider(name.to_owned_reference())
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!("Unable to fetch table provider for '{name}': {e}"))
            })?;
        Ok(Arc::new(DefaultTableSource::new(Arc::new(provider))))
    }

    async fn get_table_function_source(
        &mut self,
        name: TableReference<'_>,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> DataFusionResult<Arc<dyn TableSource>> {
        self.resolve_reference(name.to_owned_reference(), Some(args), Some(opts))
            .await
            .map(|p| Arc::new(DefaultTableSource::new(Arc::new(p))) as _)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn get_function_meta(
        &mut self,
        name: &str,
        args: &[Expr],
    ) -> DataFusionResult<Option<Expr>> {
        FUNCTION_REGISTRY
            .get_scalar_udf(name)
            .map(|f| f.try_as_expr(self.ctx.get_session_catalog(), args.to_vec()))
            .transpose()
    }

    async fn get_variable_type(&mut self, var_names: &[String]) -> Option<DataType> {
        let catalog = self.ctx.get_session_catalog();
        let cred_var_provider = CredentialsVarProvider::new(catalog);
        cred_var_provider.get_type(var_names)
    }

    async fn get_aggregate_meta(&mut self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    async fn get_window_meta(&mut self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
}

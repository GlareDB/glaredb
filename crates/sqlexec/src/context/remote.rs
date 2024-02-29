use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use catalog::mutator::CatalogMutator;
use catalog::session_catalog::SessionCatalog;
use datafusion::common::not_impl_err;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext as DfSessionContext};
use datafusion::execution::FunctionRegistry as DFRegistry;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::variable::VarType;
use datafusion_ext::functions::FuncParamValue;
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use distexec::scheduler::Scheduler;
use protogen::metastore::types::catalog::{CatalogEntry, CatalogState};
use protogen::rpcsrv::types::service::ResolvedTableReference;
use sqlbuiltins::functions::FunctionRegistry;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};
use crate::dispatch::external::ExternalDispatcher;
use crate::errors::{ExecError, Result};
use crate::extension_codec::GlareDBExtensionCodec;
use crate::remote::provider_cache::ProviderCache;
use crate::remote::staged_stream::StagedClientStreams;

/// A lightweight session context used during remote execution of physical
/// plans.
///
/// This context should be stateless in that it should not be tied to any one
/// specific session. This context should be able to execute physical plans from
/// any session for this partition database.
///
/// Datafusion extensions:
/// - StagedClientStreams
pub struct RemoteSessionContext {
    /// Database catalog.
    // TODO: Remove lock and instead track multiple catalog versions.
    catalog: Mutex<SessionCatalog>,
    /// Native tables.
    tables: NativeTableStorage,
    /// Datafusion session context used for execution.
    df_ctx: DfSessionContext,
    /// Cached table providers.
    provider_cache: ProviderCache,
    functions: FunctionRegistry,
}

impl RemoteSessionContext {
    /// Create a new remote session context.
    pub fn new(
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        spill_path: Option<PathBuf>,
        task_scheduler: Scheduler,
    ) -> Result<Self> {
        // TODO: We'll want to remove this eventually. We should be able to
        // create a datafusion context/runtime without needing these vars.
        //
        // `with_is_cloud_instance` is set here (for all remote sessions) for
        // builtins that run _only_ in remote/cloud contexts, such as
        // `cloud_upload`.
        let vars: SessionVars =
            SessionVars::default().with_is_cloud_instance(true, VarType::System);

        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(&vars);
        let mut conf: SessionConfig = opts.into();

        // Add in remote only extensions.
        //
        // Note: No temp catalog here since we should not be executing create
        // temp tables remotely.
        conf = conf
            .with_extension(Arc::new(StagedClientStreams::default()))
            .with_extension(Arc::new(catalog_mutator))
            .with_extension(Arc::new(native_tables.clone()))
            .with_extension(Arc::new(task_scheduler.clone()));

        let df_ctx = DfSessionContext::new_with_config_rt(conf, Arc::new(runtime));

        Ok(RemoteSessionContext {
            catalog: Mutex::new(catalog),
            tables: native_tables,
            df_ctx,
            provider_cache: ProviderCache::default(),
            functions: FunctionRegistry::default(),
        })
    }

    pub fn get_datafusion_context(&self) -> &DfSessionContext {
        &self.df_ctx
    }

    pub async fn get_catalog_state(&self) -> CatalogState {
        let catalog = self.catalog.lock().await;
        catalog.get_state().as_ref().clone()
    }

    pub async fn refresh_catalog(&self) -> Result<()> {
        self.catalog
            .lock()
            .await
            .maybe_refresh_state(self.catalog_mutator().get_metastore_client(), false)
            .await?;
        Ok(())
    }

    /// Returns the extension codec used for serializing and deserializing data
    /// over RPCs.
    pub fn extension_codec(&self) -> GlareDBExtensionCodec<'_> {
        GlareDBExtensionCodec::new_decoder(&self.provider_cache)
    }

    fn catalog_mutator(&self) -> Arc<CatalogMutator> {
        self.df_ctx
            .state()
            .config()
            .get_extension::<CatalogMutator>()
            .unwrap()
    }

    pub fn staged_streams(&self) -> Arc<StagedClientStreams> {
        self.df_ctx
            .state()
            .config()
            .get_extension::<StagedClientStreams>()
            .expect("remote contexts should have streams registered")
    }

    /// Execute a physical plan.
    pub fn execute_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let context = self.df_ctx.task_ctx();
        let stream = execute_stream(plan, context)?;
        Ok(stream)
    }

    /// Load a table provider, and cache it on the context.
    ///
    /// This will only attempt to load "native" tables and external tables.
    ///
    /// All parts of the table reference must be provided. It's expected that
    /// entry resolution happens client-side.
    // TODO: We should be providing the catalog version as well to ensure we're
    // getting the correct entries from the catalog.
    pub async fn load_and_cache_table(
        &self,
        table_ref: ResolvedTableReference,
        args: Option<Vec<FuncParamValue>>,
        opts: Option<HashMap<String, FuncParamValue>>,
    ) -> Result<(Uuid, Arc<dyn TableProvider>)> {
        // TODO: Remove this lock so we're not holding it for the duration of
        // the table load. We can do that by:
        // 1. Storing multiple catalog versions
        // 2. Getting a reference to the version we care about (by passing it in as an arg)
        let mut catalog = self.catalog.lock().await;
        catalog
            .maybe_refresh_state(self.catalog_mutator().get_metastore_client(), false)
            .await?;

        // Since this is operating on a remote node, always disable local fs
        // access.
        let dispatcher = ExternalDispatcher::new(&catalog, &self.df_ctx, &self.functions, true);

        let prov: Arc<dyn TableProvider> = match table_ref {
            ResolvedTableReference::Internal { table_oid } => match catalog.get_by_oid(table_oid) {
                Some(CatalogEntry::Table(tbl)) => {
                    if tbl.meta.external {
                        dispatcher.dispatch_external_table(tbl).await?
                    } else {
                        self.tables.load_table(tbl).await?.into_table_provider()
                    }
                }
                Some(CatalogEntry::Function(f)) => {
                    dispatcher.dispatch_function(f, args, opts).await?
                }
                Some(_) => {
                    return Err(ExecError::Internal(format!("oid not a table: {table_oid}")))
                }
                None => {
                    return Err(ExecError::Internal(format!(
                        "missing entry for oid: {table_oid}"
                    )))
                }
            },
            ResolvedTableReference::External {
                database,
                schema,
                name,
            } => {
                dispatcher
                    .dispatch_external(&database, &schema, &name)
                    .await?
            }
        };

        let id = Uuid::new_v4();
        self.provider_cache.put(id, prov.clone());

        Ok((id, prov))
    }
}

impl DFRegistry for RemoteSessionContext {
    fn udfs(&self) -> std::collections::HashSet<String> {
        self.functions
            .scalar_udfs_iter()
            .map(|k| k.name().to_string())
            .collect()
    }

    fn udf(
        &self,
        name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::ScalarUDF>> {
        self.functions
            .get_scalar_udf(name)
            .map(|f| f.try_into_scalar_udf())
            .transpose()?
            .map(Arc::new)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!("UDF not found: {name}"))
            })
    }

    fn udaf(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::AggregateUDF>> {
        not_impl_err!("aggregate functions")
    }

    fn udwf(
        &self,
        _name: &str,
    ) -> datafusion::error::Result<Arc<datafusion::logical_expr::WindowUDF>> {
        not_impl_err!("window functions")
    }
}

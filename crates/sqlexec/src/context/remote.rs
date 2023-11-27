use std::{collections::HashMap, path::PathBuf, sync::Arc};

use datafusion::{
    datasource::TableProvider,
    execution::context::{SessionConfig, SessionContext as DfSessionContext},
    physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_ext::{functions::FuncParamValue, vars::SessionVars};
use datasources::native::access::NativeTableStorage;
use protogen::{
    metastore::types::catalog::{CatalogEntry, CatalogState},
    rpcsrv::types::service::ResolvedTableReference,
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    dispatch::external::ExternalDispatcher,
    errors::{ExecError, Result},
    extension_codec::GlareDBExtensionCodec,
    remote::{provider_cache::ProviderCache, staged_stream::StagedClientStreams},
};
use catalog::session_catalog::{CatalogMutator, SessionCatalog};

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

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
}

impl RemoteSessionContext {
    /// Create a new remote session context.
    pub fn new(
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        spill_path: Option<PathBuf>,
    ) -> Result<Self> {
        // TODO: We'll want to remove this eventually. We should be able to
        // create a datafusion context/runtime without needing these vars.
        let vars = SessionVars::default();

        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(&vars);
        let mut conf: SessionConfig = opts.into();

        // Add in remote only extensions.
        conf = conf
            .with_extension(Arc::new(StagedClientStreams::default()))
            .with_extension(Arc::new(catalog_mutator))
            .with_extension(Arc::new(native_tables.clone()));

        // TODO: Query planners for handling custom plans.

        let df_ctx = DfSessionContext::new_with_config_rt(conf, Arc::new(runtime));

        Ok(RemoteSessionContext {
            catalog: Mutex::new(catalog),
            tables: native_tables,
            df_ctx,
            provider_cache: ProviderCache::default(),
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
        GlareDBExtensionCodec::new_decoder(&self.provider_cache, self.df_ctx.runtime_env())
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
        let dispatcher = ExternalDispatcher::new(&catalog, &self.df_ctx, true);

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

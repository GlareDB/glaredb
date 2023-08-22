use std::{path::PathBuf, sync::Arc};

use datafusion::{
    datasource::TableProvider,
    execution::context::{SessionConfig, SessionContext as DfSessionContext},
    physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use uuid::Uuid;

use crate::{
    background_jobs::JobRunner,
    dispatch::external::ExternalDispatcher,
    errors::Result,
    extension_codec::GlareDBExtensionCodec,
    metastore::catalog::{CatalogMutator, SessionCatalog},
    remote::{provider_cache::ProviderCache, staged_stream::StagedClientStreams},
};

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

/// A lightweight session context used during remote execution of physical
/// plans.
///
/// Datafusion extensions:
/// - StagedClientStreams
pub struct RemoteSessionContext {
    /// Database catalog.
    catalog: SessionCatalog,
    /// Native tables.
    tables: NativeTableStorage,
    /// Datafusion session context used for execution.
    df_ctx: DfSessionContext,
    /// Job runner for background jobs.
    _background_jobs: JobRunner,
    /// Cached table providers.
    provider_cache: ProviderCache,
}

impl RemoteSessionContext {
    /// Create a new remote session context.
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        catalog_mutator: CatalogMutator,
        native_tables: NativeTableStorage,
        background_jobs: JobRunner,
        spill_path: Option<PathBuf>,
    ) -> Result<Self> {
        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(vars);
        let mut conf: SessionConfig = opts.into();

        // Add in remote only extensions.
        conf = conf.with_extension(Arc::new(StagedClientStreams::default()));
        conf = conf.with_extension(Arc::new(catalog_mutator));

        // TODO: Query planners for handling custom plans.

        let df_ctx = DfSessionContext::with_config_rt(conf, Arc::new(runtime));

        Ok(RemoteSessionContext {
            catalog,
            tables: native_tables,
            df_ctx,
            _background_jobs: background_jobs,
            provider_cache: ProviderCache::default(),
        })
    }

    pub fn get_datafusion_context(&self) -> &DfSessionContext {
        &self.df_ctx
    }

    pub fn get_session_catalog(&self) -> &SessionCatalog {
        &self.catalog
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
        &mut self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<(Uuid, Arc<dyn TableProvider>)> {
        self.catalog
            .maybe_refresh_state(self.catalog_mutator().get_metastore_client(), false)
            .await?;

        let prov: Arc<dyn TableProvider> =
            if let Some(tbl) = self.catalog.resolve_native_table(database, schema, name) {
                self.tables.load_table(tbl).await?.into_table_provider()
            } else {
                // Since this is operating on a remote node, always disable local fs
                // access.
                let dispatcher = ExternalDispatcher::new(&self.catalog, &self.df_ctx, true);
                dispatcher.dispatch_external(database, schema, name).await?
            };

        let id = Uuid::new_v4();
        self.provider_cache.put(id, prov.clone());

        Ok((id, prov))
    }
}

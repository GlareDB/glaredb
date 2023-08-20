use std::{collections::HashMap, path::PathBuf, sync::Arc};

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
    remote::staged_stream::StagedClientStreams,
};

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

/// A lightweight session context used during remote execution of physical
/// plans.
pub struct RemoteSessionContext {
    /// Database catalog.
    catalog: SessionCatalog,
    catalog_mutator: CatalogMutator,
    /// Native tables.
    _tables: NativeTableStorage,
    /// Datafusion session context used for execution.
    df_ctx: DfSessionContext,
    /// Job runner for background jobs.
    _background_jobs: JobRunner,
    /// Cached table providers.
    cached_table_providers: HashMap<Uuid, Arc<dyn TableProvider>>,
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

        // TODO: Query planners for handling custom plans.

        let df_ctx = DfSessionContext::with_config_rt(conf, Arc::new(runtime));

        Ok(RemoteSessionContext {
            catalog,
            catalog_mutator,
            _tables: native_tables,
            df_ctx,
            _background_jobs: background_jobs,
            cached_table_providers: HashMap::new(),
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
        GlareDBExtensionCodec::new_decoder(&self.cached_table_providers)
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

    /// Load an external table, and cache it on the context.
    ///
    /// All parts of the table reference must be provided. It's expected that
    /// entry resolution happens client-side.
    // TODO: We should be providing the catalog version as well to ensure we're
    // getting the correct entries from the catalog.
    pub async fn load_and_cache_external_table(
        &mut self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<(Uuid, Arc<dyn TableProvider>)> {
        self.catalog
            .maybe_refresh_state(self.catalog_mutator.get_metastore_client(), false)
            .await?;

        // Since this is operating on a remote node, always disable local fs
        // access.
        let dispatcher = ExternalDispatcher::new(&self.catalog, &self.df_ctx, true);
        let table = dispatcher.dispatch_external(database, schema, name).await?;

        let id = Uuid::new_v4();

        self.cached_table_providers.insert(id, table.clone());

        Ok((id, table))
    }
}

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use datafusion::{
    datasource::TableProvider,
    execution::context::{SessionConfig, SessionContext as DfSessionContext},
};
use datafusion_ext::vars::SessionVars;
use datasources::native::access::NativeTableStorage;
use uuid::Uuid;

use crate::{
    background_jobs::JobRunner,
    errors::Result,
    metastore::catalog::SessionCatalog,
    remote::staged_stream::{StagedClientStreams, StagedStreams},
};

use super::{new_datafusion_runtime_env, new_datafusion_session_config_opts};

/// A lightweight session context used during remote execution of physical
/// plans.
pub struct RemoteSessionContext {
    /// Database catalog.
    catalog: SessionCatalog,
    /// Native tables.
    tables: NativeTableStorage,
    /// Datafusion session context used for execution.
    df_ctx: DfSessionContext,
    /// Job runner for background jobs.
    background_jobs: JobRunner,
    /// Cached table providers.
    cached_table_providers: HashMap<Uuid, Arc<dyn TableProvider>>,
}

impl RemoteSessionContext {
    /// Create a new remote session context.
    pub fn new(
        vars: SessionVars,
        catalog: SessionCatalog,
        native_tables: NativeTableStorage,
        background_jobs: JobRunner,
        spill_path: Option<PathBuf>,
    ) -> Result<Self> {
        let runtime = new_datafusion_runtime_env(&vars, &catalog, spill_path)?;
        let opts = new_datafusion_session_config_opts(vars);
        let mut conf: SessionConfig = opts.into();

        // Add in remote only extensions.
        conf = conf.with_extension(Arc::new(StagedClientStreams::default()));

        // TODO: Query planners.

        let df_ctx = DfSessionContext::with_config_rt(conf, Arc::new(runtime));

        Ok(RemoteSessionContext {
            catalog,
            tables: native_tables,
            df_ctx,
            background_jobs,
            cached_table_providers: HashMap::new(),
        })
    }
}

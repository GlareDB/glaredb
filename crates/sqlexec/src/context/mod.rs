pub mod local;
pub mod remote;

use std::{path::PathBuf, sync::Arc};

use datafusion::{
    config::{CatalogOptions, ConfigOptions, Extensions, OptimizerOptions},
    execution::{
        context::{
            SessionConfig as DfSessionConfig, SessionContext as DfSessionContext,
            SessionState as DfSessionState,
        },
        disk_manager::DiskManagerConfig,
        memory_pool::GreedyMemoryPool,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
};
use datafusion_ext::vars::SessionVars;
use datasources::object_store::init_session_registry;
use protogen::metastore::types::catalog::CatalogEntry;

use crate::{
    errors::Result, metastore::catalog::SessionCatalog, remote::staged_stream::StagedClientStreams,
};

/// Create a new datafusion runtime env common to both remote and local
/// sessions.
///
/// If `spill_path` is provided, a new disk manager will be created pointing
/// to that path. Otherwise the manager will be kept as default (request
/// temp files from the OS).
///
/// If `memory_limit_bytes` in session varables is non-zero, a new memory pool
/// will be created with the max set to this value.
pub(crate) fn new_datafusion_runtime_env(
    vars: &SessionVars,
    catalog: &SessionCatalog,
    spill_path: Option<PathBuf>,
) -> Result<RuntimeEnv> {
    // Create a new datafusion runtime env with disk manager and memory pool
    // if needed.
    let mut runtime_conf = RuntimeConfig::default();
    if let Some(spill_path) = spill_path {
        runtime_conf =
            runtime_conf.with_disk_manager(DiskManagerConfig::NewSpecified(vec![spill_path]));
    }
    if let Some(mem_limit) = vars.memory_limit_bytes() {
        // TODO: Make this actually have optional semantics.
        if mem_limit > 0 {
            runtime_conf =
                runtime_conf.with_memory_pool(Arc::new(GreedyMemoryPool::new(mem_limit)));
        }
    }

    // let config = config.with_extension(Arc::new(vars));
    let runtime = RuntimeEnv::new(runtime_conf)?;

    // Register the object store in the registry for all the tables.
    let entries = catalog.iter_entries().filter_map(|e| {
        if !e.builtin {
            if let CatalogEntry::Table(entry) = e.entry {
                Some(&entry.options)
            } else {
                None
            }
        } else {
            None
        }
    });
    init_session_registry(&runtime, entries)?;

    Ok(runtime)
}

/// Create a new datafusion config opts common to both local and remote
/// sessions.
pub(crate) fn new_datafusion_session_config_opts(vars: SessionVars) -> ConfigOptions {
    // NOTE: We handle catalog/schema defaults and information schemas
    // ourselves.
    let mut catalog_opts = CatalogOptions::default();
    catalog_opts.create_default_catalog_and_schema = false;
    catalog_opts.information_schema = false;
    let mut optimizer_opts = OptimizerOptions::default();
    optimizer_opts.prefer_hash_join = true;
    let mut config_opts = ConfigOptions::new();

    config_opts.catalog = catalog_opts;
    config_opts.optimizer = optimizer_opts;

    // Insert extensions common to both local and remote sessions.
    let mut e = Extensions::new();
    e.insert(vars);
    config_opts = config_opts.with_extensions(e);

    config_opts
}

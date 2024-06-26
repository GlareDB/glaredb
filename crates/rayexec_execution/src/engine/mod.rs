pub mod modify;
pub mod result_stream;
pub mod session;
pub mod vars;

use rayexec_error::{Result, ResultExt};
use session::Session;
use std::sync::Arc;

use crate::{
    database::{storage::system::SystemCatalog, DatabaseContext},
    datasource::{DataSourceRegistry, MemoryDataSource},
    scheduler::ComputeScheduler,
};

#[derive(Debug)]
pub struct EngineRuntime {
    /// The primary compute scheduler for executing queries.
    ///
    /// Capable of executing async code, but is highly specialized for execution
    /// query pipelines.
    pub scheduler: ComputeScheduler,

    /// Reference to the tokio runtime used at the "edges" of the system.
    ///
    /// Some data sources require parts be executed inside a tokio runtime. It's
    /// assumed that all futures are Send, and so can be sent to the compute
    /// scheduler for actual query execution.
    ///
    /// It's possible this may be made optional in the future.
    pub tokio: tokio::runtime::Runtime,
}

impl EngineRuntime {
    pub fn try_new_shared() -> Result<Arc<Self>> {
        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .thread_name("rayexec_tokio")
            .build()
            .context("Failed to build tokio runtime")?;

        let scheduler = ComputeScheduler::try_new()?;

        Ok(Arc::new(EngineRuntime { scheduler, tokio }))
    }
}

#[derive(Debug)]
pub struct Engine {
    rt: Arc<EngineRuntime>,
    registry: Arc<DataSourceRegistry>,
    system_catalog: SystemCatalog,
}

impl Engine {
    pub fn new(rt: Arc<EngineRuntime>) -> Result<Self> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;
        Self::new_with_registry(rt, registry)
    }

    pub fn new_with_registry(rt: Arc<EngineRuntime>, registry: DataSourceRegistry) -> Result<Self> {
        let system_catalog = SystemCatalog::new(&registry);

        Ok(Engine {
            rt,
            registry: Arc::new(registry),
            system_catalog,
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(Session::new(
            context,
            self.rt.clone(),
            self.registry.clone(),
        ))
    }
}

pub mod profiler;
pub mod result;
pub mod server_state;
pub mod session;

use std::sync::Arc;

use rayexec_error::Result;
use server_state::ServerState;
use session::Session;

use crate::database::memory_catalog::MemoryCatalog;
use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::{DataSourceRegistry, MemoryDataSource};
use crate::runtime::{PipelineExecutor, Runtime};

#[derive(Debug)]
pub struct Engine<P: PipelineExecutor, R: Runtime> {
    registry: Arc<DataSourceRegistry>,
    system_catalog: Arc<MemoryCatalog>,
    executor: P,
    runtime: R,
}

impl<P, R> Engine<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(executor: P, runtime: R) -> Result<Self> {
        let registry =
            DataSourceRegistry::default().with_datasource("memory", Box::new(MemoryDataSource))?;
        Self::new_with_registry(executor, runtime, registry)
    }

    pub fn new_with_registry(
        executor: P,
        runtime: R,
        registry: DataSourceRegistry,
    ) -> Result<Self> {
        let system_catalog = Arc::new(new_system_catalog(&registry)?);

        Ok(Engine {
            registry: Arc::new(registry),
            system_catalog,
            executor,
            runtime,
        })
    }

    /// Creates a new database context that contains only the system catalog and
    /// a temporary catalog.
    ///
    /// This should be the base of all session catalogs.
    pub fn new_base_database_context(&self) -> Result<DatabaseContext> {
        DatabaseContext::new(self.system_catalog.clone())
    }

    pub fn new_session(&self) -> Result<Session<P, R>> {
        let context = self.new_base_database_context()?;
        Ok(Session::new(
            context,
            self.executor.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }

    pub fn new_server_state(&self) -> Result<ServerState<P, R>> {
        Ok(ServerState::new(
            self.executor.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }
}

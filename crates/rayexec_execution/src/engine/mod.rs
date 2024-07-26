pub mod result;
pub mod server_session;
pub mod session;
pub mod vars;

use rayexec_error::Result;
use server_session::ServerSession;
use session::Session;
use std::sync::Arc;

use crate::{
    database::{storage::system::SystemCatalog, DatabaseContext},
    datasource::{DataSourceRegistry, MemoryDataSource},
    runtime::{PipelineExecutor, Runtime},
};

#[derive(Debug)]
pub struct Engine<P: PipelineExecutor, R: Runtime> {
    registry: Arc<DataSourceRegistry>,
    system_catalog: SystemCatalog,
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
        let system_catalog = SystemCatalog::new(&registry);

        Ok(Engine {
            registry: Arc::new(registry),
            system_catalog,
            executor,
            runtime,
        })
    }

    pub fn new_session(&self) -> Result<Session<P, R>> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(Session::new(
            context,
            self.executor.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }

    pub fn new_server_session(&self) -> Result<ServerSession<P, R>> {
        let context = DatabaseContext::new(self.system_catalog.clone());
        Ok(ServerSession::new(
            context,
            self.executor.clone(),
            self.runtime.clone(),
            self.registry.clone(),
        ))
    }
}

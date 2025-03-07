pub mod profiler;
pub mod query_result;
pub mod session;
pub mod single_user;

use std::sync::Arc;

use rayexec_error::Result;
use session::Session;

use crate::catalog::context::{AccessMode, Database, DatabaseContext};
use crate::catalog::system::new_system_catalog;
use crate::runtime::{PipelineExecutor, Runtime};

#[derive(Debug)]
pub struct Engine<P: PipelineExecutor, R: Runtime> {
    system_catalog: Arc<Database>,
    executor: P,
    runtime: R,
}

impl<P, R> Engine<P, R>
where
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(executor: P, runtime: R) -> Result<Self> {
        let system_catalog = Arc::new(Database {
            mode: AccessMode::ReadOnly,
            catalog: new_system_catalog()?,
            attach_info: None,
        });

        Ok(Engine {
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
        ))
    }
}

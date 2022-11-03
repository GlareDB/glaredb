use crate::catalog::DatabaseCatalog;
use crate::errors::Result;
use crate::session::Session;
use access::runtime::AccessRuntime;
use datafusion::execution::runtime_env::RuntimeEnv;
use std::sync::Arc;

pub struct Engine {
    catalog: Arc<DatabaseCatalog>,
    runtime: Arc<RuntimeEnv>, // TODO: Per session runtime.
    access_runtime: Arc<AccessRuntime>,
}

impl Engine {
    pub fn new(db_name: impl Into<String>, access: Arc<AccessRuntime>) -> Result<Engine> {
        let runtime = RuntimeEnv::default();

        let catalog = DatabaseCatalog::new(db_name);
        catalog.insert_default_schema()?;

        let catalog = Arc::new(catalog);
        DatabaseCatalog::insert_information_schema(catalog.clone())?;

        Ok(Engine {
            catalog,
            runtime: Arc::new(runtime),
            access_runtime: access,
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        Ok(Session::new(
            self.catalog.clone(),
            self.runtime.clone(),
            self.access_runtime.clone(),
        ))
    }
}

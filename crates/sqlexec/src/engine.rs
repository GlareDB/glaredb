use crate::errors::Result;
use crate::session::Session;
use access::runtime::AccessRuntime;
use catalog::catalog::DatabaseCatalog;
use std::sync::Arc;

/// Wrapper around the database catalog.
pub struct Engine {
    catalog: DatabaseCatalog,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(runtime: Arc<AccessRuntime>) -> Result<Engine> {
        let catalog = DatabaseCatalog::open(runtime).await?;
        Ok(Engine { catalog })
    }

    pub fn new_session(&self) -> Result<Session> {
        Ok(Session::new(self.catalog.clone())?)
    }
}

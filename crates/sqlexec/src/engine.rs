use crate::catalog::Catalog;
use crate::errors::Result;
use crate::session::Session;
use stablestore::StableStorage;
use std::sync::Arc;
use uuid::Uuid;

/// Wrapper around the database catalog.
pub struct Engine {
    catalog: Arc<Catalog>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new<S: StableStorage>(storage: S) -> Result<Engine> {
        let catalog = Catalog::open().await?;
        Ok(Engine {
            catalog: Arc::new(catalog),
        })
    }

    /// Create a new session with the given id.
    pub fn new_session(&self, id: Uuid) -> Result<Session> {
        Session::new(self.catalog.clone(), id)
    }
}

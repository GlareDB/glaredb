use crate::errors::Result;
use crate::session::Session;
use access::runtime::AccessRuntime;
use jsoncat::catalog::Catalog;
use jsoncat::load_catalog;
use jsoncat::transaction::StubCatalogContext;
use std::sync::Arc;

/// Wrapper around the database catalog.
pub struct Engine {
    catalog: Arc<Catalog>,
}

impl Engine {
    /// Create a new engine using the provided access runtime.
    pub async fn new(runtime: Arc<AccessRuntime>) -> Result<Engine> {
        let catalog = load_catalog(
            &StubCatalogContext,
            runtime.config().db_name.clone(),
            runtime.object_store().clone(),
        )
        .await?;
        Ok(Engine {
            catalog: Arc::new(catalog),
        })
    }

    pub fn new_session(&self) -> Result<Session> {
        Session::new(self.catalog.clone())
    }
}

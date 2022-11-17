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
    pub async fn new(runtime: Arc<AccessRuntime>) -> Result<Engine> {
        let catalog = DatabaseCatalog::open(runtime).await?;

        // catalog.insert_default_schema()?;

        // let catalog = Arc::new(catalog);
        // DatabaseCatalog::insert_information_schema(catalog.clone())?;

        Ok(Engine { catalog })
    }

    pub fn new_session(&self) -> Result<Session> {
        Ok(Session::new(self.catalog.clone())?)
    }
}

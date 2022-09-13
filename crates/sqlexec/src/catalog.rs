use crate::errors::{internal, Result};
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::planner::ContextProvider;
use parking_lot::Mutex;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct DatabaseCatalog {
    schemas: Arc<Mutex<HashMap<String, Arc<dyn SchemaProvider>>>>,
}

impl CatalogList for DatabaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!() // Purposely unimplemented.
    }

    fn catalog_names(&self) -> Vec<String> {
        vec!["glaredb".into()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == "glaredb" {
            Some(Arc::new(self.clone()))
        } else {
            None
        }
    }
}

impl CatalogProvider for DatabaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.lock();
        schemas.iter().map(|(name, _)| name).cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.lock();
        schemas.get(name).map(|schema| schema.clone())
    }
}

#[derive(Clone)]
pub struct SchemaCatalog {
    tables: Arc<Mutex<HashMap<String, Arc<dyn TableProvider>>>>,
}

impl SchemaProvider for SchemaCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.lock();
        tables.iter().map(|(name, _)| name).cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.lock();
        tables.get(name).map(|table| table.clone())
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.lock();
        tables.contains_key(name)
    }
}

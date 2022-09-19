use crate::errors::{internal, Result};
use crate::information_schema::{InformationSchemaProvider, INFORMATION_SCHEMA};
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use parking_lot::RwLock;
use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub const DEFAULT_SCHEMA: &str = "public";

#[derive(Clone, Default)]
pub struct DatabaseCatalog {
    name: String,
    schemas: Arc<RwLock<HashMap<String, Arc<dyn SchemaProvider>>>>,
}

impl DatabaseCatalog {
    /// Create a new database catalog.
    pub fn new(name: impl Into<String>) -> DatabaseCatalog {
        DatabaseCatalog {
            name: name.into(),
            schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Insert an information schema into this catalog.
    ///
    /// Note that this takes an arc as the information schema view needs a
    /// reference to the catalog since everything is built during query time.
    pub fn insert_information_schema(self: Arc<Self>) -> Result<()> {
        let schema = InformationSchemaProvider::new(self.clone());
        self.insert_schema(INFORMATION_SCHEMA, Arc::new(schema))?;
        Ok(())
    }

    /// Insert the default "public" schema.
    pub fn insert_default_schema(&self) -> Result<()> {
        let schema = Arc::new(SchemaCatalog::new());
        self.insert_schema(DEFAULT_SCHEMA, schema)?;
        Ok(())
    }

    /// Insert a schema with the given name.
    pub fn insert_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<()> {
        let mut schemas = self.schemas.write();
        match schemas.entry(name.into()) {
            Entry::Occupied(ent) => return Err(internal!("duplicate schema: {}", ent.key())),
            Entry::Vacant(ent) => {
                ent.insert(schema);
            }
        }

        Ok(())
    }

    /// Drop a schema.
    pub fn drop_schema(&self, name: &str) -> Result<()> {
        let mut schemas = self.schemas.write();
        if schemas.remove(name).is_none() {
            return Err(internal!("cannot drop non-existent schema: {}", name));
        }
        Ok(())
    }
}

impl CatalogList for DatabaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // Purposely unimplemented.
        //
        // The unreleased version of datafusion has a default impl that errors.
        // We'll want to switch to that once it's released.
        unimplemented!()
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == self.name {
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
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read();
        schemas.get(name).cloned()
    }
}

#[derive(Clone, Default)]
pub struct SchemaCatalog {
    tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
}

impl SchemaCatalog {
    pub fn new() -> SchemaCatalog {
        SchemaCatalog {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn insert_table(&self, name: String, table: Arc<dyn TableProvider>) -> Result<()> {
        let mut tables = self.tables.write();
        match tables.entry(name) {
            Entry::Occupied(ent) => return Err(internal!("duplicate table: {}", ent.key())),
            Entry::Vacant(ent) => {
                ent.insert(table);
            }
        }

        Ok(())
    }
}

impl SchemaProvider for SchemaCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        self.insert_table(name, table)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(None)
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read();
        tables.get(name).cloned()
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}

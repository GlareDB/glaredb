use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion_ext::{errors::ExtensionError, functions::VirtualLister};
use protogen::metastore::types::catalog::CatalogEntry;
use sqlbuiltins::builtins::DEFAULT_CATALOG;

use crate::metastore::catalog::SessionCatalog;

pub struct CatalogLister {
    pub catalog: SessionCatalog,
    pub external: bool,
}

impl CatalogLister {
    pub fn new(catalog: SessionCatalog, external: bool) -> Self {
        CatalogLister { catalog, external }
    }
}

#[async_trait]
impl VirtualLister for CatalogLister {
    async fn list_schemas(&self) -> Result<Vec<String>, ExtensionError> {
        let catalog_state = self.catalog.get_state();
        let mut schemas: Vec<String> = Vec::new();
        for (_id, ent) in &catalog_state.entries {
            let name = ent.get_meta().name.clone();
            match ent {
                CatalogEntry::Schema(_) => schemas.push(name),
                _ => {}
            }
        }
        Ok(schemas)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let catalog_state = self.catalog.get_state();
        let schema_id = self.catalog.resolve_schema(schema).unwrap().meta.id;
        let mut tables: Vec<String> = Vec::new();
        for (_id, ent) in &catalog_state.entries {
            let name = ent.get_meta().name.clone();
            match ent {
                CatalogEntry::Table(_) if ent.get_meta().parent == schema_id => tables.push(name),
                _ => {}
            }
        }
        Ok(tables)
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        let table_entry = self
            .catalog
            .resolve_entry(DEFAULT_CATALOG, schema, table)
            .unwrap();
        match table_entry {
            CatalogEntry::Table(ent) => {
                let cols = ent.get_internal_columns();
                if let Some(cols) = cols {
                    let fields: Vec<Field> = cols
                        .iter()
                        .map(|c| Field::new(c.name.clone(), c.arrow_type.clone(), c.nullable))
                        .collect();
                    Ok(Fields::from(fields))
                } else {
                    return Err(ExtensionError::String("test".to_string()));
                }
            }
            _ => return Err(ExtensionError::String("test".to_string())),
        }
    }
}

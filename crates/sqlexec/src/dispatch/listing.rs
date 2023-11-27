use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion_ext::{errors::ExtensionError, functions::VirtualLister};
use protogen::metastore::types::catalog::CatalogEntry;
use sqlbuiltins::builtins::DEFAULT_CATALOG;

use catalog::session_catalog::SessionCatalog;

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
        for ent in catalog_state.entries.values() {
            if let CatalogEntry::Schema(_) = ent {
                let name = ent.get_meta().name.clone();
                let external = ent.get_meta().external;
                if external == self.external {
                    schemas.push(name)
                }
            }
        }
        Ok(schemas)
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<String>, ExtensionError> {
        let catalog_state = self.catalog.get_state();
        let schema_id = self.catalog.resolve_schema(schema).unwrap().meta.id;
        let mut tables: Vec<String> = Vec::new();
        for ent in catalog_state.entries.values() {
            if let CatalogEntry::Table(_) = ent {
                if ent.get_meta().parent == schema_id {
                    let name = ent.get_meta().name.clone();
                    let external = ent.get_meta().external;
                    if external == self.external {
                        tables.push(name)
                    }
                }
            }
        }
        Ok(tables)
    }

    async fn list_columns(&self, schema: &str, table: &str) -> Result<Fields, ExtensionError> {
        let table_entry = self.catalog.resolve_entry(DEFAULT_CATALOG, schema, table);
        if let Some(CatalogEntry::Table(ent)) = table_entry {
            let external = ent.meta.external;
            if external {
                return Err(ExtensionError::Unimplemented(
                    "list_columns for external tables",
                ));
            } else {
                let cols = ent.get_internal_columns();
                if let Some(cols) = cols {
                    let fields: Vec<Field> = cols
                        .iter()
                        .map(|c| Field::new(c.name.clone(), c.arrow_type.clone(), c.nullable))
                        .collect();
                    Ok(Fields::from(fields))
                } else {
                    return Err(ExtensionError::String(
                        "No such information present".to_string(),
                    ));
                }
            }
        } else {
            return Err(ExtensionError::String("No such table exists".to_string()));
        }
    }
}

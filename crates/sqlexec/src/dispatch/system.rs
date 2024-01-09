use super::{DispatchError, Result};
use catalog::session_catalog::SessionCatalog;
use datafusion::datasource::TableProvider;
use datasources::native::access::NativeTableStorage;
use futures::TryFutureExt;
use protogen::metastore::types::catalog::TableEntry;
use sqlbuiltins::tables::TABLE_REGISTRY;
use std::sync::Arc;

/// Dispatch to builtin system tables.
pub struct SystemTableDispatcher<'a> {
    catalog: &'a SessionCatalog,
    tables: &'a NativeTableStorage,
}

impl<'a> SystemTableDispatcher<'a> {
    pub fn new(catalog: &'a SessionCatalog, tables: &'a NativeTableStorage) -> Self {
        SystemTableDispatcher { catalog, tables }
    }

    pub async fn dispatch(&self, ent: &TableEntry) -> Result<Arc<dyn TableProvider>> {
        let schema_ent = self
            .catalog
            .get_by_oid(ent.meta.parent)
            .ok_or_else(|| DispatchError::MissingObjectWithOid(ent.meta.parent))?;
        let name = &ent.meta.name;
        let schema = &schema_ent.get_meta().name;
        if TABLE_REGISTRY.contains(ent.meta.id) {
            TABLE_REGISTRY
                .dispatch_table(ent.meta.id, self.catalog, self.tables)
                .map_err(|e| DispatchError::String(e.to_string()))
                .await
        } else {
            Err(DispatchError::MissingBuiltinTable {
                schema: schema.to_string(),
                name: name.to_string(),
            })
        }
    }
}

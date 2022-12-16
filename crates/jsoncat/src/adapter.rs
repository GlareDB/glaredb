//! Adapter types for use with datafusion.
use crate::access::AccessMethod;
use crate::catalog::{Catalog, Schema};
use crate::constants::{DEFAULT_CATALOG, INTERNAL_SCHEMA};
use crate::entry::table::TableEntry;
use crate::system::{schemas_memory_table, views_memory_table};
use crate::transaction::Context;
use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use std::any::Any;
use std::sync::Arc;

pub struct CatalogListAdapter<C> {
    provider: Arc<CatalogProviderAdapter<C>>,
}

impl<C: Context> CatalogListAdapter<C> {
    pub fn new(ctx: C, catalog: Arc<Catalog>) -> Self {
        CatalogListAdapter {
            provider: Arc::new(CatalogProviderAdapter { ctx, catalog }),
        }
    }
}

impl<C: Context + 'static> CatalogList for CatalogListAdapter<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!("register_catalog purposely unimplemented")
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![DEFAULT_CATALOG.to_string()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == DEFAULT_CATALOG {
            Some(self.provider.clone())
        } else {
            None
        }
    }
}

pub struct CatalogProviderAdapter<C> {
    catalog: Arc<Catalog>,
    ctx: C,
}

impl<C: Context> CatalogProviderAdapter<C> {
    /// Create a new adapter.
    pub fn new(ctx: C, catalog: Arc<Catalog>) -> Self {
        CatalogProviderAdapter { ctx, catalog }
    }
}

impl<C: Context + 'static> CatalogProvider for CatalogProviderAdapter<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let names = self
            .catalog
            .schemas
            .iter(&self.ctx)
            .map(|ent| ent.name.clone())
            .collect();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schema = self.catalog.schemas.get_entry(&self.ctx, name)?;
        Some(Arc::new(SchemaProviderAdapater {
            catalog: self.catalog.clone(),
            schema,
            ctx: self.ctx.clone(),
        }))
    }
}

pub struct SchemaProviderAdapater<C> {
    catalog: Arc<Catalog>,
    schema: Arc<Schema>,
    ctx: C,
}

impl<C: Context + 'static> SchemaProvider for SchemaProviderAdapater<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self
            .schema
            .tables
            .iter(&self.ctx)
            .map(|ent| ent.name.clone());
        let views = self
            .schema
            .views
            .iter(&self.ctx)
            .map(|ent| ent.name.clone());
        tables.chain(views).collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // TODO: Views
        let ent = self.schema.tables.get_entry(&self.ctx, name)?;
        dispatch_access(&self.ctx, &self.catalog, &ent)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.schema.tables.entry_exists(&self.ctx, name)
            || self.schema.views.entry_exists(&self.ctx, name)
    }
}

/// Return a datafusion table provider based on the table's access method.
fn dispatch_access<C: Context>(
    ctx: &C,
    catalog: &Catalog,
    ent: &TableEntry,
) -> Option<Arc<dyn TableProvider>> {
    match &ent.access {
        AccessMethod::InternalMemory => {
            // Just match on the built-in tables.
            match (ent.schema.as_str(), ent.name.as_str()) {
                (INTERNAL_SCHEMA, "views") => Some(Arc::new(views_memory_table(ctx, catalog))),
                (INTERNAL_SCHEMA, "schemas") => Some(Arc::new(schemas_memory_table(ctx, catalog))),
                _ => None,
            }
        }
        _ => None,
    }
}

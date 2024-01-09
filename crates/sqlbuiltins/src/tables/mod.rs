pub mod glare_catalog;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use catalog::session_catalog::SessionCatalog;
use datafusion::{
    arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema},
    datasource::TableProvider,
};

use datasources::native::access::NativeTableStorage;
use once_cell::sync::Lazy;
use protogen::metastore::types::options::InternalColumnDefinition;

use self::glare_catalog::*;
use crate::errors::BuiltinError;
use crate::errors::Result;

/// A registry of all builtin tables.
pub static TABLE_REGISTRY: Lazy<TableRegistry> = Lazy::new(TableRegistry::new);

#[async_trait]
/// A builtin table.
/// TODO: Do we want something to indicate if a table is persisted in delta?
pub trait BuiltinTable: Sync + Send {
    /// The schema of the table.
    fn schema(&self) -> &'static str;

    /// The name of the table.
    fn name(&self) -> &'static str;

    /// The oid of the table.
    fn oid(&self) -> u32;

    /// The columns of the table.
    fn columns(&self) -> Vec<InternalColumnDefinition>;

    /// Function to build or retrieve the table.
    async fn build_table(
        &self,
        catalog: &SessionCatalog,
        tables: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>>;

    /// The arrow schema of the table.
    fn arrow_schema(&self) -> ArrowSchema {
        ArrowSchema::new(
            self.columns()
                .iter()
                .map(|col| ArrowField::new(&col.name, col.arrow_type.clone(), col.nullable))
                .collect::<Vec<_>>(),
        )
    }
}

/// A registry of builtin tables.
pub struct TableRegistry {
    tables: HashMap<u32, Arc<dyn BuiltinTable>>,
}

impl TableRegistry {
    pub fn new() -> Self {
        let tables: Vec<Arc<dyn BuiltinTable>> = vec![
            // 'glare_catalog' tables
            Arc::new(GlareDatabases),
            Arc::new(GlareTunnels),
            Arc::new(GlareCredentials),
            Arc::new(GlareSchemas),
            Arc::new(GlareTables),
            Arc::new(GlareViews),
            Arc::new(GlareColumns),
            Arc::new(GlareFunctions),
            Arc::new(GlareSShKeys),
            Arc::new(GlareDeploymentMetadata),
            Arc::new(GlareCachedExternalDatabaseTables),
        ];
        Self {
            tables: tables.into_iter().map(|t| (t.oid(), t)).collect(),
        }
    }
    pub fn contains(&self, oid: u32) -> bool {
        self.tables.contains_key(&oid)
    }
    pub fn tables_iter(&self) -> impl Iterator<Item = &Arc<dyn BuiltinTable>> {
        self.tables.values()
    }
    pub async fn dispatch_table(
        &self,
        oid: u32,
        catalog: &SessionCatalog,
        tables: &NativeTableStorage,
    ) -> Result<Arc<dyn TableProvider>> {
        let table = self
            .tables
            .get(&oid)
            .ok_or_else(|| BuiltinError::Internal("invalid oid".to_string()))?;
        table.build_table(catalog, tables).await
    }
}
impl Default for TableRegistry {
    fn default() -> Self {
        Self::new()
    }
}

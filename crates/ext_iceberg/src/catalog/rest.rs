use std::sync::Arc;

use futures::{Stream, stream};
use glaredb_core::catalog::create::{
    CreateAggregateFunctionInfo, CreateScalarFunctionInfo, CreateSchemaInfo,
    CreateTableFunctionInfo, CreateTableInfo, CreateViewInfo,
};
use glaredb_core::catalog::drop::DropInfo;
use glaredb_core::catalog::entry::{CatalogEntry, CatalogEntryType};
use glaredb_core::catalog::{Catalog, Schema};
use glaredb_core::execution::operators::PlannedOperator;
use glaredb_core::execution::planner::OperatorIdGen;
use glaredb_core::storage::storage_manager::{StorageManager, StorageTableId};
use glaredb_error::{DbError, Result};

/// A catalog implementation for the Iceberg REST API.
///
/// This is a stub implementation that will be expanded to support the full
/// Iceberg REST API spec.
///
/// Reference: <https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml>
#[derive(Debug)]
pub struct RestCatalog {
    #[allow(dead_code)]
    base_url: String,
}

impl RestCatalog {
    pub fn new(base_url: String) -> Self {
        RestCatalog { base_url }
    }
}

/// Schema implementation for Iceberg REST catalog
///
/// Reference: https://iceberg.apache.org/spec/#rest-catalog-api-namespace-and-table-operations
#[derive(Debug)]
pub struct RestSchema {
    schema: Arc<CatalogEntry>,
    #[allow(dead_code)]
    base_url: String,
    #[allow(dead_code)]
    namespace: String,
}

impl Catalog for RestCatalog {
    type Schema = RestSchema;

    fn create_schema(&self, _create: &CreateSchemaInfo) -> Result<Arc<Self::Schema>> {
        //

        Err(DbError::new(
            "Iceberg REST catalog create_schema not yet implemented",
        ))
    }

    fn get_schema(&self, _name: &str) -> Result<Option<Arc<Self::Schema>>> {
        //

        Err(DbError::new(
            "Iceberg REST catalog get_schema not yet implemented",
        ))
    }

    fn drop_entry(&self, _drop: &DropInfo) -> Result<Option<Arc<CatalogEntry>>> {
        //

        Err(DbError::new(
            "Iceberg REST catalog drop_entry not yet implemented",
        ))
    }

    fn plan_create_view(
        self: &Arc<Self>,
        _id_gen: &mut OperatorIdGen,
        _schema: &str,
        _create: CreateViewInfo,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_create_view not yet implemented",
        ))
    }

    fn plan_create_table(
        self: &Arc<Self>,
        _storage: &Arc<StorageManager>,
        _id_gen: &mut OperatorIdGen,
        _schema: &str,
        _create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_create_table not yet implemented",
        ))
    }

    fn plan_insert(
        self: &Arc<Self>,
        _storage: &Arc<StorageManager>,
        _id_gen: &mut OperatorIdGen,
        _table: Arc<CatalogEntry>,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_insert not yet implemented",
        ))
    }

    fn plan_create_table_as(
        self: &Arc<Self>,
        _storage: &Arc<StorageManager>,
        _id_gen: &mut OperatorIdGen,
        _schema: &str,
        _create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_create_table_as not yet implemented",
        ))
    }

    fn plan_create_schema(
        self: &Arc<Self>,
        _id_gen: &mut OperatorIdGen,
        _create: CreateSchemaInfo,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_create_schema not yet implemented",
        ))
    }

    fn plan_drop(
        self: &Arc<Self>,
        _storage: &Arc<StorageManager>,
        _id_gen: &mut OperatorIdGen,
        _drop: DropInfo,
    ) -> Result<PlannedOperator> {
        Err(DbError::new(
            "Iceberg REST catalog plan_drop not yet implemented",
        ))
    }

    fn list_schemas(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<Self::Schema>>>> + Sync + Send + 'static {
        //

        stream::once(async move { Ok(vec![]) })
    }
}

impl Schema for RestSchema {
    fn as_entry(&self) -> Arc<CatalogEntry> {
        self.schema.clone()
    }

    fn create_table(
        &self,
        _create: &CreateTableInfo,
        _storage_id: StorageTableId,
    ) -> Result<Arc<CatalogEntry>> {
        //

        Err(DbError::new(
            "Iceberg REST schema create_table not yet implemented",
        ))
    }

    fn create_view(&self, _create: &CreateViewInfo) -> Result<Arc<CatalogEntry>> {
        Err(DbError::new(
            "Iceberg REST schema create_view not yet implemented",
        ))
    }

    fn create_scalar_function(
        &self,
        _create: &CreateScalarFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        Err(DbError::new(
            "Iceberg REST schema create_scalar_function not yet implemented",
        ))
    }

    fn create_aggregate_function(
        &self,
        _create: &CreateAggregateFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        Err(DbError::new(
            "Iceberg REST schema create_aggregate_function not yet implemented",
        ))
    }

    fn create_table_function(
        &self,
        _create: &CreateTableFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        Err(DbError::new(
            "Iceberg REST schema create_table_function not yet implemented",
        ))
    }

    fn get_table_or_view(&self, _name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        //

        Err(DbError::new(
            "Iceberg REST schema get_table_or_view not yet implemented",
        ))
    }

    fn get_table_function(&self, _name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema get_table_function not yet implemented",
        ))
    }

    fn get_inferred_table_function(&self, _path: &str) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema get_inferred_table_function not yet implemented",
        ))
    }

    fn get_function(&self, _name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema get_function not yet implemented",
        ))
    }

    fn get_scalar_function(&self, _name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema get_scalar_function not yet implemented",
        ))
    }

    fn get_aggregate_function(&self, _name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema get_aggregate_function not yet implemented",
        ))
    }

    fn find_similar_entry(
        &self,
        _entry_types: &[CatalogEntryType],
        _name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        Err(DbError::new(
            "Iceberg REST schema find_similar_entry not yet implemented",
        ))
    }

    fn list_entries(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<CatalogEntry>>>> + Sync + Send + 'static {
        //

        stream::once(async move { Ok(vec![]) })
    }

    fn list_tables(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<CatalogEntry>>>> + Sync + Send + 'static {
        //

        stream::once(async move { Ok(vec![]) })
    }
}

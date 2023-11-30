//! Wrappers around native table storage for persisting system tables that need
//! to store data on disk.

use super::access::{NativeTable, NativeTableStorage};
use crate::native::errors::{NativeError, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;
use deltalake::protocol::SaveMode;
use protogen::metastore::types::catalog::TableEntry;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SystemTableStorage<'a> {
    storage: &'a NativeTableStorage,
}

impl<'a> SystemTableStorage<'a> {
    pub fn new(storage: &'a NativeTableStorage) -> Self {
        SystemTableStorage { storage }
    }

    /// Load a system table.
    pub async fn load(&self, ent: &TableEntry) -> Result<PersistedSystemTable> {
        let table = if self.storage.table_exists(ent).await? {
            self.storage.load_table(ent).await?
        } else {
            self.storage.create_table(ent, SaveMode::Overwrite).await?
        };

        unimplemented!()
    }
}

#[derive(Debug)]
pub struct PersistedSystemTable {
    native: NativeTable,
}

#[async_trait]
impl TableProvider for PersistedSystemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.native.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.native.scan(session, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.native.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.native.statistics()
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self.native.insert_exec(input, overwrite))
    }
}

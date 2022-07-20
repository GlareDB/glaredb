use super::{StorageEngine, StorageTransaction};
use crate::store::Store;
use anyhow::Result;
use async_trait::async_trait;
use coretypes::batch::Batch;
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;
use coretypes::stream::{BatchStream, MemoryStream};
use coretypes::vec::ColumnVec;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LocalEngine {
    store: Arc<RwLock<Store>>,
}

impl LocalEngine {
    pub fn new(store: Store) -> LocalEngine {
        LocalEngine {
            store: Arc::new(RwLock::new(store)),
        }
    }
}

impl StorageEngine for LocalEngine {
    type Transaction = LocalTransaction;

    fn begin(&self, interactivity: super::Interactivity) -> Result<Self::Transaction> {
        Ok(LocalTransaction {
            engine: self.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct LocalTransaction {
    engine: LocalEngine,
}

#[async_trait]
impl StorageTransaction for LocalTransaction {
    async fn commit(self) -> Result<()> {
        Ok(())
    }

    async fn create_relation(&self, name: &str, schema: RelationSchema) -> Result<()> {
        let mut store = self.engine.store.write();
        store.create_relation(name, schema)
    }

    async fn delete_relation(&self, name: &str) -> Result<()> {
        let mut store = self.engine.store.write();
        store.delete_relation(name)
    }

    async fn get_relation(&self, name: &str) -> Result<Option<RelationSchema>> {
        let store = self.engine.store.read();
        store.get_table_schema(name)
    }

    async fn insert(&self, table: &str, row: &Row) -> Result<()> {
        let mut store = self.engine.store.write();
        store.insert(table, row)
    }

    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        limit: usize,
    ) -> Result<BatchStream> {
        // TODO: Use projections, filters, etc.
        // TODO: Actually stream.
        let store = self.engine.store.read();
        let batch = store.scan(table, filter, limit)?;
        let stream = MemoryStream::from_batch(batch);

        Ok(Box::pin(stream))
    }
}

use crate::store::Store;
use crate::{Result, StoreError};
use async_trait::async_trait;
use coretypes::column::{ColumnVec, NullableColumnVec};
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;
use futures::stream::{self, Stream};
use parking_lot::RwLock;
use std::sync::Arc;

pub type BatchStream = Box<dyn Stream<Item = Result<Vec<NullableColumnVec>>>>;

/// Client interface to an underlying storage engine.
#[async_trait]
pub trait Client: Clone {
    /// Create a new relation with the given name.
    async fn create_relation(&mut self, name: &str, schema: RelationSchema) -> Result<()>;

    /// Delete a relation with the given name. Errors if the relation does not
    /// exist.
    async fn delete_relation(&mut self, name: &str) -> Result<()>;

    /// Get a relation schema.
    async fn get_relation(&self, name: &str) -> Result<Option<RelationSchema>>;

    async fn insert(&mut self, table: &str, row: &Row) -> Result<()>;

    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        limit: usize,
    ) -> Result<BatchStream>;
}

#[derive(Debug, Clone)]
pub struct LocalClient {
    store: Arc<RwLock<Store>>,
}

impl LocalClient {
    pub fn new(store: Store) -> LocalClient {
        LocalClient {
            store: Arc::new(RwLock::new(store)),
        }
    }
}

#[async_trait]
impl Client for LocalClient {
    async fn create_relation(&mut self, name: &str, schema: RelationSchema) -> Result<()> {
        let mut store = self.store.write();
        store.create_relation(name, schema)
    }

    async fn delete_relation(&mut self, name: &str) -> Result<()> {
        let mut store = self.store.write();
        store.delete_relation(name)
    }

    async fn get_relation(&self, name: &str) -> Result<Option<RelationSchema>> {
        unimplemented!()
    }

    async fn insert(&mut self, table: &str, row: &Row) -> Result<()> {
        let mut store = self.store.write();
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
        let store = self.store.read();
        let batch = store.scan(table);
        let stream = stream::iter(std::iter::once(batch));

        Ok(Box::new(stream))
    }
}

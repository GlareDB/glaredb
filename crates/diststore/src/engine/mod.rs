use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;
use coretypes::stream::{BatchStream, MemoryStream};
use futures::stream::{self, Stream};

pub mod local;

/// Transaction interactivity level.
#[derive(Debug, PartialEq)]
pub enum Interactivity {
    None,
    Interactive,
}

pub trait StorageEngine: Clone {
    type Transaction: StorageTransaction;

    fn begin(&self, interactivity: Interactivity) -> Result<Self::Transaction>;
}

/// Client interface to an underlying storage engine.
#[async_trait]
pub trait StorageTransaction: Sync + Send {
    async fn commit(self) -> Result<()>;

    /// Create a new relation with the given name.
    async fn create_relation(&mut self, name: &str, schema: RelationSchema) -> Result<()>;

    /// Delete a relation with the given name. Errors if the relation does not
    /// exist.
    async fn delete_relation(&mut self, name: &str) -> Result<()>;

    /// Get a relation schema.
    async fn get_relation(&self, name: &str) -> Result<Option<RelationSchema>>;

    /// Insert a row.
    async fn insert(&mut self, table: &str, row: &Row) -> Result<()>;

    /// Scan a table.
    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        limit: usize,
    ) -> Result<BatchStream>;
}

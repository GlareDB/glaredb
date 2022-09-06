use crate::arrow::chunk::{Chunk, TypeSchema};
use crate::arrow::expr::ScalarExpr;
use crate::arrow::queryexec::PinnedChunkStream;
use crate::arrow::row::Row;
use crate::arrow::scalar::ScalarOwned;
use crate::errors::Result;
use async_trait::async_trait;

pub mod memory;

#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Column indices for determining the primary key for records.
    pk_idxs: Vec<usize>,
    /// Types for each column.
    schema: TypeSchema,
}

/// A unique reference to a single row.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PkRowRef(Box<[ScalarOwned]>);

// TODO: Index scan.
// TODO: Start/end for scan.
#[async_trait]
pub trait QueryDataSource: Sync + Send + std::fmt::Debug {
    /// Get a single row with the given key.
    async fn get(&self, table: &str, key: PkRowRef) -> Result<Option<Row>>;

    /// Scan from a table, returning a stream of chunks.
    ///
    /// An optional filtering expression and projection list may be provided.
    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        projection: Option<Vec<usize>>,
    ) -> Result<PinnedChunkStream>;
}

// TODO: Index create.
// TODO: Alter table.
#[async_trait]
pub trait MutableDataSource: QueryDataSource {
    /// Allocate a new table. Should error if there's an existing table.
    async fn allocate_table(&self, table: &str, schema: TableSchema) -> Result<()>;

    /// Deallocate an existing table. Should error if the table does not exist.
    async fn deallocate_table(&self, table: &str) -> Result<()>;

    /// Insert a chunk into in the table.
    async fn insert_chunk(&self, table: &str, chunk: Chunk) -> Result<()>;

    /// Delete a single row.
    async fn delete_one(&self, table: &str, key: PkRowRef) -> Result<()>;

    /// Delete many rows matching the given filter expression.
    async fn delete_many(&self, table: &str, filter: ScalarExpr) -> Result<()>;
}

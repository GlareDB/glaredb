use crate::repr::df::{DataFrame, Schema};
use crate::repr::expr::ScalarExpr;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

pub type TableKey = String;

/// An async stream of dataframes.
///
/// Every dataframe in the stream must have the same schema.
pub type DataFrameStream = Pin<Box<dyn Stream<Item = Result<DataFrame>> + Send>>;

/// A readable source is able read dataframes and dataframe schemas.
#[async_trait]
pub trait ReadableSource: Sync + Send {
    /// Read from a source, returning a stream of dataframes.
    ///
    /// An optional filter expression can be provided.
    ///
    /// Returns `None` if the table doesn't exist.
    async fn scan(
        &self,
        table: &TableKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>>;

    /// Get the schema for a given table.
    ///
    /// Returns `None` if the table doesn't exist.
    async fn get_schema(&self, table: &TableKey) -> Result<Option<Schema>>;
}

/// A writeable source is able to write dataframes to underlying tables, as well
/// as create, alter, and delete tables.
#[async_trait]
pub trait WriteableSource: Sync + Send {
    /// Create a table with the given schema. Errors if the table already
    /// exists.
    async fn create_table(&self, table: TableKey, schema: Schema) -> Result<()>;

    /// Drop a table. Errors if the table doesn't exist.
    async fn drop_table(&self, table: &TableKey) -> Result<()>;

    /// Insert data into a table. Errors if the table doesn't exist.
    async fn insert(&self, table: &TableKey, data: DataFrame) -> Result<()>;
}

/// Execute a reading operation.
#[async_trait]
pub trait ReadExecutor<R: ReadableSource> {
    /// Execute a read against `source`, returning a data stream.
    async fn execute_read(self, source: &R) -> Result<DataFrameStream>;
}

/// Execute a writing operation.
#[async_trait]
pub trait WriteExecutor<W: ReadableSource + WriteableSource> {
    /// Execute a write against `source`, returning an optional data stream.
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>>;
}

/// A simple in-memory stream holding already materialized dataframes.
#[derive(Debug)]
pub struct MemoryStream {
    dfs: VecDeque<DataFrame>,
}

impl MemoryStream {
    pub fn empty() -> Self {
        MemoryStream {
            dfs: VecDeque::new(),
        }
    }

    pub fn one(df: DataFrame) -> Self {
        let mut dfs = VecDeque::with_capacity(1);
        dfs.push_back(df);
        MemoryStream { dfs }
    }

    pub fn with_dataframes(dfs: impl IntoIterator<Item = DataFrame>) -> Self {
        MemoryStream {
            dfs: dfs.into_iter().collect(),
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<DataFrame>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.dfs.pop_front() {
            Some(df) => Poll::Ready(Some(Ok(df))),
            None => Poll::Ready(None),
        }
    }
}

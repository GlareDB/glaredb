use crate::arrow::expr::ScalarExpr;
use crate::arrow::queryexec::PinnedChunkStream;
use crate::errors::Result;
use async_trait::async_trait;

#[async_trait]
pub trait QueryDataSource: Sync + Send + std::fmt::Debug {
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

#[async_trait]
pub trait MutableDataSource: QueryDataSource {}

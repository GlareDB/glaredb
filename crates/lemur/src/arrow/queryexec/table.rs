use crate::arrow::datasource::QueryDataSource;
use crate::arrow::expr::ScalarExpr;
use crate::errors::Result;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct TableScan {
    table: String,
    filter: Option<ScalarExpr>,
    projection: Option<Vec<usize>>,
    source: Box<dyn QueryDataSource>,
}

impl QueryExecutor for TableScan {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}

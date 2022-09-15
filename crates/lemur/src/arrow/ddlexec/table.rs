use crate::arrow::datasource::{MutableDataSource, TableSchema};
use crate::errors::Result;

use super::{DdlExecutor, PinnedDdlFuture};

#[derive(Debug)]
pub struct AllocateTable {
    table: String,
    schema: TableSchema,
    source: Box<dyn MutableDataSource>,
}

impl AllocateTable {
    pub fn new(
        table: String,
        schema: TableSchema,
        source: Box<dyn MutableDataSource>,
    ) -> AllocateTable {
        AllocateTable {
            table,
            schema,
            source,
        }
    }

    async fn execute_inner(self) -> Result<()> {
        self.source.allocate_table(&self.table, self.schema).await
    }
}

impl DdlExecutor for AllocateTable {
    fn execute(self) -> Result<PinnedDdlFuture> {
        Ok(Box::pin(self.execute_inner()))
    }
}

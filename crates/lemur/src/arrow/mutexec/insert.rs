use crate::arrow::datasource::MutableDataSource;
use crate::arrow::queryexec::QueryExecutor;
use crate::errors::Result;
use futures::StreamExt;

use super::{MutExecutor, PinnedMutFuture};

#[derive(Debug)]
pub struct Insert {
    table: String,
    data: Box<dyn QueryExecutor>,
    source: Box<dyn MutableDataSource>,
}

impl Insert {
    pub fn new(
        table: String,
        data: Box<dyn QueryExecutor>,
        source: Box<dyn MutableDataSource>,
    ) -> Insert {
        Insert {
            table,
            data,
            source,
        }
    }

    async fn execute_inner(self) -> Result<()> {
        let mut stream = self.data.execute_boxed()?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            self.source.insert_chunk(&self.table, chunk).await?;
        }
        Ok(())
    }
}

impl MutExecutor for Insert {
    fn execute(self) -> Result<PinnedMutFuture> {
        Ok(Box::pin(self.execute_inner()))
    }
}

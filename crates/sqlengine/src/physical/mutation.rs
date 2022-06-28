use super::PhysicalOperator;
use crate::catalog::{Catalog, ResolvedTableReference, TableSchema};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::batch::Batch;
use coretypes::stream::{BatchStream, MemoryStream};
use diststore::engine::StorageTransaction;
use futures::{future, Future, FutureExt};
use std::sync::Arc;
use std::task::Poll;

// TODO: These should all return relevant info upon success. Probably need to
// extend stream building to allow returning things other than batches.

#[derive(Debug)]
pub struct CreateTable {
    pub table: TableSchema,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalOperator<T> for CreateTable {
    async fn execute_stream(self, tx: &T) -> Result<Option<BatchStream>> {
        let name = self.table.reference.to_string();
        tx.create_relation(&name, self.table.schema).await?;
        Ok(None)
    }
}

use super::{PhysicalOperator, PhysicalPlan};
use crate::catalog::{Catalog, ResolvedTableReference, TableSchema};
use crate::engine::Transaction;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::batch::Batch;
use coretypes::stream::{BatchStream, MemoryStream};
use diststore::engine::StorageTransaction;
use futures::{future, Future, FutureExt, Stream, StreamExt};
use log::debug;
use std::sync::Arc;
use std::task::Poll;

// TODO: These should all return relevant info upon success. Probably need to
// extend stream building to allow returning things other than batches.

#[derive(Debug)]
pub struct CreateTable {
    pub table: TableSchema,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for CreateTable {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        tx.create_table(self.table)?;
        Ok(None)
    }
}

#[derive(Debug)]
pub struct Insert {
    pub table: ResolvedTableReference,
    pub input: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Insert {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        debug!("executing insert on table: {}", self.table);
        let mut input = self
            .input
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("input did not return stream"))?;

        for result in input.next().await {
            let batch = result?.into_shrunk_batch();

            // TODO: Bulk insert.
            for row in batch.row_iter() {
                tx.insert(&self.table, &row).await?;
            }
        }

        Ok(None)
    }
}

use super::read::ReadPlan;
use crate::repr::df::{DataFrame, Schema};
use crate::runtime::datasource::{
    DataFrameStream, ReadExecutor, ReadableSource, TableKey, WriteExecutor, WriteableSource,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use log::error;

#[derive(Debug)]
pub enum WritePlan {
    Insert(Insert),
    DropTable(DropTable),
    CreateTable(CreateTable),
}

#[async_trait]
impl<W: ReadableSource + WriteableSource> WriteExecutor<W> for WritePlan {
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>> {
        match self {
            WritePlan::Insert(n) => n.execute_write(source).await,
            WritePlan::DropTable(n) => n.execute_write(source).await,
            WritePlan::CreateTable(n) => n.execute_write(source).await,
        }
    }
}

#[derive(Debug)]
pub struct Insert {
    pub table: TableKey,
    pub input: ReadPlan,
}

#[async_trait]
impl<W: ReadableSource + WriteableSource> WriteExecutor<W> for Insert {
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>> {
        let mut input = self.input.execute_read(source).await?;

        // Note that we're inserting as we receive from the input. It's up to
        // the writeable source implementation to ensure this is transactional
        // (if required).
        while let Some(input_result) = input.next().await {
            let df = input_result?;
            source.insert(&self.table, df).await?;
        }

        Ok(None)
    }
}

#[derive(Debug)]
pub struct DropTable {
    pub table: TableKey,
}

#[async_trait]
impl<W: ReadableSource + WriteableSource> WriteExecutor<W> for DropTable {
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>> {
        source.drop_table(&self.table).await?;
        Ok(None)
    }
}

#[derive(Debug)]
pub struct CreateTable {
    pub table: TableKey,
    pub schema: Schema,
}

#[async_trait]
impl<W: ReadableSource + WriteableSource> WriteExecutor<W> for CreateTable {
    async fn execute_write(self, source: &W) -> Result<Option<DataFrameStream>> {
        source.create_table(self.table, self.schema).await?;
        Ok(None)
    }
}

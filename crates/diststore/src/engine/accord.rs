use super::local::LocalEngine;
use super::{Interactivity, StorageEngine, StorageTransaction};
use crate::accord;
use crate::accord::keys::{ExactString, KeySet};
use crate::accord::server::Client;
use crate::accord::timestamp::Timestamp;
use crate::accord::{ComputeData, Executor, ReadData, WriteData};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::batch::{Batch, BatchError, BatchRepr};
use coretypes::datatype::{RelationSchema, Row};
use coretypes::expr::ScalarExpr;
use coretypes::stream::{BatchStream, MemoryStream};
use coretypes::vec::ColumnVec;
use futures::executor;
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteCommand {
    CreateRelation {
        table: String,
        schema: RelationSchema,
    },
    DeleteRelation {
        table: String,
    },
    InsertRow {
        table: String,
        row: Row,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteCommandResponse {
    CreateRelationOk,
    DeleteRelationOk,
    InsertRowOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadCommand {
    GetRelation {
        table: String,
    },
    ScanRelation {
        table: String,
        filter: Option<ScalarExpr>,
        limit: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadCommandResponse {
    GetRelationOk { schema: Option<RelationSchema> },
    ScanRelationOk { columns: Vec<Vec<ColumnVec>> },
}

#[derive(Debug, Clone)]
pub struct AccordEngine {
    client: Client<ExactString>,
}

impl StorageEngine for AccordEngine {
    type Transaction = AccordTransaction;

    fn begin(&self, interactivity: Interactivity) -> Result<Self::Transaction> {
        Ok(AccordTransaction::new(self.client.clone()))
    }
}

pub struct AccordTransaction {
    client: Client<ExactString>,
}

impl AccordTransaction {
    fn new(client: Client<ExactString>) -> Self {
        AccordTransaction { client }
    }

    fn keyset_from_table_name(&self, table: &str) -> KeySet<ExactString> {
        KeySet::from_key(table.into())
    }
}

#[async_trait]
impl StorageTransaction for AccordTransaction {
    async fn commit(self) -> Result<()> {
        Ok(())
    }

    async fn create_relation(&self, table: &str, schema: RelationSchema) -> Result<()> {
        // TODO: Deserialize response.
        let _ = self
            .client
            .write(
                self.keyset_from_table_name(table),
                serialize(&WriteCommand::CreateRelation {
                    table: table.to_string(),
                    schema,
                })?,
            )
            .await?;
        Ok(())
    }

    async fn delete_relation(&self, table: &str) -> Result<()> {
        // TODO: Deserialize response.
        let _ = self
            .client
            .write(
                self.keyset_from_table_name(table),
                serialize(&WriteCommand::DeleteRelation {
                    table: table.to_string(),
                })?,
            )
            .await?;
        Ok(())
    }

    async fn get_relation(&self, table: &str) -> Result<Option<RelationSchema>> {
        let resp = self
            .client
            .read(
                self.keyset_from_table_name(table),
                serialize(&ReadCommand::GetRelation {
                    table: table.to_string(),
                })?,
            )
            .await?;
        match deserialize::<ReadCommandResponse>(&resp.data)? {
            ReadCommandResponse::GetRelationOk { schema } => Ok(schema),
            resp => Err(anyhow!("invalid read resp: {:?}", resp)),
        }
    }

    async fn insert(&self, table: &str, row: &Row) -> Result<()> {
        // TODO: Deserialize response.
        // TODO: Try not to clone the row.
        let _ = self
            .client
            .write(
                self.keyset_from_table_name(table),
                serialize(&WriteCommand::InsertRow {
                    table: table.to_string(),
                    row: row.clone(),
                })?,
            )
            .await?;
        Ok(())
    }

    async fn scan(
        &self,
        table: &str,
        filter: Option<ScalarExpr>,
        limit: usize,
    ) -> Result<BatchStream> {
        let resp = self
            .client
            .read(
                self.keyset_from_table_name(table),
                serialize(&ReadCommand::ScanRelation {
                    table: table.to_string(),
                    filter,
                    limit,
                })?,
            )
            .await?;
        match deserialize::<ReadCommandResponse>(&resp.data)? {
            ReadCommandResponse::ScanRelationOk { columns } => {
                // TODO: Don't collect.
                let batches = columns
                    .into_iter()
                    .map(|batch_cols| Batch::from_columns(batch_cols))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Box::pin(MemoryStream::from_batches(batches)))
            }
            resp => Err(anyhow!("invalid read resp: {:?}", resp)),
        }
    }
}

/// Execute commands sent through the accord protocal.
#[derive(Debug)]
pub struct AccordExecutor {
    local: LocalEngine,
}

impl AccordExecutor {
    pub fn new(local: LocalEngine) -> AccordExecutor {
        AccordExecutor { local }
    }
}

impl Executor<ExactString> for AccordExecutor {
    type Error = anyhow::Error;

    fn read(
        &self,
        ts: &Timestamp,
        tx: &accord::transaction::Transaction<ExactString>,
    ) -> crate::accord::Result<ReadData, Self::Error> {
        // Immediately return if this is a write tx.
        // TODO: Write transactions may include read commands in the future.
        if tx.is_write_tx() {
            return Ok(ReadData::default());
        }
        let command = deserialize::<ReadCommand>(tx.get_command())?;
        let local_tx = self.local.begin(Interactivity::None)?; // TODO: Resume interactive.
        let resp: ReadCommandResponse = match command {
            ReadCommand::GetRelation { table } => {
                let schema = executor::block_on(local_tx.get_relation(&table))?;
                ReadCommandResponse::GetRelationOk { schema }
            }
            ReadCommand::ScanRelation {
                table,
                filter,
                limit,
            } => {
                // TODO: Implement streaming.
                let stream = executor::block_on(local_tx.scan(&table, filter, limit))?;
                // Buffer all batches. Each batch is transformed into a vector
                // of columnvecs to facilitate serializing.
                let columns = executor::block_on(
                    stream
                        .map(|result| match result {
                            Ok(batch) => {
                                let batch = batch.into_shrunk_batch();
                                let cols: Vec<_> = batch
                                    .columns
                                    .iter()
                                    .map(|col| ColumnVec::clone(col))
                                    .collect();
                                Ok(cols)
                            }
                            Err(e) => Err(e),
                        })
                        .collect::<Vec<_>>(),
                );
                let columns = columns.into_iter().collect::<Result<Vec<_>>>()?;
                ReadCommandResponse::ScanRelationOk { columns }
            }
        };

        Ok(ReadData {
            data: serialize(&resp)?,
        })
    }

    fn compute(
        &self,
        data: &ReadData,
        ts: &Timestamp,
        tx: &accord::transaction::Transaction<ExactString>,
    ) -> crate::accord::Result<ComputeData, Self::Error> {
        // Currently a no-op.
        Ok(ComputeData {
            data: data.data.clone(),
        })
    }

    fn write(
        &self,
        data: &ComputeData,
        ts: &Timestamp,
        tx: &accord::transaction::Transaction<ExactString>,
    ) -> crate::accord::Result<WriteData, Self::Error> {
        let command = deserialize::<WriteCommand>(tx.get_command())?;
        let local_tx = self.local.begin(Interactivity::None)?;
        let resp: WriteCommandResponse = match command {
            WriteCommand::CreateRelation { table, schema } => {
                executor::block_on(local_tx.create_relation(&table, schema))?;
                WriteCommandResponse::CreateRelationOk
            }
            WriteCommand::DeleteRelation { table } => {
                executor::block_on(local_tx.delete_relation(&table))?;
                WriteCommandResponse::DeleteRelationOk
            }
            WriteCommand::InsertRow { table, row } => {
                executor::block_on(local_tx.insert(&table, &row))?;
                WriteCommandResponse::InsertRowOk
            }
        };

        Ok(WriteData {
            data: serialize(&resp)?,
        })
    }
}

fn serialize<T: Serialize>(command: &T) -> Result<Vec<u8>> {
    Ok(bincode::serialize(command)?)
}

fn deserialize<'de, T: Deserialize<'de>>(command: &'de [u8]) -> Result<T> {
    Ok(bincode::deserialize(command)?)
}

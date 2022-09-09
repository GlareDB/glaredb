use anyhow::{Result, anyhow};
use std::{sync::Arc, net::SocketAddr};

use async_trait::async_trait;
use lemur::{execute::stream::source::{DataSource, ReadTx, WriteTx, DataFrameStream, MemoryStream}, repr::{relation::{RelationKey, PrimaryKeyIndices}, df::{Schema, DataFrame}, expr::ScalarExpr}};
use crate::{client::ConsensusClient, message::{Response, DataSourceRequest, DataSourceResponse, ReadTxRequest, ReadTxResponse, WriteTxRequest, ScanRequest, InsertRequest}, repr::NodeId, rpc::pb::GetSchemaRequest};

#[derive(Clone)]
pub struct RaftClientSource {
    pub(self) inner: Arc<ConsensusClient>,
}

impl RaftClientSource {
    pub fn new(leader_id: NodeId, leader_addr: SocketAddr) -> Self {
        let url = format!("http://{}", leader_addr);
        RaftClientSource {
            inner: Arc::new(ConsensusClient::new(leader_id, url)),
        }
    }

    pub fn from_client(client: ConsensusClient) -> Self {
        RaftClientSource {
            inner: Arc::new(client),
        }
    }
}

#[async_trait]
impl DataSource for RaftClientSource {
    type Tx = TxClient;

    async fn begin(&self) -> Result<Self::Tx> {
        println!("begin");
        let resp = self.inner.write(DataSourceRequest::Begin.into()).await?;

        match resp.data {
            Response::DataSource(DataSourceResponse::Begin(tx_id)) => {
                Ok(TxClient {
                    client: self.clone(),
                    tx_id,
                })
            }
            _ => Err(anyhow!("unexpected response: {:?}", resp)),
        }
    }
}

pub struct TxClient {
    client: RaftClientSource,
    #[allow(dead_code)]
    tx_id: u64,
}

#[async_trait]
impl ReadTx for TxClient {
    async fn get_schema(&self, _table: &RelationKey) -> Result<Option<Schema>> {
        let resp = self.client.inner.read(
            ReadTxRequest::GetSchema(GetSchemaRequest {
                table: _table.clone(),
            })
        ).await.unwrap();

        match resp {
            ReadTxResponse::TableSchema(schema) => Ok(schema),
            _ => Err(anyhow!("unexpected response: {:?}", resp)),
        }
    }

    async fn scan(
        &self,
        table: &RelationKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>> {
        let resp = self.client.inner.read(
            ReadTxRequest::Scan(ScanRequest {
                table: table.to_string(),
                filter,
            })
        ).await.unwrap();

        match resp {
            ReadTxResponse::Scan(Some(chunks)) => Ok(Some(Box::pin(MemoryStream::with_dataframes(chunks)))),
            ReadTxResponse::Scan(None) => Ok(None),
            _ => Err(anyhow!("unexpected response: {:?}", resp)),
        }
    }
}

#[async_trait]
impl WriteTx for TxClient {
    async fn commit(self) -> Result<()> {
        todo!();
    }

    async fn rollback(self) -> Result<()> {
        todo!();
    }

    async fn allocate_table(&self, table: RelationKey, schema: Schema) -> Result<()> {
        self.client.inner.write(
            WriteTxRequest::AllocateTable(table, schema).into()
        ).await.unwrap();


        Ok(())
    }

    async fn deallocate_table(&self, _table: &RelationKey) -> Result<()> {
        todo!();
    }

    async fn insert(
        &self,
        table: &RelationKey,
        pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<()> {
         self.client.inner.write(
            WriteTxRequest::Insert(InsertRequest {
                table: table.clone(),
                pk_idxs: pk_idxs.to_vec(),
                data,
            }).into()
        ).await.unwrap();

        Ok(())
    }
}

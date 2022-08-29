use crate::wal::{LemurOp, Wal};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use lemur::execute::stream::source::{DataFrameStream, DataSource, ReadTx, WriteTx};
use lemur::repr::df::{DataFrame, Schema};
use lemur::repr::expr::ScalarExpr;
use lemur::repr::relation::{PrimaryKeyIndices, RelationKey};
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[async_trait]
impl<S: DataSource + 'static> DataSource for Wal<S> {
    type Tx = WalTx<S>;

    async fn begin(&self) -> Result<Self::Tx, anyhow::Error> {
        static TX_ID: AtomicU64 = AtomicU64::new(0);
        let tx_id = TX_ID.fetch_add(1, Ordering::Relaxed);
        self.send_op(LemurOp::Begin { tx_id }).await?;

        let tx = self.get_source().begin().await?;
        Ok(WalTx {
            wal: self.clone(),
            tx_id,
            tx,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WalTx<S: DataSource> {
    wal: Wal<S>,
    tx_id: u64,
    tx: S::Tx,
}

#[async_trait]
impl<S: DataSource> ReadTx for WalTx<S> {
    async fn get_schema(&self, table: &RelationKey) -> Result<Option<Schema>, anyhow::Error> {
        let schema = self.tx.get_schema(table).await?;
        Ok(schema)
    }

    async fn scan(
        &self,
        table: &RelationKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>, anyhow::Error> {
        let stream = self.tx.scan(table, filter).await?;
        Ok(stream)
    }
}

#[async_trait]
impl<S: DataSource + 'static> WriteTx for WalTx<S> {
    async fn commit(self) -> Result<(), anyhow::Error> {
        self.wal
            .send_op(LemurOp::Commit { tx_id: self.tx_id })
            .await?;
        self.tx.commit().await
    }
    async fn rollback(self) -> Result<(), anyhow::Error> {
        self.wal
            .send_op(LemurOp::Rollback { tx_id: self.tx_id })
            .await?;
        self.tx.rollback().await
    }

    async fn allocate_table(
        &self,
        table: RelationKey,
        schema: Schema,
    ) -> Result<(), anyhow::Error> {
        self.wal
            .send_op(LemurOp::AllocateTable {
                tx_id: self.tx_id,
                table: table.clone(),
                schema: schema.clone(),
            })
            .await?;
        self.tx.allocate_table(table, schema).await
    }

    async fn deallocate_table(&self, table: &RelationKey) -> Result<(), anyhow::Error> {
        self.wal
            .send_op(LemurOp::DeallocateTable {
                tx_id: self.tx_id,
                table: table.clone(),
            })
            .await?;
        self.tx.deallocate_table(table).await
    }

    async fn insert(
        &self,
        table: &RelationKey,
        pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<(), anyhow::Error> {
        self.wal
            .send_op(LemurOp::Insert {
                tx_id: self.tx_id,
                table: table.clone(),
                pk_idxs: pk_idxs.to_vec(),
                data: data.clone(),
            })
            .await?;
        self.tx.insert(table, pk_idxs, data).await
    }
}

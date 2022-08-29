use crate::wal::Wal;
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
impl<S: DataSource> DataSource for Wal<S> {
    type Tx = WalTx<S>;

    async fn begin(&self) -> Result<Self::Tx, anyhow::Error> {
        let tx = self.get_source().begin().await?;
        static TX_ID: AtomicU64 = AtomicU64::new(0);
        Ok(WalTx {
            wal: self.clone(),
            tx_id: TX_ID.fetch_add(1, Ordering::Relaxed),
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
impl<S: DataSource> WriteTx for WalTx<S> {
    async fn commit(self) -> Result<(), anyhow::Error> {
        unimplemented!()
    }
    async fn rollback(self) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    async fn allocate_table(
        &self,
        table: RelationKey,
        schema: Schema,
    ) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    async fn deallocate_table(&self, table: &RelationKey) -> Result<(), anyhow::Error> {
        unimplemented!()
    }

    async fn insert(
        &self,
        table: &RelationKey,
        pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<(), anyhow::Error> {
        unimplemented!()
    }
}

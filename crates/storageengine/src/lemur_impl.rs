use anyhow::Result;
use async_trait::async_trait;
use lemur::execute::stream::source::{DataFrameStream, DataSource, MemoryStream, ReadTx, WriteTx};
use lemur::repr::df::{DataFrame, Schema};
use lemur::repr::expr::ScalarExpr;
use lemur::repr::relation::{PrimaryKeyIndices, RelationKey};

use crate::rocks::{RocksStore, StorageTx};

#[async_trait]
impl DataSource for RocksStore {
    type Tx = StorageTx;

    async fn begin(&self) -> Result<StorageTx> {
        Ok(self.begin())
    }
}

#[async_trait]
impl ReadTx for StorageTx {
    async fn get_schema(&self, table: &RelationKey) -> Result<Option<Schema>> {
        let maybe_schema = self.read_schema(table.clone())?;
        Ok(maybe_schema)
    }

    async fn scan(
        &self,
        table: &RelationKey,
        filter: Option<ScalarExpr>,
    ) -> Result<Option<DataFrameStream>> {
        let df = self.scan_all(table.clone(), &[], filter)?;
        Ok(Some(Box::pin(MemoryStream::one(df))))
    }
}

#[async_trait]
impl WriteTx for StorageTx {
    async fn commit(self) -> Result<()> {
        self.commit()?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        self.abort()?;
        Ok(())
    }

    async fn allocate_table(&self, table: RelationKey, schema: Schema) -> Result<()> {
        self.store_schema(table, schema)?;
        Ok(())
    }

    async fn deallocate_table(&self, _table: &RelationKey) -> Result<()> {
        todo!() // Eventually
    }

    async fn insert(
        &self,
        table: &RelationKey,
        pk_idxs: PrimaryKeyIndices<'_>,
        data: DataFrame,
    ) -> Result<()> {
        for row_ref in data.iter_row_refs() {
            self.insert(table.clone(), pk_idxs, row_ref.into_row())?;
        }
        Ok(())
    }
}

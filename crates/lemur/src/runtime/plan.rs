use crate::repr::df::DataFrame;
use crate::repr::expr::{ColumnScalarExprs, ScalarExpr};
use crate::runtime::datasource::{
    DataFrameStream, MemoryStream, ReadExecutor, ReadableSource, TableKey,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bitvec::vec::BitVec;
use futures::{Stream, StreamExt};
use log::error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const STREAM_BUFFER: usize = 16;

/// A plan for executing a read against some data source.
#[derive(Debug)]
pub enum ReadPlan {
    Filter(Filter),
    ScanSource(ScanSource),
    Values(Values),
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for ReadPlan {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        match self {
            ReadPlan::Filter(n) => n.execute_read(source).await,
            ReadPlan::ScanSource(n) => n.execute_read(source).await,
            ReadPlan::Values(n) => n.execute_read(source).await,
        }
    }
}

macro_rules! match_send_err {
    ($item:expr, $tx:ident) => {
        match $item {
            Ok(item) => item,
            Err(e) => {
                if let Err(e) = $tx.send(Err(e)).await {
                    error!("failed to send error to channel, original error: {:?}", e);
                }
                return;
            }
        }
    };
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<ReadPlan>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Filter {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let mut input = self.input.execute_read(source).await?;
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            while let Some(df) = input.next().await {
                let df = match_send_err!(df, tx);
                let evaled = match_send_err!(self.predicate.evaluate(&df), tx);
                let bools = match_send_err!(
                    evaled
                        .as_ref()
                        .downcast_bool_vec()
                        .ok_or(anyhow!("vec not a bool vec")),
                    tx
                );

                // TODO: Turn boolvec into bitvec to avoid doing this.
                let mut mask = BitVec::with_capacity(bools.len());
                // TODO: How to handle nulls?
                for v in bools.iter_values() {
                    mask.push(*v);
                }

                let df = df.filter(&mask);
                if let Err(_) = tx.send(df).await {
                    error!("failed to send filtered dataframe to channel");
                };
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
pub struct ScanSource {
    pub table: TableKey,
    pub filter: Option<ScalarExpr>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for ScanSource {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        match source.scan(&self.table, self.filter).await? {
            Some(stream) => Ok(Box::pin(stream)),
            None => Err(anyhow!("missing table: {}", self.table)),
        }
    }
}

/// Produce in memory values.
#[derive(Debug)]
pub struct Values {
    /// Column-oriented values.
    pub values: Vec<ColumnScalarExprs>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Values {
    async fn execute_read(self, _source: &R) -> Result<DataFrameStream> {
        let columns = self
            .values
            .into_iter()
            .map(|exprs| exprs.evaluate_constant())
            .collect::<Result<Vec<_>>>()?;
        let df = DataFrame::from_columns(columns)?;
        Ok(Box::pin(MemoryStream::one(df)))
    }
}

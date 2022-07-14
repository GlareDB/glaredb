use crate::repr::df::groupby::{Accumulator, SortedGroupByDataFrame};
use crate::repr::df::DataFrame;
use crate::repr::expr::ScalarExpr;
use crate::repr::value::{Row, Value};
use crate::runtime::datasource::{
    DataFrameStream, MemoryStream, ReadExecutor, ReadableSource, TableKey,
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bitvec::vec::BitVec;
use futures::{Stream, StreamExt};
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const STREAM_BUFFER: usize = 16;

#[derive(Debug)]
pub enum ReadPlan {
    Debug(Debug),
    Aggregate(Aggregate),
    OrderByGroupBy(OrderByGroupBy),
    CrossJoin(CrossJoin),
    Project(Project),
    Filter(Filter),
    ScanSource(ScanSource),
    Values(Values),
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for ReadPlan {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        match self {
            ReadPlan::Debug(n) => n.execute_read(source).await,
            ReadPlan::Aggregate(n) => n.execute_read(source).await,
            ReadPlan::OrderByGroupBy(n) => n.execute_read(source).await,
            ReadPlan::CrossJoin(n) => n.execute_read(source).await,
            ReadPlan::Project(n) => n.execute_read(source).await,
            ReadPlan::Filter(n) => n.execute_read(source).await,
            ReadPlan::ScanSource(n) => n.execute_read(source).await,
            ReadPlan::Values(n) => n.execute_read(source).await,
        }
    }
}

/// Utility macro for sending the error from an expression to a channel and
/// immediately returning.
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
pub struct Debug {
    pub input: Box<ReadPlan>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Debug {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let input = self.input.execute_read(source).await?;
        let stream = input.map(move |stream_result| match stream_result {
            Ok(df) => {
                debug!("{:?}", df);
                Ok(df)
            }
            Err(e) => Err(e),
        });
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
pub struct Aggregate {
    pub group_columns: Option<Vec<usize>>,
    pub accumulators: Vec<Accumulator>,
    pub input: Box<ReadPlan>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Aggregate {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let mut input = self.input.execute_read(source).await?;
        // TODO: As with `OrderByGroupBy`, don't collect everything. The
        // resulting accumulated dataframe for each input should be treated as
        // an intermediate value awaiting further aggregation with other
        // intermediate values.
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut df = match input.next().await {
                Some(stream_result) => match_send_err!(stream_result, tx),
                None => return,
            };

            while let Some(stream_result) = input.next().await {
                let next_df = match_send_err!(stream_result, tx);
                df = match_send_err!(df.vstack(next_df), tx);
            }

            let df = match_send_err!(
                match &self.group_columns {
                    Some(cols) => SortedGroupByDataFrame::from_dataframe(df, cols),
                    None => SortedGroupByDataFrame::from_dataframe(df, &[]),
                },
                tx
            );

            let df = match_send_err!(df.accumulate(self.accumulators), tx);
            match tx.send(Ok(df)).await {
                Ok(_) => (),
                Err(_) => error!("failed to send dataframe with aggregates"),
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

/// A combined "order by" and "group by" node. This is done mostly due the
/// implementation of group by on dataframes.
#[derive(Debug)]
pub struct OrderByGroupBy {
    pub columns: Vec<usize>,
    pub input: Box<ReadPlan>,
    // TODO: Asc/desc,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for OrderByGroupBy {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let mut input = self.input.execute_read(source).await?;
        // TODO: Don't collect everything. Implement splitting/merging to get a
        // global sort.
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut df = match input.next().await {
                Some(stream_result) => match_send_err!(stream_result, tx),
                None => return,
            };

            while let Some(stream_result) = input.next().await {
                let next_df = match_send_err!(stream_result, tx);
                df = match_send_err!(df.vstack(next_df), tx);
            }

            let df = match_send_err!(
                SortedGroupByDataFrame::from_dataframe(df, &self.columns),
                tx
            );
            let df = df.into_dataframe();
            match tx.send(Ok(df)).await {
                Ok(_) => (),
                Err(_) => error!("failed to send sorted and grouped dataframe"),
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
pub struct CrossJoin {
    pub left: Box<ReadPlan>,
    pub right: Box<ReadPlan>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for CrossJoin {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let (mut left, mut right) = futures::try_join!(
            self.left.execute_read(source),
            self.right.execute_read(source)
        )?;
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            // TODO: Allow spilling to disk.
            let mut lefts: Vec<DataFrame> = Vec::new();
            let mut rights: Vec<DataFrame> = Vec::new();

            // TODO: Run awaiting for left and right dataframes in parallel..
            loop {
                if let Some(left_df) = left.next().await {
                    let left_df = match_send_err!(left_df, tx);
                    for right_df in rights.iter() {
                        let crossed =
                            match_send_err!(left_df.clone().cross_join(right_df.clone()), tx);
                        match tx.send(Ok(crossed)).await {
                            Ok(_) => (),
                            Err(_) => {
                                error!("failed to send result of cross join");
                                return;
                            }
                        }
                    }
                    lefts.push(left_df);
                    // Continuing here may result in us reading in all lefts
                    // before processing a single right, but that should be fine
                    // for now.
                    continue;
                }

                if let Some(right_df) = right.next().await {
                    let right_df = match_send_err!(right_df, tx);
                    for left_df in lefts.iter() {
                        let crossed =
                            match_send_err!(left_df.clone().cross_join(right_df.clone()), tx);
                        match tx.send(Ok(crossed)).await {
                            Ok(_) => (),
                            Err(_) => {
                                error!("failed to send result of cross join");
                                return;
                            }
                        }
                    }
                    rights.push(right_df);
                    continue;
                }

                // Left and right streams exhausted, we're done.
                return;
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
pub struct Project {
    pub columns: Vec<ScalarExpr>,
    pub input: Box<ReadPlan>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Project {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let input = self.input.execute_read(source).await?;
        let columns = Arc::new(self.columns);
        let stream = input.map(move |stream_result| match stream_result {
            Ok(df) => {
                let eval = columns
                    .clone()
                    .iter()
                    .map(|expr| expr.evaluate(&df))
                    .collect::<Result<Vec<_>>>();
                match eval {
                    Ok(vecs) => DataFrame::from_expr_vecs(vecs),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        });
        Ok(Box::pin(stream))
    }
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

                let df = match_send_err!(df.filter(&mask), tx);
                if let Err(_) = tx.send(Ok(df)).await {
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

#[derive(Debug)]
pub struct Values {
    pub rows: Vec<Row>,
}

#[async_trait]
impl<R: ReadableSource> ReadExecutor<R> for Values {
    async fn execute_read(self, _source: &R) -> Result<DataFrameStream> {
        let df = DataFrame::from_rows(self.rows)?;
        Ok(Box::pin(MemoryStream::one(df)))
    }
}

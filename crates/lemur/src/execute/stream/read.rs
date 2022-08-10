use crate::execute::stream::source::{DataFrameStream, MemoryStream, ReadExecutor, ReadTx};
use crate::repr::df::DataFrame;
use crate::repr::expr::{
    Aggregate, CrossJoin, Filter, OrderByGroupBy, Project, RelationExpr, Source, Values,
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use futures::{StreamExt};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error};

const STREAM_BUFFER: usize = 16;

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for RelationExpr {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        match self {
            RelationExpr::Project(n) => n.execute_read(source).await,
            RelationExpr::Aggregate(n) => n.execute_read(source).await,
            RelationExpr::OrderByGroupBy(n) => n.execute_read(source).await,
            RelationExpr::CrossJoin(n) => n.execute_read(source).await,
            RelationExpr::Filter(n) => n.execute_read(source).await,
            RelationExpr::Values(n) => n.execute_read(source).await,
            RelationExpr::Source(n) => n.execute_read(source).await,
            RelationExpr::Placeholder => {
                Ok(Box::pin(MemoryStream::one(DataFrame::new_placeholder())))
            }
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

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for Project {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let input = self.input.execute_read(source).await?;
        let stream = input.map(move |stream_result| match stream_result {
            Ok(df) => df.project_exprs(&self.columns),
            Err(e) => Err(e),
        });
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for Aggregate {
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

            let df = match_send_err!(df.aggregate(&self.group_by, &self.exprs), tx);
            match tx.send(Ok(df)).await {
                Ok(_) => (),
                Err(_) => error!("failed to send dataframe with aggregates"),
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for OrderByGroupBy {
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

            let df = match_send_err!(df.order_by_group_by(&self.columns), tx);
            match tx.send(Ok(df)).await {
                Ok(_) => (),
                Err(_) => error!("failed to send sorted and grouped dataframe"),
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for CrossJoin {
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

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for Filter {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        let mut input = self.input.execute_read(source).await?;
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            while let Some(df) = input.next().await {
                let df = match_send_err!(df, tx);
                let df = match_send_err!(df.filter_expr(&self.predicate), tx);
                if let Err(_) = tx.send(Ok(df)).await {
                    error!("failed to send filtered dataframe to channel");
                };
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for Source {
    async fn execute_read(self, source: &R) -> Result<DataFrameStream> {
        match source.scan(&self.source, self.filter).await? {
            Some(stream) => Ok(Box::pin(stream)),
            None => Err(anyhow!("missing relation: {}", self.source)),
        }
    }
}

#[async_trait]
impl<R: ReadTx> ReadExecutor<R> for Values {
    async fn execute_read(self, _source: &R) -> Result<DataFrameStream> {
        let df = DataFrame::from_rows(self.rows)?;
        Ok(Box::pin(MemoryStream::one(df)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::stream::source::{DataSource, MemoryDataSource, TxInteractivity};

    #[tokio::test]
    async fn nothing_produces_dummy_row() {
        let source = MemoryDataSource::new();
        let tx = source.begin(TxInteractivity::NonInteractive).await.unwrap();

        let dfs = RelationExpr::Placeholder
            .execute_read(&tx)
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;
        let df = dfs.get(0).unwrap();
        assert_eq!(1, df.num_rows());
    }
}

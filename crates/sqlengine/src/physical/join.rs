use super::{PhysicalOperator, PhysicalPlan};
use crate::engine::Transaction;
use crate::logical::{JoinOperator, JoinType, RelationalPlan};
use crate::physical::spill::{BatchSpill, SpillStream};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::batch::{Batch, BatchRepr, CrossJoinBatch};
use coretypes::expr::ScalarExpr;
use coretypes::stream::{BatchStream, EitherBatch, EitherStream, MemoryStream};
use futures::{ready, stream::unfold, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// A very slow join.
#[derive(Debug)]
pub struct NestedLoopJoin {
    pub join_type: JoinType,
    pub predicate: ScalarExpr,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for NestedLoopJoin {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        let left = self
            .left
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("left operator did not return stream"))?;
        let right = self
            .right
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("right operator did not return stream"))?;

        let stream = EitherStream::from_streams(left, right);

        let left_spill = BatchSpill::new();
        let right_spill = BatchSpill::new();

        let stream = unfold(
            (stream, left_spill, right_spill),
            |(mut stream, mut left_spill, mut right_spill)| async move {
                let input = match stream.next().await {
                    Some(input) => input,
                    None => return None,
                };

                let output: Vec<_> = match input {
                    EitherBatch::Left(Ok(left)) => {
                        build_cross_joins(left, true, &right_spill, &mut left_spill).await
                    }
                    EitherBatch::Right(Ok(right)) => {
                        build_cross_joins(right, false, &left_spill, &mut right_spill).await
                    }
                    EitherBatch::Left(Err(e)) | EitherBatch::Right(Err(e)) => vec![Err(e)],
                };

                Some((output, (stream, left_spill, right_spill)))
            },
        );

        let stream = stream
            .map(|input| futures::stream::iter(input))
            .flatten()
            .boxed();

        Ok(Some(stream))
    }
}

/// Build cross joins between the provided batch and what's already in the read
/// batch spill.
///
/// This will also ensure that the batch is written out to the provided write
/// spill.
///
/// A vector of results is returned to make turning this back into a stream
/// straightfoward.
async fn build_cross_joins(
    batch: BatchRepr,
    is_left: bool,
    read_spill: &BatchSpill,
    write_spill: &mut BatchSpill,
) -> Vec<Result<BatchRepr>> {
    let batch = batch.into_batch();

    match write_spill.send(batch.clone()).await {
        Ok(()) => (),
        Err(e) => return vec![Err(e)],
    }

    // Nothing to cross join with yet.
    if read_spill.is_empty() {
        return Vec::new();
    }

    let mut stream = match read_spill.stream_spilled(0) {
        Ok(stream) => stream,
        Err(e) => return vec![Err(e)],
    };

    let mut cross_joins = Vec::new();
    while let Some(other) = stream.next().await {
        let cross_join = if is_left {
            CrossJoinBatch::from_batches(batch.clone(), other)
        } else {
            CrossJoinBatch::from_batches(other, batch.clone())
        };
        cross_joins.push(cross_join);
    }

    cross_joins
        .into_iter()
        .map(|v| Ok(BatchRepr::from(v)))
        .collect()
}

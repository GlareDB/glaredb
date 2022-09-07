use crate::arrow::chunk::Chunk;
use crate::arrow::column::Column;
use crate::arrow::expr::{Accumulator, AggregateExpr, GroupByExpr, ScalarExpr};
use crate::arrow::scalar::ScalarOwned;
use crate::errors::Result;
use futures::StreamExt;
use tracing::trace;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Aggregate {
    exprs: Vec<Box<dyn AggregateExpr>>,
    group_by: GroupByExpr,
    input: Box<dyn QueryExecutor>,
}

impl QueryExecutor for Aggregate {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        todo!()
    }
}

#[derive(Debug)]
struct HashAggregate {
    input_exprs: Vec<Vec<ScalarExpr>>,
    accumulators: Vec<Box<dyn Accumulator>>,
    group_by: GroupByExpr,
}

impl HashAggregate {
    fn new(agg_exprs: Vec<Box<dyn AggregateExpr>>, group_by: GroupByExpr) -> Self {
        let input_exprs: Vec<_> = agg_exprs.iter().map(|expr| expr.inputs()).collect();
        let accumulators: Vec<_> = agg_exprs.iter().map(|expr| expr.accumulator()).collect();

        HashAggregate {
            input_exprs,
            accumulators,
            group_by,
        }
    }

    /// Execute the hash aggregate on the stream returned from the provided
    /// input.
    ///
    /// Note that this will block the stream since a hash aggregate requires the
    /// entire input.
    async fn execute_inner(mut self, input: Box<dyn QueryExecutor>) -> Result<PinnedChunkStream> {
        let mut stream = input.execute_boxed()?;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            self.aggregate(chunk)?;
        }

        unimplemented!()
    }

    /// Execute aggregations on a chunk, updating the internal states for
    /// accumulators.
    fn aggregate(&mut self, chunk: Chunk) -> Result<()> {
        let grouping_sets_columns = self.eval_group_by(&chunk)?;

        // Compute aggregates per grouping set.
        for grouping_set_columns in grouping_sets_columns.into_iter() {
            unimplemented!()
        }

        Ok(())
    }

    /// Evaluate the expressions in the group by against the provided chunk.
    ///
    /// Each grouping set will produce a vector of columns. A single grouping
    /// set (the typical group by case) will produce a singe vector.
    fn eval_group_by(&self, chunk: &Chunk) -> Result<Vec<Vec<Column>>> {
        trace!("evaling group by");
        // Evaluate against all expressions in the group by.
        //
        // Same number of rows as the input chunk.
        let out = self
            .group_by
            .get_exprs()
            .iter()
            .map(|expr| {
                expr.evaluate(chunk)
                    .and_then(|r| r.into_column_or_expand(chunk.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        // Working with a single grouping set, we're done.
        if self.group_by.num_groups() == 1 {
            return Ok(vec![out]);
        }

        // Produce (typed) null columns.
        let mut nulls = Vec::with_capacity(out.len());
        for col in out.iter() {
            let null_val = ScalarOwned::new_null_with_type(col.get_datatype()?);
            let null_col =
                Column::try_repeat_scalar(null_val.data_type(), null_val, chunk.num_rows())?;
            nulls.push(null_col);
        }

        // Get a vector of columns for each grouping set.
        let grouping_set_cols = self
            .group_by
            .get_null_mask()
            .iter()
            .map(|group_mask| {
                group_mask
                    .iter()
                    .enumerate()
                    .map(|(idx, is_null)| {
                        if *is_null {
                            nulls[idx].clone()
                        } else {
                            out[idx].clone()
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Ok(grouping_set_cols)
    }
}

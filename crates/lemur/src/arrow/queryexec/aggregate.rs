use crate::arrow::chunk::Chunk;
use crate::arrow::column::Column;
use crate::arrow::expr::{Accumulator, AggregateExpr, GroupByExpr, ScalarExpr};
use crate::arrow::scalar::ScalarOwned;
use crate::errors::Result;
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
    input: Box<dyn QueryExecutor>,
}

impl HashAggregate {
    async fn execute_inner(self) -> Result<PinnedChunkStream> {
        unimplemented!()
    }

    fn aggregate(&mut self) -> Result<()> {
        unimplemented!()
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

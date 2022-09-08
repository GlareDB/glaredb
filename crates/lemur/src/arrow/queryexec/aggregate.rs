use crate::arrow::chunk::Chunk;
use crate::arrow::column::{hash, Column};
use crate::arrow::expr::{Accumulator, AggregateExpr, GroupByExpr, ScalarExpr};
use crate::arrow::row::Row;
use crate::arrow::scalar::ScalarOwned;
use crate::errors::Result;
use futures::{stream, StreamExt};
use std::collections::HashMap;
use tracing::trace;

use super::{MemoryStream, PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Aggregate {
    exprs: Vec<Box<dyn AggregateExpr>>,
    group_by: GroupByExpr,
    input: Box<dyn QueryExecutor>,
}

impl QueryExecutor for Aggregate {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        let hash_agg = HashAggregate {
            input_exprs: self.exprs.iter().map(|expr| expr.inputs()).collect(),
            agg_exprs: self.exprs,
            states: AggregateStates::default(),
            group_by: self.group_by,
        };

        let input = self.input;
        Ok(Box::pin(stream::once(async move {
            let chunk = hash_agg.execute_inner(input).await;
            chunk
        })))
    }
}

#[derive(Debug)]
struct AggregateState {
    accs: Vec<Box<dyn Accumulator>>,
    /// Temporary per-chunk state to track indexes for each chunk. Cleared when
    /// moving to a new chunk.
    idxs: Vec<u64>,
}

#[derive(Debug, Default)]
struct AggregateStates {
    /// Group hashed values to a list of accumulators.
    group_accs: HashMap<u64, AggregateState>,
}

#[derive(Debug)]
struct HashAggregate {
    input_exprs: Vec<Vec<ScalarExpr>>,
    agg_exprs: Vec<Box<dyn AggregateExpr>>,
    states: AggregateStates,
    group_by: GroupByExpr,
}

impl HashAggregate {
    /// Execute the hash aggregate on the stream returned from the provided
    /// input.
    ///
    /// Note that this will block the stream since a hash aggregate requires the
    /// entire input.
    async fn execute_inner(mut self, input: Box<dyn QueryExecutor>) -> Result<Chunk> {
        let mut stream = input.execute_boxed()?;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            self.aggregate(chunk)?;
        }

        // Done accumulating, return a chunk with the results.
        let mut rows = Vec::with_capacity(self.states.group_accs.len());
        for (hash, state) in self.states.group_accs.iter() {
            let row: Row = state
                .accs
                .iter()
                .map(|acc| acc.evaluate())
                .collect::<Result<Vec<_>>>()?
                .into();
            trace!(?row, ?hash, "pushing aggregate row for hash");
            rows.push(row);
        }

        Chunk::from_rows(rows)
    }

    /// Execute aggregations on a chunk, updating the internal states for
    /// accumulators.
    fn aggregate(&mut self, chunk: Chunk) -> Result<()> {
        let grouping_sets_columns = self.eval_group_by(&chunk)?;

        let inputs = self.eval_input_exprs(&chunk)?;

        // Compute aggregates per grouping set.
        for grouping_set_columns in grouping_sets_columns.into_iter() {
            let hashes = hash::hash_column(&grouping_set_columns)?;

            // Ensure we have aggregate states for each hash, and track the
            // current chunk rows that belong to each state.
            for (row_idx, hash) in hashes.iter().enumerate() {
                // TODO: Check values actually equal the group values.

                let ent = self.states.group_accs.get_mut(hash);
                match ent {
                    Some(ent) => {
                        ent.idxs.push(row_idx as u64);
                    }
                    None => {
                        // Create a new "aggregate state" for this hash group,
                        // initializing fresh accumulators.
                        let accs: Vec<_> = self
                            .agg_exprs
                            .iter()
                            .map(|agg_expr| agg_expr.accumulator())
                            .collect();

                        let state = AggregateState {
                            accs,
                            idxs: vec![row_idx as u64],
                        };

                        self.states.group_accs.insert(*hash, state);
                    }
                }
            }

            // For each group, update each aggregate with that aggregator's
            // input.
            for (_hash, state) in self.states.group_accs.iter_mut() {
                for (acc, input) in state.accs.iter_mut().zip(inputs.iter()) {
                    acc.accumulate(input)?;
                }

                state.idxs.clear();
            }
        }

        Ok(())
    }

    /// Evaluate the input expressions for the aggregates.
    fn eval_input_exprs(&self, chunk: &Chunk) -> Result<Vec<Vec<Column>>> {
        let inputs = Vec::with_capacity(self.input_exprs.len());
        for exprs in self.input_exprs.iter() {
            for expr in exprs.iter() {
                expr.evaluate(chunk)?
                    .into_column_or_expand(chunk.num_rows())?;
            }
        }
        Ok(inputs)
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

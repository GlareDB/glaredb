use super::{PhysicalOperator, PhysicalPlan};
use crate::catalog::{Catalog, ResolvedTableReference, TableSchema};
use crate::engine::Transaction;
use crate::logical::{JoinOperator, JoinType, RelationalPlan};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::{
    batch::{Batch, BatchError, BatchRepr, SelectivityBatch},
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::{EvaluatedExpr, ExprError, ScalarExpr},
    stream::{BatchStream, EitherBatch, EitherStream, MemorySink, MemoryStream},
    vec::{BoolVec, ColumnVec},
};
use diststore::engine::StorageTransaction;
use futures::stream::FuturesUnordered;
use futures::stream::{Stream, StreamExt};
use std::sync::Arc;

const NLJ_BATCH_SIZE: usize = 16;

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Filter {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        let input = self
            .input
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("operator input did not return a stream"))?;
        let stream = input.map(move |batch| match batch {
            Ok(batch) => {
                let batch = batch.into_batch();
                let evaled = self.predicate.evaluate(&batch)?;
                match evaled {
                    EvaluatedExpr::Column(col) => {
                        let v = col
                            .try_as_bool_vec()
                            .ok_or(anyhow!("column not a bool vec"))?;
                        Ok(BatchRepr::Selectivity(SelectivityBatch::new_with_bool_vec(
                            batch, v,
                        )?))
                    }
                    EvaluatedExpr::ColumnRef(col) => {
                        let v = col
                            .try_as_bool_vec()
                            .ok_or(anyhow!("column not a bool vec"))?;
                        Ok(BatchRepr::Selectivity(SelectivityBatch::new_with_bool_vec(
                            batch, v,
                        )?))
                    }
                    // Expression retruned a single bool, either we return the
                    // batch as is, or we return nothing.
                    //
                    // E.g. "WHERE 1 = 1"
                    //
                    // TODO: What to do with null values?
                    EvaluatedExpr::Value(val, _) => match val {
                        DataValue::Bool(b) => {
                            if b {
                                Ok(batch.into())
                            } else {
                                Ok(BatchRepr::empty())
                            }
                        }
                        _ => return Err(anyhow!("expression did not evaluate to a bool")),
                    },
                }
            }
            Err(e) => Err(e),
        });
        Ok(Some(Box::pin(stream)))
    }
}

#[derive(Debug)]
pub struct Project {
    pub expressions: Vec<ScalarExpr>,
    pub input: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Project {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        let input = self
            .input
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("projection input did not return a stream"))?;
        let stream = input.map(move |batch| match batch {
            Ok(batch) => {
                let batch = batch.into_batch();
                let eval_results = self
                    .expressions
                    .iter()
                    .map(|expr| expr.evaluate(&batch).map_err(|e| e.into())) // TODO: Handle expr error conversion better.
                    .collect::<Result<Vec<_>>>()?;
                let batch = Batch::from_expression_results(eval_results)?;
                Ok(batch.into())
            }
            Err(e) => Err(e),
        });
        Ok(Some(Box::pin(stream)))
    }
}

#[derive(Debug)]
pub struct Scan {
    pub table: ResolvedTableReference,
    pub project: Option<Vec<usize>>,
    pub filter: Option<ScalarExpr>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Scan {
    async fn execute_stream(self, tx: &mut T) -> Result<Option<BatchStream>> {
        // TODO: Pass in projection.
        let stream = tx.scan(&self.table, self.filter).await?;
        Ok(Some(stream))
    }
}

#[derive(Debug)]
pub struct Values {
    pub schema: RelationSchema,
    pub values: Vec<Vec<ScalarExpr>>,
}

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Values {
    async fn execute_stream(self, _tx: &mut T) -> Result<Option<BatchStream>> {
        unimplemented!()
        // let mut batch = Batch::new_from_schema(&self.schema, self.values.len());
        // for row_exprs in self.values.iter() {
        //     let values = row_exprs
        //         .iter()
        //         .map(|expr| expr.evaluate_constant())
        //         .collect::<std::result::Result<Vec<_>, _>>()?;
        //     batch.push_row(values.into())?;
        // }

        // let stream = MemoryStream::from_batch(batch);
        // Ok(Some(Box::pin(stream)))
    }
}

/// Produce an empty batch.
#[derive(Debug)]
pub struct Nothing;

#[async_trait]
impl<T: Transaction + 'static> PhysicalOperator<T> for Nothing {
    async fn execute_stream(self, _tx: &mut T) -> Result<Option<BatchStream>> {
        Ok(Some(Box::pin(MemoryStream::from_batch(Batch::empty()))))
    }
}

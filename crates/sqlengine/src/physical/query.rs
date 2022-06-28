use super::{PhysicalOperator, PhysicalPlan};
use crate::catalog::{Catalog, ResolvedTableReference, TableSchema};
use crate::logical::{JoinOperator, JoinType, RelationalPlan};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use coretypes::{
    batch::{Batch, BatchError, BatchRepr, SelectivityBatch},
    column::{BoolVec, ColumnVec},
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::{EvaluatedExpr, ExprError, ScalarExpr},
    stream::{BatchStream, MemoryStream},
};
use diststore::engine::StorageTransaction;
use futures::stream::{Stream, StreamExt};
use std::sync::Arc;

#[derive(Debug)]
pub struct NestedLoopJoin {
    pub join_type: JoinType,
    pub operator: JoinOperator,
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalOperator<T> for Filter {
    async fn execute_stream(self, tx: &T) -> Result<Option<BatchStream>> {
        let input = self
            .input
            .execute_stream(tx)
            .await?
            .ok_or(anyhow!("operator input did not return a stream"))?;
        let stream = input.map(move |batch| match batch {
            Ok(batch) => {
                let evaled = self.predicate.evaluate(&batch)?;
                // TODO: This removes any previous selectivity.
                let batch = batch.get_batch().clone();
                match evaled {
                    EvaluatedExpr::Value(_) => {
                        Err(anyhow!("got value from expr: {}", self.predicate))
                    }
                    EvaluatedExpr::Column(col) => {
                        let v = col
                            .get_values()
                            .try_as_bool_vec()
                            .ok_or(anyhow!("column not a bool vec"))?;
                        Ok(BatchRepr::Selectivity(SelectivityBatch::new_with_bool_vec(
                            batch, v,
                        )?))
                    }
                    EvaluatedExpr::ColumnRef(col) => {
                        let v = col
                            .get_values()
                            .try_as_bool_vec()
                            .ok_or(anyhow!("column not a bool vec"))?;
                        Ok(BatchRepr::Selectivity(SelectivityBatch::new_with_bool_vec(
                            batch, v,
                        )?))
                    }
                }
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
impl<T: StorageTransaction + 'static> PhysicalOperator<T> for Scan {
    async fn execute_stream(self, tx: &T) -> Result<Option<BatchStream>> {
        let name = self.table.to_string();
        // TODO: Pass in projection.
        let stream = tx.scan(&name, self.filter, 10).await?;
        Ok(Some(stream))
    }
}

#[derive(Debug)]
pub struct Values {
    pub schema: RelationSchema,
    pub values: Vec<Vec<ScalarExpr>>,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalOperator<T> for Values {
    async fn execute_stream(self, _tx: &T) -> Result<Option<BatchStream>> {
        let mut batch = Batch::new_from_schema(&self.schema, self.values.len());
        for row_exprs in self.values.iter() {
            let values = row_exprs
                .iter()
                .map(|expr| expr.evaluate_constant())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            batch.push_row(values.into())?;
        }

        let stream = MemoryStream::with_single_batch(batch.into());
        Ok(Some(Box::pin(stream)))
    }
}

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

#[async_trait]
pub trait PhysicalNode<T: StorageTransaction> {
    /// Produce a streaming pipeline.
    async fn build_stream(self, tx: Arc<T>) -> Result<BatchStream>;
}

#[derive(Debug)]
pub enum PhysicalPlan {
    Scan(Scan),
    Values(Values),
    Filter(Filter),
    NestedLoopJoin(NestedLoopJoin),
}

#[derive(Debug)]
pub struct NestedLoopJoin {
    pub join_type: JoinType,
    pub operator: JoinOperator,
}

impl NestedLoopJoin {
    pub async fn stream(self, left: BatchStream, right: BatchStream) -> Result<BatchStream> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
    pub input: Box<PhysicalPlan>,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalNode<T> for Filter {
    async fn build_stream(self, tx: Arc<T>) -> Result<BatchStream> {
        let input = self.input.build_stream(tx).await?;
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
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
pub struct Scan {
    pub table: ResolvedTableReference,
    pub project: Option<Vec<usize>>,
    pub filter: Option<ScalarExpr>,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalNode<T> for Scan {
    async fn build_stream(self, tx: Arc<T>) -> Result<BatchStream> {
        let name = self.table.to_string();
        // TODO: Pass in projection.
        let stream = tx.scan(&name, self.filter, 10).await?;
        Ok(stream)
    }
}

#[derive(Debug)]
pub struct Values {
    pub schema: RelationSchema,
    pub values: Vec<Vec<ScalarExpr>>,
}

#[async_trait]
impl<T: StorageTransaction + 'static> PhysicalNode<T> for Values {
    async fn build_stream(self, _tx: Arc<T>) -> Result<BatchStream> {
        let mut batch = Batch::new_from_schema(&self.schema, self.values.len());
        for row_exprs in self.values.iter() {
            let values = row_exprs
                .iter()
                .map(|expr| expr.evaluate_constant())
                .collect::<std::result::Result<Vec<_>, _>>()?;
            batch.push_row(values.into())?;
        }

        let stream = MemoryStream::with_single_batch(batch.into());
        Ok(Box::pin(stream))
    }
}

#[derive(Debug)]
pub struct CreateTable {
    pub table: TableSchema,
}

impl PhysicalPlan {
    pub fn from_logical(logical: RelationalPlan) -> Result<PhysicalPlan> {
        Ok(match logical {
            RelationalPlan::Filter(filter) => PhysicalPlan::Filter(Filter {
                predicate: filter.predicate,
                input: Box::new(Self::from_logical(*filter.input)?),
            }),
            _ => return Err(anyhow!("unsupported logical node")),
        })
    }

    pub async fn build_stream<T>(self, tx: Arc<T>) -> Result<BatchStream>
    where
        T: StorageTransaction + 'static,
    {
        Ok(match self {
            PhysicalPlan::Scan(scan) => scan.build_stream(tx).await?,
            PhysicalPlan::Filter(filter) => filter.build_stream(tx).await?,
            PhysicalPlan::Values(values) => values.build_stream(tx).await?,
            _ => return Err(anyhow!("unimplemented physical node")),
        })
    }
}

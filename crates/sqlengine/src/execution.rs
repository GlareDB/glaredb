use crate::catalog::ResolvedTableReference;
use coretypes::{
    batch::{Batch, BatchError, SelectivityBatch},
    column::{BoolVec, ColumnVec},
    datatype::{DataType, DataValue, NullableType, RelationSchema},
    expr::{EvaluatedExpr, ExprError, ScalarExpr},
};
use diststore::client::Client;
use diststore::stream::{BatchStream, MemoryStream, StreamError};
use diststore::StoreError;
use futures::stream::{Stream, StreamExt};

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("internal: {0}")]
    Internal(String),
    #[error(transparent)]
    StoreError(#[from] StoreError),
    #[error(transparent)]
    BatchError(#[from] BatchError),
    #[error(transparent)]
    ExprError(#[from] ExprError),
}

type Result<T, E = ExecutionError> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum PhysicalPlan {
    Scan(Scan),
    Values(Values),
    Filter(Filter),
}

#[derive(Debug)]
pub struct Filter {
    pub predicate: ScalarExpr,
}

impl Filter {
    pub async fn stream(self, input: BatchStream) -> Result<BatchStream> {
        // let stream = input.map(|batch| match batch {
        //     Ok(batch) => {
        //         let evaled = match self.predicate.evaluate(&batch) {
        //             Ok(evaled) => evaled,
        //             _ => unimplemented!(),
        //         };
        //         let col = match evaled.try_into_column() {
        //             Ok(col) => col,
        //             Err(evaled) => unimplemented!(),
        //         };
        //         let (vals, _) = col.into_parts();
        //         match vals {
        //             // TODO: This ignores any previous selectivity.
        //             ColumnVec::Bool(col) => Ok(SelectivityBatch::new_with_bool_vec(
        //                 batch.get_ref().clone(),
        //                 col,
        //             )
        //             .unwrap()), // TODO
        //             col => unimplemented!(),
        //         }
        //     }
        //     Err(e) => Err(e),
        // });
        // Ok(Box::pin(stream))
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Scan {
    pub table: ResolvedTableReference,
    pub projected_schema: RelationSchema,
    pub project: Option<Vec<usize>>,
    pub filter: Option<ScalarExpr>,
}

impl Scan {
    pub async fn stream<C>(self, client: &C) -> Result<BatchStream>
    where
        C: Client,
    {
        let tbl = self.table.to_string();
        let stream = client.scan(&tbl, self.filter, 10).await?;
        Ok(stream)
    }
}

#[derive(Debug)]
pub struct Values {
    pub schema: RelationSchema,
    pub values: Vec<Vec<ScalarExpr>>,
}

impl Values {
    pub fn stream(self) -> Result<BatchStream> {
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

impl PhysicalPlan {}

#[cfg(test)]
mod tests {
    use super::*;
    use coretypes::datatype::{DataType, DataValue};
    use futures::stream::StreamExt;

    #[tokio::test]
    async fn values_simple() {
        let exprs = vec![
            vec![
                ScalarExpr::Constant(DataValue::Int16(4), DataType::Int16.into()),
                ScalarExpr::Constant(DataValue::Utf8("hello".to_string()), DataType::Utf8.into()),
            ],
            vec![
                ScalarExpr::Constant(DataValue::Int16(5), DataType::Int16.into()),
                ScalarExpr::Constant(DataValue::Utf8("world".to_string()), DataType::Utf8.into()),
            ],
        ];
        let schema = RelationSchema::new(vec![DataType::Int16.into(), DataType::Utf8.into()]);
        let values = Values {
            schema,
            values: exprs,
        };

        let mut stream = values.stream().unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(2, batch.get_batch().num_rows());
        assert_eq!(2, batch.get_batch().arity());

        let batch = stream.next().await;
        assert!(batch.is_none());
    }

    #[test]
    fn values_schema_mismatch() {
        let exprs = vec![
            vec![
                ScalarExpr::Constant(DataValue::Int16(4), DataType::Int16.into()),
                ScalarExpr::Constant(DataValue::Utf8("hello".to_string()), DataType::Utf8.into()),
            ],
            vec![
                ScalarExpr::Constant(DataValue::Int16(5), DataType::Int16.into()),
                ScalarExpr::Constant(DataValue::Int16(6), DataType::Int16.into()),
            ],
        ];

        let schema = RelationSchema::new(vec![DataType::Int16.into(), DataType::Utf8.into()]);
        let values = Values {
            schema,
            values: exprs,
        };

        let result = values.stream();
        assert!(result.is_err());
    }
}

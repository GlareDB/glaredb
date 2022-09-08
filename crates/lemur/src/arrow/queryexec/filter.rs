use crate::arrow::expr::ScalarExpr;
use crate::errors::Result;
use crate::{arrow::chunk::Chunk, errors::internal};
use futures::stream::StreamExt;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Filter {
    predicate: ScalarExpr,
    input: Box<dyn QueryExecutor>,
}

impl Filter {
    pub fn new(predicate: ScalarExpr, input: impl Into<Box<dyn QueryExecutor>>) -> Self {
        Self {
            predicate,
            input: input.into(),
        }
    }

    fn filter_chunk(predicate: &ScalarExpr, input: Result<Chunk>) -> Result<Chunk> {
        match input {
            Ok(chunk) => {
                let size = chunk.num_rows();
                if let Some(mask) = predicate
                    .evaluate(&chunk)?
                    .into_column_or_expand(size)?
                    .try_downcast_bool()
                {
                    chunk.filter(mask)
                } else {
                    Err(internal!(
                        "predicate expression failed to down cast to BoolColumn mask"
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl QueryExecutor for Filter {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        let stream = self.input.execute_boxed()?;
        let predicate = self.predicate;
        let stream = stream.map(move |result| Self::filter_chunk(&predicate, result));
        Ok(Box::pin(stream))
    }
}

impl From<Filter> for Box<dyn QueryExecutor> {
    fn from(v: Filter) -> Self {
        Box::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::expr::BinaryOperation;
    use crate::arrow::expr::ScalarExpr;
    use crate::arrow::expr::UnaryOperation;
    use crate::arrow::queryexec::RowValues;
    use crate::arrow::row::Row;
    use crate::arrow::scalar::ScalarOwned;
    use crate::arrow::testutil;

    #[tokio::test]
    async fn scalar_constant_true() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ]);

        let predicate = ScalarExpr::Constant(ScalarOwned::Bool(Some(true)));

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> = vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn scalar_constant_false() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ]);

        let predicate = ScalarExpr::Constant(ScalarOwned::Bool(Some(false)));

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> = vec![];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn scalar_binary_eq_constant() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ]);

        let predicate = ScalarExpr::Binary {
            op: BinaryOperation::Eq,
            left: Box::new(ScalarExpr::Column(0)),
            right: Box::new(ScalarExpr::Constant(ScalarOwned::Int8(Some(2)))),
        };

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> =
            vec![vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into()];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn scalar_binary_neq_constant() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ]);

        // let predicate = ScalarExpr::Column(1);
        let predicate = ScalarExpr::Binary {
            op: BinaryOperation::Neq,
            left: Box::new(ScalarExpr::Column(0)),
            right: Box::new(ScalarExpr::Constant(ScalarOwned::Int8(Some(2)))),
        };

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> = vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(0))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(None)].into(),
        ];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn scalar_binary_gt_column() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(4)), ScalarOwned::Int8(Some(5))].into(),
        ]);

        let predicate = ScalarExpr::Binary {
            op: BinaryOperation::Gt,
            left: Box::new(ScalarExpr::Column(0)),
            right: Box::new(ScalarExpr::Column(1)),
        };

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> = vec![
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(Some(1))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(Some(1))].into(),
        ];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn scalar_unary_is_null_column() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(None)].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(None)].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(Some(3))].into(),
            vec![ScalarOwned::Int8(Some(4)), ScalarOwned::Int8(Some(4))].into(),
        ]);

        let predicate = ScalarExpr::Unary {
            op: UnaryOperation::IsNull,
            input: Box::new(ScalarExpr::Column(1)),
        };

        let filter = Filter::new(predicate, input);

        let out = testutil::collect_result(filter).await.unwrap();
        let expected: Vec<Row> = vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(None)].into(),
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Int8(None)].into(),
        ];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }
}

use crate::arrow::chunk::Chunk;
use crate::arrow::expr::ScalarExpr;
use crate::errors::Result;
use futures::stream::StreamExt;

use super::{PinnedChunkStream, QueryExecutor};

#[derive(Debug)]
pub struct Project {
    columns: Vec<ScalarExpr>,
    input: Box<dyn QueryExecutor>,
}

impl Project {
    pub fn new(columns: Vec<ScalarExpr>, input: impl Into<Box<dyn QueryExecutor>>) -> Project {
        Project {
            columns,
            input: input.into(),
        }
    }

    fn execute_inner(exprs: &[ScalarExpr], input: Result<Chunk>) -> Result<Chunk> {
        match input {
            Ok(chunk) => {
                let size = chunk.num_rows();
                let cols = exprs
                    .iter()
                    .flat_map(|expr| expr.evaluate(&chunk).map(|r| r.try_into_column(size)))
                    .collect::<Result<Vec<_>>>()?;
                cols.try_into()
            }
            Err(e) => Err(e),
        }
    }
}

impl QueryExecutor for Project {
    fn execute_boxed(self: Box<Self>) -> Result<PinnedChunkStream> {
        let stream = self.input.execute_boxed()?;
        let exprs = self.columns;
        let stream = stream.map(move |result| Self::execute_inner(&exprs, result));
        Ok(Box::pin(stream))
    }
}

impl From<Project> for Box<dyn QueryExecutor> {
    fn from(v: Project) -> Self {
        Box::new(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::expr::ScalarExpr;
    use crate::arrow::queryexec::RowValues;
    use crate::arrow::row::Row;
    use crate::arrow::scalar::ScalarOwned;
    use crate::arrow::testutil;

    #[tokio::test]
    async fn simple() {
        logutil::init_test();

        let input = RowValues::new(vec![
            vec![ScalarOwned::Int8(Some(1)), ScalarOwned::Int8(Some(2))].into(),
            vec![ScalarOwned::Int8(Some(3)), ScalarOwned::Int8(Some(4))].into(),
        ]);

        let columns = vec![
            ScalarExpr::Column(1),
            ScalarExpr::Constant(ScalarOwned::Bool(Some(true))),
        ];

        let proj = Project::new(columns, input);

        let out = testutil::collect_result(proj).await.unwrap();
        let expected: Vec<Row> = vec![
            vec![ScalarOwned::Int8(Some(2)), ScalarOwned::Bool(Some(true))].into(),
            vec![ScalarOwned::Int8(Some(4)), ScalarOwned::Bool(Some(true))].into(),
        ];
        let got: Vec<Row> = out.row_iter().collect();

        assert_eq!(expected, got);
    }
}

use futures::TryStreamExt;
use rayexec_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::field::ColumnSchema;
use crate::execution::operators::results::streaming::ResultStream;
use crate::runtime::handle::QueryHandle;

#[derive(Debug)]
pub struct QueryResult {
    pub handle: Box<dyn QueryHandle>,
    pub output_schema: ColumnSchema,
    pub output: Output,
}

#[derive(Debug)]
pub enum Output {
    Stream(ResultStream),
}

impl Output {
    pub async fn collect(&mut self) -> Result<Vec<Batch>> {
        match self {
            Self::Stream(stream) => stream.try_collect::<Vec<_>>().await,
        }
    }
}

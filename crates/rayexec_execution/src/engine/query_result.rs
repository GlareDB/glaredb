use futures::TryStreamExt;
use rayexec_error::Result;

use super::result::ResultStream;
use crate::arrays::batch::Batch;
use crate::arrays::field::Schema;

#[derive(Debug)]
pub struct QueryResult {
    pub output_schema: Schema,
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

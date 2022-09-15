use crate::arrow::chunk::Chunk;
use crate::arrow::queryexec::QueryExecutor;
use crate::errors::Result;
use futures::StreamExt;

/// Collect the entire output of a query execution into a single chunk, or
/// return the first error encountered.
pub async fn collect_result(exec: impl Into<Box<dyn QueryExecutor>>) -> Result<Chunk> {
    let stream = exec.into().execute_boxed()?;
    let chunks = stream.collect::<Vec<Result<Chunk>>>().await;
    let chunks = chunks.into_iter().collect::<Result<Vec<_>>>()?;

    let mut iter = chunks.into_iter();
    let first = match iter.next() {
        Some(first) => first,
        None => return Ok(Chunk::empty()),
    };

    let mut reduced = first;
    for chunk in iter {
        reduced = reduced.vstack(&chunk)?;
    }

    Ok(reduced)
}

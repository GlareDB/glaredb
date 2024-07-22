use std::sync::Arc;
use std::task::Context;
use std::{fmt, task::Poll};

use futures::{future::BoxFuture, FutureExt};
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_error::Result;
use rayexec_execution::runtime::ExecutionRuntime;
use rayexec_execution::{
    execution::operators::{PollFinalize, PollPush},
    functions::copy::{CopyToFunction, CopyToSink},
};
use rayexec_io::{location::FileLocation, FileSink};

use crate::reader::DialectOptions;
use crate::writer::CsvEncoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvCopyToFunction;

impl CopyToFunction for CsvCopyToFunction {
    fn name(&self) -> &'static str {
        "csv_copy_to"
    }

    fn create_sinks(
        &self,
        runtime: &Arc<dyn ExecutionRuntime>,
        schema: Schema,
        location: FileLocation,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn CopyToSink>>> {
        let provider = runtime.file_provider();

        let mut sinks = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let sink = provider.file_sink(location.clone())?;
            let dialect = DialectOptions::default();

            sinks.push(Box::new(CsvCopyToSink {
                future: None,
                did_finalize: false,
                encoder: CsvEncoder::new(schema.clone(), dialect),
                sink,
            }) as _)
        }

        Ok(sinks)
    }
}

// TODO: Refactor to allow this to essentially just be an async function. The
// polling should be happening in the operator.
pub struct CsvCopyToSink {
    future: Option<BoxFuture<'static, Result<()>>>,
    did_finalize: bool,
    encoder: CsvEncoder,
    sink: Box<dyn FileSink>,
}

impl CopyToSink for CsvCopyToSink {
    fn poll_push(&mut self, cx: &mut Context, batch: Batch) -> Result<PollPush> {
        if let Some(mut future) = self.future.take() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {
                    self.future = Some(future);
                    return Ok(PollPush::Pending(batch));
                }
            }
        }

        let mut buf = Vec::with_capacity(1024);
        self.encoder.encode(&batch, &mut buf)?;

        let mut fut = self.sink.write_all(buf.into()).boxed();
        match fut.poll_unpin(cx) {
            Poll::Ready(Ok(_)) => Ok(PollPush::Pushed),
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => {
                self.future = Some(fut);
                // A bit confusing... but we've "accepted" the batch. This will
                // likely be refactored a bit.
                Ok(PollPush::Pushed)
            }
        }
    }

    fn poll_finalize(&mut self, cx: &mut Context) -> Result<PollFinalize> {
        if let Some(mut future) = self.future.take() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(_)) => (),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => {
                    self.future = Some(future);
                    return Ok(PollFinalize::Pending);
                }
            }
        }

        if !self.did_finalize {
            let fut = self.sink.finish().boxed();
            self.did_finalize = true;
            self.future = Some(fut);
            return self.poll_finalize(cx);
        }

        Ok(PollFinalize::Finalized)
    }
}

impl fmt::Debug for CsvCopyToSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvCopyToSink")
            .field("encoder", &self.encoder)
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}

use crate::errors::Result;
use crate::remote::local_side::ClientSendExecsRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::JoinSet;

use super::client_send::ClientExchangeSendExec;

/// Drives execution of the output stream from the server in conjunction with
/// the send streams to the server.
///
/// This exec should produce the final result of a query.
#[derive(Debug)]
pub struct SendRecvJoinExec {
    /// The execution plan producing the output stream.
    input: Arc<dyn ExecutionPlan>,

    /// The execution plans for sending batches to the remote server.
    ///
    /// Note that these only get handled on the the call to the first partition
    /// execute.
    send_execs: Arc<Mutex<Vec<ClientExchangeSendExec>>>,
}

impl SendRecvJoinExec {
    /// Create a new execution plan that drives both the stream from the input
    /// execution plan, and the send execs for sending record batches to the
    /// remote node.
    ///
    /// This execution plan should only be create *after* the client send execs
    /// have been populated by calling `LocalSideTableProvider::scan` (which
    /// should have already been done by creating the provided input execution
    /// plan).
    pub fn new(input: Arc<dyn ExecutionPlan>, refs: Vec<ClientSendExecsRef>) -> SendRecvJoinExec {
        let send_execs: Vec<_> = refs.into_iter().flat_map(|r| r.take_execs()).collect();

        SendRecvJoinExec {
            input,
            send_execs: Arc::new(Mutex::new(send_execs)),
        }
    }
}

impl ExecutionPlan for SendRecvJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SendRecvJoinExec {
            input: children[0].clone(),
            send_execs: self.send_execs.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Set up send exec tokio tasks.
        //
        // Will be empty if this isn't the first call to execute, which is fine
        // because we only want these handled once.
        let send_execs: Vec<ClientExchangeSendExec> =
            std::mem::take(self.send_execs.lock().as_mut());

        let mut join_set: JoinSet<Result<(), DataFusionError>> = JoinSet::new();
        for send_exec in send_execs {
            let context = context.clone();
            join_set.spawn(async move {
                let mut stream = send_exec.execute(0, context)?;
                while let Some(result) = stream.next().await {
                    // The ouput of this stream is a record batch containing the
                    // number of rows sent, so just ignore.
                    let _ = result?;
                }
                Ok(())
            });
        }

        // Now get the output stream we want to read the results from.
        let stream = self.input.execute(partition, context)?;

        let stream = SendRecvJoinStream {
            join_set,
            stream,
            task_did_err: false,
            tasks_all_completed: false,
        };

        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for SendRecvJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SendRecvExec")
    }
}

/// The stream which will drive all tasks in the join set, as well as the output
/// record batch stream.
///
/// This will attempt to read from the output stream before trying to make
/// progress on driving the join set.
struct SendRecvJoinStream {
    /// The tasks for sending local record batches to the remote node.
    join_set: JoinSet<Result<(), DataFusionError>>,
    /// The stream producing output.
    stream: SendableRecordBatchStream,
    /// Set to true if any of the tasks errored.
    task_did_err: bool,
    /// Set to true if all the tasks completed without error.
    tasks_all_completed: bool,
}

impl Stream for SendRecvJoinStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        if this.task_did_err {
            return Poll::Ready(None);
        }

        loop {
            println!("receiving");
            // Try poll stream first.
            match this.stream.poll_next_unpin(cx) {
                // Stream has data (batch or error)
                Poll::Ready(out) => return Poll::Ready(out),

                // Tasks all completed, we're just waiting for the stream to
                // finish.
                Poll::Pending if this.tasks_all_completed => return Poll::Pending,

                // Only fall down to join set if we know the tasks haven't been
                // completed yet.
                Poll::Pending => {
                    // Stream waiting, now try polling tokio tasks.
                    match this.join_set.poll_join_next(cx) {
                        // A task completed with no error. Continue to try to do
                        // more work.
                        Poll::Ready(Some(Ok(Ok(_)))) => continue,

                        // A task completed with an error. This should
                        // effectively stop the entire stream and completion of
                        // other tasks.
                        Poll::Ready(Some(Ok(Err(e)))) => {
                            this.task_did_err = true;
                            return Poll::Ready(Some(Err(e)));
                        }

                        // Task was aborted.
                        Poll::Ready(Some(Err(e))) => {
                            this.task_did_err = true;
                            return Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                                "task aborted: {e}"
                            )))));
                        }

                        // All tasks completed. Stream might still have more to
                        // do, so continue.
                        Poll::Ready(None) => {
                            this.tasks_all_completed = true;
                            continue;
                        }

                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for SendRecvJoinStream {
    fn schema(&self) -> Arc<Schema> {
        self.stream.schema()
    }
}

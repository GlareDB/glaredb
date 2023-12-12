use crate::errors::{Result, RpcsrvError};
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::variable::VarType;
use datafusion_ext::vars::SessionVars;
use futures::{Stream, StreamExt};
use protogen::{
    gen::rpcsrv::simple,
    rpcsrv::types::simple::{
        ExecuteQueryRequest, ExecuteQueryResponse, QueryResultError, QueryResultSuccess,
    },
};
use sqlexec::{engine::Engine, OperationInfo};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status};

/// The "simple query" rpc handler.
///
/// Note that this doesn't keep state about sessions, and sessions only last the
/// lifetime of a query.
pub struct SimpleHandler {
    /// Core db engine for creating sessions.
    engine: Arc<Engine>,
}

impl SimpleHandler {
    pub fn new(engine: Arc<Engine>) -> SimpleHandler {
        SimpleHandler { engine }
    }
}

#[async_trait]
impl simple::simple_service_server::SimpleService for SimpleHandler {
    type ExecuteQueryStream = SimpleExecuteQueryStream;

    async fn execute_query(
        &self,
        request: Request<simple::ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        // Note that this creates a local session independent of any "remote"
        // sessions. This provides full session capabilities (e.g. parsing sql,
        // use the dist exec scheduler).
        //
        // This may be something we change (into what?)
        let request = ExecuteQueryRequest::try_from(request.into_inner())?;
        let vars = SessionVars::default().with_database_id(request.database_id, VarType::System);
        let mut session = self
            .engine
            .new_local_session_context(vars, request.config.into())
            .await
            .map_err(RpcsrvError::from)?;

        let plan = session
            .sql_to_lp(&request.query_text)
            .await
            .map_err(RpcsrvError::from)?;
        let plan = plan.try_into_datafusion_plan().map_err(RpcsrvError::from)?;
        let physical = session
            .create_physical_plan(plan, &OperationInfo::default())
            .await
            .map_err(RpcsrvError::from)?;
        let stream = session
            .execute_physical(physical)
            .await
            .map_err(RpcsrvError::from)?;

        Ok(Response::new(SimpleExecuteQueryStream::new(stream)))
    }
}

/// Stream implementation for sending the results of a simple query request.
// TODO: Only supports a single response stream (we can do many).
// TODO: Only provides "success" or "error" info to the client. Doesn't return
// the actual results.
pub struct SimpleExecuteQueryStream {
    inner: SendableRecordBatchStream,
    done: bool,
}

impl SimpleExecuteQueryStream {
    pub fn new(stream: SendableRecordBatchStream) -> SimpleExecuteQueryStream {
        SimpleExecuteQueryStream {
            inner: stream,
            done: false,
        }
    }
}

impl Stream for SimpleExecuteQueryStream {
    type Item = Result<simple::ExecuteQueryResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            match self.inner.poll_next_unpin(cx) {
                // Drop the result, we're not sending it back to the client.
                // And continue to the next loop iteration.
                Poll::Ready(Some(Ok(_))) => (),

                // Stream completed without error, return success to the client.
                Poll::Ready(None) => {
                    self.done = true; // Make sure we properly signal stream end on next poll.
                    return Poll::Ready(Some(Ok(ExecuteQueryResponse::SuccessResult(
                        QueryResultSuccess {},
                    )
                    .into())));
                }

                // We got an error, send it back to the client.
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Ok(ExecuteQueryResponse::ErrorResult(
                        QueryResultError { msg: e.to_string() },
                    )
                    .into())))
                }

                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{datatypes::Schema, record_batch::RecordBatch},
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use futures::stream::{self, StreamExt};

    #[tokio::test]
    async fn simple_stream_exits() {
        // https://github.com/GlareDB/glaredb/issues/2242

        let inner = Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(Schema::empty()),
            stream::iter([
                Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
                Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))),
            ]),
        ));

        let stream = SimpleExecuteQueryStream::new(inner);
        let output = stream
            .map(|result| result.unwrap())
            .collect::<Vec<_>>()
            .await;

        let expected: &[simple::ExecuteQueryResponse] =
            &[ExecuteQueryResponse::SuccessResult(QueryResultSuccess {}).into()];

        assert_eq!(output, expected)
    }
}

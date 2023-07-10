use crate::engine::Engine;
use crate::remoteexec::errors::{RemoteExecError, Result};
use crate::remoteexec::proto::plan::plan_service_client::PlanServiceClient;
use crate::remoteexec::proto::plan::plan_service_server::PlanService;
use crate::remoteexec::proto::plan::{
    ExecutePlanFail, ExecutePlanRequest, ExecutePlanResponse, ExecutePlanSuccess,
};
use crate::session::Session;
use async_trait::async_trait;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

/// Wrapper around an engine for executing query plans.
pub struct ExecEngine {
    engine: Arc<Engine>,
}

#[async_trait]
impl PlanService for ExecEngine {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<ExecutePlanResponse, Status>> + Send>>;

    async fn execute_plan(
        &self,
        request: Request<ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let session: Session = {
            // TODO: Create session.
            unimplemented!()
        };

        let mut stream: SendableRecordBatchStream = {
            // TODO: Deserialize plan and execute.
            unimplemented!()
        };

        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut buf = Vec::new();
            while let Some(item) = stream.next().await {
                let msg = match item {
                    Ok(batch) => {
                        // TODO: Unwraps
                        let mut writer = FileWriter::try_new(buf, &batch.schema()).unwrap();
                        writer.write(&batch).unwrap();
                        writer.finish().unwrap();
                        buf = writer.into_inner().unwrap();
                        ExecutePlanResponse {
                            response: Some(
                                super::proto::plan::execute_plan_response::Response::Success(
                                    ExecutePlanSuccess {
                                        arrow_ipc: buf.clone(),
                                    },
                                ),
                            ),
                        }
                    }
                    Err(e) => ExecutePlanResponse {
                        response: Some(super::proto::plan::execute_plan_response::Response::Fail(
                            ExecutePlanFail { msg: e.to_string() },
                        )),
                    },
                };

                if tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ExecutePlanStream
        ))
    }
}

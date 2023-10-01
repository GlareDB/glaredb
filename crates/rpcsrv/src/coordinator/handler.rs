//! GRPC impl.

use super::{CoordinatorError, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use protogen::gen::rpcsrv::{common, coordinator};
use sqlexec::{
    engine::{Engine, SessionStorageConfig},
    remote::batch_stream::ExecutionBatchStream,
};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Request, Response, Status, Streaming};

pub struct CoordinatorHandler {}

impl CoordinatorHandler {}

#[async_trait]
impl coordinator::coordinator_service_server::CoordinatorService for CoordinatorHandler {
    async fn register_worker(
        &self,
        request: Request<coordinator::RegisterWorkerRequest>,
    ) -> Result<Response<coordinator::RegisterWorkerResponse>, Status> {
        unimplemented!()
    }

    async fn deregister_worker(
        &self,
        request: Request<coordinator::DeregisterWorkerRequest>,
    ) -> Result<Response<coordinator::DeregisterWorkerResponse>, Status> {
        unimplemented!()
    }

    async fn heartbeat(
        &self,
        request: Request<coordinator::HeartbeatRequest>,
    ) -> Result<Response<coordinator::HeartbeatResponse>, Status> {
        unimplemented!()
    }

    async fn poll_work(
        &self,
        request: Request<coordinator::PollWorkRequest>,
    ) -> Result<Response<coordinator::PollWorkResponse>, Status> {
        unimplemented!()
    }

    async fn install_batch_stream(
        &self,
        request: Request<Streaming<common::ExecutionBatchStream>>,
    ) -> Result<Response<coordinator::InstallBatchStreamResponse>, Status> {
        unimplemented!()
    }
}

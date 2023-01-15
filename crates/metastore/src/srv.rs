use crate::proto::service::metastore_service_server::MetastoreService;
use crate::proto::service::*;
use async_trait::async_trait;
use tonic::{Request, Response, Status};

/// Metastore GRPC service.
pub struct Service {}

impl Service {}

#[async_trait]
impl MetastoreService for Service {
    async fn fetch_catalog(
        &self,
        request: Request<FetchCatalogRequest>,
    ) -> Result<Response<FetchCatalogResponse>, Status> {
        unimplemented!()
    }

    async fn mutate_catalog(
        &self,
        request: Request<MutateRequest>,
    ) -> Result<Response<MutateResponse>, Status> {
        unimplemented!()
    }
}

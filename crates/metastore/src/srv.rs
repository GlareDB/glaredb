use crate::database::DatabaseCatalog;
use crate::errors::MetastoreError;
use crate::proto::service::metastore_service_server::MetastoreService;
use crate::proto::service::{
    self, FetchCatalogRequest, FetchCatalogResponse, MutateRequest, MutateResponse,
};
use crate::types::service::Mutation;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Metastore GRPC service.
pub struct Service {
    catalogs: RwLock<HashMap<Uuid, DatabaseCatalog>>, // Fancy!
}

impl Service {}

#[async_trait]
impl MetastoreService for Service {
    async fn fetch_catalog(
        &self,
        request: Request<FetchCatalogRequest>,
    ) -> Result<Response<FetchCatalogResponse>, Status> {
        let req = request.into_inner();
        let id = Uuid::from_slice(&req.db_id)
            .map_err(|_| MetastoreError::InvalidDatabaseId(req.db_id))?;
        let catalogs = self.catalogs.read().await;

        // Catalog exists.
        if let Some(catalog) = catalogs.get(&id) {
            let state = catalog.get_state().await?;
            return Ok(Response::new(FetchCatalogResponse {
                catalog: Some(state.into()),
            }));
        }

        // Catalog doesn't exist, upgrade and initialize.
        std::mem::drop(catalogs);

        // Init, get state before inserting.
        let catalog = DatabaseCatalog::open(id).await?;
        let state = catalog.get_state().await?;
        let mut catalogs = self.catalogs.write().await;

        // We raced, catalog was created before we got the lock.
        if let Some(catalog) = catalogs.get(&id) {
            let state = catalog.get_state().await?;
            return Ok(Response::new(FetchCatalogResponse {
                catalog: Some(state.into()),
            }));
        }

        // Insert new catalog.
        catalogs.insert(id, catalog);

        Ok(Response::new(FetchCatalogResponse {
            catalog: Some(state.into()),
        }))
    }

    async fn mutate_catalog(
        &self,
        request: Request<MutateRequest>,
    ) -> Result<Response<MutateResponse>, Status> {
        let req = request.into_inner();
        let id = Uuid::from_slice(&req.db_id)
            .map_err(|_| MetastoreError::InvalidDatabaseId(req.db_id))?;

        let catalogs = self.catalogs.read().await;
        let catalog = catalogs
            .get(&id)
            .ok_or_else(|| MetastoreError::MissingCatalog(id))?;

        let mutations = req
            .mutations
            .into_iter()
            .map(|m| Mutation::try_from(m).map_err(MetastoreError::from))
            .collect::<Result<_, _>>()?;

        // TODO: Catch error and return status.

        let updated = catalog.try_mutate(req.catalog_version, mutations).await?;

        Ok(Response::new(MutateResponse {
            status: service::mutate_response::Status::Applied as i32,
            catalog: Some(updated.into()),
        }))
    }
}

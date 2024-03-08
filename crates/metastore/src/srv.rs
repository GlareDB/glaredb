use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use object_store::ObjectStore;
use protogen::gen::metastore::options::TableOptions as ProtoTableOptions;
use protogen::gen::metastore::service::metastore_service_server::MetastoreService;
use protogen::gen::metastore::service::mutation::Mutation as ProtoMutationType;
use protogen::gen::metastore::service::{
    self,
    FetchCatalogRequest,
    FetchCatalogResponse,
    MutateRequest,
    MutateResponse,
    Mutation as ProtoMutation,
};
use protogen::metastore::types::options::TableOptionsOld;
use protogen::metastore::types::service::{CreateExternalTable, Mutation};
use protogen::ProtoConvError;
use tonic::{Request, Response, Status};
use tracing::{debug, info};
use uuid::Uuid;

use crate::database::DatabaseCatalog;
use crate::errors::MetastoreError;
use crate::storage::persist::Storage;
/// Metastore GRPC service.
pub struct Service {
    /// Reference to underlying object storage.
    storage: Arc<Storage>,
    /// Database catalogs that this process knows about.
    ///
    /// This is filled on demand. There's currently no method for dropping
    /// unused catalogs (other than restarts).
    ///
    /// Any number of Metastore instances may have references for a single
    /// database catalog. Catalog mutations are synchronized at the storage
    /// layer. It is possible for Metastore to serve out of date catalogs, but
    /// it's not possible to make mutations against an out of date catalog.
    catalogs: DashMap<Uuid, Arc<DatabaseCatalog>>,
}

impl Service {
    pub fn new(store: Arc<dyn ObjectStore>) -> Service {
        println!("Creating new Metastore service");
        let process_id = Uuid::new_v4();
        info!(%process_id, "Creating new Metastore service");

        let storage = Arc::new(Storage::new(process_id, store));
        Service {
            storage,
            catalogs: DashMap::new(),
        }
    }

    /// Get an already loaded catalog, or load it into memory.
    async fn get_or_load_catalog(
        &self,
        db_id: Uuid,
    ) -> Result<Arc<DatabaseCatalog>, MetastoreError> {
        // Fast path, already have the catalog.
        if let Some(catalog) = self.catalogs.get(&db_id) {
            return Ok(catalog.value().clone());
        }

        let catalog = Arc::new(DatabaseCatalog::open(db_id, self.storage.clone()).await?);

        if self.catalogs.insert(db_id, catalog.clone()).is_some() {
            // If there's an entry, it means we raced. However this isn't an
            // issue since the in-memory catalog is really a reference to what's
            // in object store. Every attempt to mutate the catalog will ensure
            // that it's in sync with object store.
            debug!(%db_id, "catalog init raced");
        }

        Ok(catalog)
    }
}

#[async_trait]
impl MetastoreService for Service {
    async fn fetch_catalog(
        &self,
        request: Request<FetchCatalogRequest>,
    ) -> Result<Response<FetchCatalogResponse>, Status> {
        let req = request.into_inner();
        debug!(?req, "fetch catalog");
        let id = Uuid::from_slice(&req.db_id)
            .map_err(|_| MetastoreError::InvalidDatabaseId(req.db_id))?;

        let catalog = self.get_or_load_catalog(id).await?;
        let state = catalog.get_state().await?;

        Ok(Response::new(FetchCatalogResponse {
            catalog: Some(state.try_into().map_err(MetastoreError::from)?),
        }))
    }

    async fn mutate_catalog(
        &self,
        request: Request<MutateRequest>,
    ) -> Result<Response<MutateResponse>, Status> {
        let req = request.into_inner();

        let id = Uuid::from_slice(&req.db_id)
            .map_err(|_| MetastoreError::InvalidDatabaseId(req.db_id))?;

        let catalog = self.get_or_load_catalog(id).await?;

        let mutations = req
            .mutations
            .into_iter()
            .map(|m| Mutation::try_from(m).map_err(MetastoreError::from))
            .collect::<Result<_, _>>()?;
        // TODO: Catch error and return status.

        let updated = catalog.try_mutate(req.catalog_version, mutations).await?;

        Ok(Response::new(MutateResponse {
            status: service::mutate_response::Status::Applied as i32,
            catalog: Some(updated.try_into().map_err(MetastoreError::from)?),
        }))
    }
}


#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use protogen::metastore::types::catalog::{CatalogEntry, CatalogState};
    use protogen::metastore::types::service::{CreateSchema, Mutation};

    use super::*;

    fn new_service() -> Service {
        let store = Arc::new(InMemory::new());
        Service::new(store)
    }

    #[tokio::test]
    async fn first_fetch() {
        let svc = new_service();
        svc.fetch_catalog(Request::new(FetchCatalogRequest {
            db_id: Uuid::new_v4().into_bytes().to_vec(),
        }))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn simple_mutate() {
        let svc = new_service();
        let id = Uuid::new_v4();
        let id_bs = id.into_bytes().to_vec();

        // Fetch initial catalog.
        let resp = svc
            .fetch_catalog(Request::new(FetchCatalogRequest {
                db_id: id_bs.clone(),
            }))
            .await
            .unwrap();
        let resp = resp.into_inner();

        // Mutate (create schema)
        svc.mutate_catalog(Request::new(MutateRequest {
            db_id: id_bs.clone(),
            catalog_version: resp.catalog.unwrap().version,
            mutations: vec![Mutation::CreateSchema(CreateSchema {
                name: "test_schema".to_string(),
                if_not_exists: false,
            })
            .try_into()
            .unwrap()],
        }))
        .await
        .unwrap();

        // Fetch new catalog.
        let resp = svc
            .fetch_catalog(Request::new(FetchCatalogRequest {
                db_id: id_bs.clone(),
            }))
            .await
            .unwrap();
        let resp = resp.into_inner();

        // Check that we got the new schema.
        let state: CatalogState = resp.catalog.unwrap().try_into().unwrap();
        let ent = state
            .entries
            .into_values()
            .find(|ent| ent.get_meta().name == "test_schema")
            .unwrap();
        assert!(matches!(ent, CatalogEntry::Schema(_)));
    }
}

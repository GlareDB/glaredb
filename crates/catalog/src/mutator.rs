use std::sync::Arc;

use protogen::metastore::strategy::ResolveErrorStrategy;
use protogen::metastore::types::catalog::CatalogState;
use protogen::metastore::types::service::Mutation;
use tracing::debug;

use super::client::MetastoreClientHandle;
use crate::errors::{CatalogError, Result};

/// Wrapper around a metastore client for mutating the catalog.
#[derive(Clone)]
pub struct CatalogMutator {
    pub client: Option<MetastoreClientHandle>,
}

impl CatalogMutator {
    pub fn empty() -> Self {
        CatalogMutator { client: None }
    }

    pub fn new(client: Option<MetastoreClientHandle>) -> Self {
        CatalogMutator { client }
    }

    pub fn get_metastore_client(&self) -> Option<&MetastoreClientHandle> {
        self.client.as_ref()
    }

    /// Mutate the catalog if possible.
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date
    /// catalog.
    pub async fn mutate(
        &self,
        catalog_version: u64,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let client = match &self.client {
            Some(client) => client,
            None => return Err(CatalogError::new("metastore client not configured")),
        };

        // Note that when we have transactions, these shouldn't be sent until
        // commit.
        let mutations: Vec<_> = mutations.into_iter().collect();
        let state = match client.try_mutate(catalog_version, mutations.clone()).await {
            Ok(state) => state,
            Err(CatalogError {
                msg,
                strategy: Some(ResolveErrorStrategy::FetchCatalogAndRetry),
            }) => {
                // Go ahead and refetch the catalog and retry the mutation.
                //
                // Note that this relies on metastore _always_ being stricter
                // when validating mutations. What this means is that retrying
                // here should be semantically equivalent to manually refreshing
                // the catalog and rerunning and replanning the query.
                debug!(error_message = msg, "retrying mutations");

                client.refresh_cached_state().await?;
                let state = client.get_cached_state().await?;
                let version = state.version;

                client.try_mutate(version, mutations).await?
            }
            Err(e) => return Err(e),
        };

        Ok(state)
    }
}

impl From<MetastoreClientHandle> for CatalogMutator {
    fn from(value: MetastoreClientHandle) -> Self {
        CatalogMutator {
            client: Some(value),
        }
    }
}

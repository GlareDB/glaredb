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

    pub fn is_empty(&self) -> bool {
        self.client.is_none()
    }
    pub fn new(client: Option<MetastoreClientHandle>) -> Self {
        CatalogMutator { client }
    }

    pub fn get_metastore_client(&self) -> Option<&MetastoreClientHandle> {
        self.client.as_ref()
    }

    /// Commit the catalog state.
    /// This persists the state to the metastore.
    pub async fn commit_state(
        &self,
        catalog_version: u64,
        state: CatalogState,
    ) -> Result<Arc<CatalogState>> {
        let client = match &self.client {
            Some(client) => client,
            None => return Err(CatalogError::new("metastore client not configured")),
        };

        let state = match client.commit_state(catalog_version, state).await {
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

                client.commit_state(version, state.as_ref().clone()).await?
            }
            Err(e) => return Err(e),
        };

        Ok(state)
    }

    /// Mutate the catalog if possible.
    /// This returns the catalog state with the mutations reflected.
    /// IMPORTANT: these changes are not yet persisted and must be 'committed' manually via `commit_state`.
    /// If you wish to mutate and immediately commit, use `mutate_and_commit`
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date catalog.
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

    /// Mutate the catalog if possible and immediately commit the changes.
    ///
    /// Errors if the metastore client isn't configured.
    ///
    /// This will retry mutations if we were working with an out of date
    /// catalog.
    pub async fn mutate_and_commit(
        &self,
        catalog_version: u64,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<Arc<CatalogState>> {
        let state = self.mutate(catalog_version, mutations).await?;
        self.commit_state(catalog_version, state.as_ref().clone())
            .await
    }
}

impl From<MetastoreClientHandle> for CatalogMutator {
    fn from(value: MetastoreClientHandle) -> Self {
        CatalogMutator {
            client: Some(value),
        }
    }
}

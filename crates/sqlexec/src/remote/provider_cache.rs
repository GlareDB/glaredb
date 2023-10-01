use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use datafusion::datasource::TableProvider;
use uuid::Uuid;

/// Cache for table providers on the remote side.
// TODO: Need to occasionally clean out.
#[derive(Default)]
pub struct ProviderCache {
    providers: DashMap<Uuid, Arc<dyn TableProvider>>,
}

impl ProviderCache {
    pub fn put(&self, id: Uuid, table: Arc<dyn TableProvider>) {
        self.providers.insert(id, table);
    }

    pub fn get(&self, id: &Uuid) -> Option<Arc<dyn TableProvider>> {
        let prov = self.providers.get(id)?;
        Some(prov.value().clone())
    }
}

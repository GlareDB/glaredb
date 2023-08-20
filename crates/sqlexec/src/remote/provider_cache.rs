use std::{collections::HashMap, sync::Arc};

use datafusion::datasource::TableProvider;
use uuid::Uuid;

/// Cache for table providers on the remote side.
// TODO: Need to occasionally clean out.
#[derive(Default)]
pub struct ProviderCache {
    providers: HashMap<Uuid, Arc<dyn TableProvider>>,
}

impl ProviderCache {
    pub fn put(&mut self, id: Uuid, table: Arc<dyn TableProvider>) {
        self.providers.insert(id, table);
    }

    pub fn get(&self, id: &Uuid) -> Option<Arc<dyn TableProvider>> {
        self.providers.get(id).cloned()
    }
}

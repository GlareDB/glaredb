use std::sync::Arc;

use crate::catalog::context::{DatabaseContext, SYSTEM_CATALOG};
use crate::catalog::database::{AccessMode, Database};
use crate::catalog::system::new_system_catalog;
use crate::storage::storage_manager::StorageManager;

/// Create a test database context.
///
/// The context will have a system catalog.
pub fn test_db_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(Database {
        name: SYSTEM_CATALOG.to_string(),
        mode: AccessMode::ReadOnly,
        catalog: Arc::new(new_system_catalog().unwrap()),
        storage: Arc::new(StorageManager::empty()),
        attach_info: None,
    }))
    .unwrap()
}

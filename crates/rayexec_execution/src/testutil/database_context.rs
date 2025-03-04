use std::sync::Arc;

use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;

/// Create a test database context.
///
/// The context will have a system catalog, and a default (empty) data source
/// registry.
pub fn test_db_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(
        new_system_catalog(&DataSourceRegistry::default()).unwrap(),
    ))
    .unwrap()
}

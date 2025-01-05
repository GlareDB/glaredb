use std::sync::Arc;

use crate::database::system::new_system_catalog;
use crate::database::DatabaseContext;
use crate::datasource::DataSourceRegistry;

pub fn test_database_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(
        new_system_catalog(&DataSourceRegistry::default()).unwrap(),
    ))
    .unwrap()
}

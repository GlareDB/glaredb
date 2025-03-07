use std::sync::Arc;

use crate::catalog::context::{AccessMode, Database, DatabaseContext};
use crate::catalog::system::new_system_catalog;

/// Create a test database context.
///
/// The context will have a system catalog.
pub fn test_db_context() -> DatabaseContext {
    DatabaseContext::new(Arc::new(Database {
        mode: AccessMode::ReadOnly,
        catalog: new_system_catalog().unwrap(),
        attach_info: None,
    }))
    .unwrap()
}

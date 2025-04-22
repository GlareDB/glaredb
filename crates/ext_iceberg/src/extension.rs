use glaredb_core::extension::{Extension, ExtensionTableFunction};

use crate::catalog::RESTCatalog;

#[derive(Debug, Clone, Copy)]
pub struct IcebergExtension;

impl Extension for IcebergExtension {
    const NAME: &str = "iceberg";
    const FUNCTION_NAMESPACE: Option<&str> = Some("iceberg");

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        &[]
    }
}

pub fn create_rest_catalog(base_url: String) -> RESTCatalog {
    RESTCatalog::new(base_url)
}

use glaredb_core::extension::{Extension, ExtensionTableFunction};

#[derive(Debug, Clone, Copy)]
pub struct IcebergExtension;

impl Extension for IcebergExtension {
    const NAME: &str = "iceberg";
    const FUNCTION_NAMESPACE: Option<&str> = Some("iceberg");

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        &[]
    }
}

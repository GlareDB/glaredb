use glaredb_core::extension::{Extension, ExtensionTableFunction};

use crate::functions::metadata::{
    FUNCTION_SET_ICEBERG_DATA_FILES,
    FUNCTION_SET_ICEBERG_MANIFEST_LIST,
    FUNCTION_SET_ICEBERG_METADATA,
    FUNCTION_SET_ICEBERG_SNAPSHOTS,
};

#[derive(Debug, Clone, Copy)]
pub struct IcebergExtension;

impl Extension for IcebergExtension {
    const NAME: &str = "iceberg";
    // TODO: Hmm
    const FUNCTION_NAMESPACE: Option<&str> = None;

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        const FUNCTIONS: &[ExtensionTableFunction] = &[
            // Metadata functions
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_SNAPSHOTS),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_MANIFEST_LIST),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_DATA_FILES),
        ];

        FUNCTIONS
    }
}

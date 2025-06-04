use glaredb_core::extension::{Extension, ExtensionFunctions, ExtensionTableFunction};

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

    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        namespace: "iceberg",
        scalar: &[],
        aggregate: &[],
        table: &[
            // Metadata functions
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_SNAPSHOTS),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_MANIFEST_LIST),
            ExtensionTableFunction::new(&FUNCTION_SET_ICEBERG_DATA_FILES),
        ],
    });
}

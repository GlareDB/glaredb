use glaredb_core::catalog::create::FileInferScan;
use glaredb_core::extension::{Extension, ExtensionTableFunction};

use crate::functions::metadata::{
    FUNCTION_SET_PARQUET_COLUMN_METADATA,
    FUNCTION_SET_PARQUET_FILE_METADATA,
    FUNCTION_SET_PARQUET_ROWGROUP_METADATA,
};
use crate::functions::scan::FUNCTION_SET_READ_PARQUET;

#[derive(Debug, Clone, Copy)]
pub struct ParquetExtension;

impl Extension for ParquetExtension {
    const NAME: &str = "parquet";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        const FUNCTIONS: &[ExtensionTableFunction] = &[
            // Metadata functions
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_FILE_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_ROWGROUP_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_COLUMN_METADATA),
            // Scan functions
            ExtensionTableFunction {
                infer_scan: Some(FileInferScan {
                    can_handle: |path| path.ends_with(".parquet"), // TODO: Not sure what we want to do with compression extensions yet.
                }),
                function: &FUNCTION_SET_READ_PARQUET,
            },
        ];

        FUNCTIONS
    }
}

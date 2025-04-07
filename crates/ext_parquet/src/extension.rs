use glaredb_core::extension::Extension;
use glaredb_core::functions::function_set::TableFunctionSet;

use crate::functions::metadata::FUNCTION_SET_PARQUET_FILE_METADATA;

#[derive(Debug, Clone, Copy)]
pub struct ParquetExtension;

impl Extension for ParquetExtension {
    const NAME: &str = "parquet";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.

    fn table_functions(&self) -> &[TableFunctionSet] {
        &[FUNCTION_SET_PARQUET_FILE_METADATA]
    }
}

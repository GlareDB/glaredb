use glaredb_core::extension::Extension;

#[derive(Debug, Clone, Copy)]
pub struct ParquetExtension;

impl Extension for ParquetExtension {
    const NAME: &str = "parquet";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.
}

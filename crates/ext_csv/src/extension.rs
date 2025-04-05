use glaredb_core::extension::Extension;

#[derive(Debug, Clone, Copy)]
pub struct CsvExtension {}

impl CsvExtension {
    pub const fn new() -> Self {
        CsvExtension {}
    }
}

impl Extension for CsvExtension {
    const NAME: &str = "csv";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.
}

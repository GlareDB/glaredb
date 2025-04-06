use glaredb_core::extension::Extension;
use glaredb_core::functions::function_set::TableFunctionSet;

use crate::functions::read_csv::FUNCTION_SET_READ_CSV;

#[derive(Debug, Clone, Copy)]
pub struct CsvExtension;

impl Extension for CsvExtension {
    const NAME: &str = "csv";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.

    fn table_functions(&self) -> &[TableFunctionSet] {
        &[FUNCTION_SET_READ_CSV]
    }
}

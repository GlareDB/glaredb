use glaredb_core::catalog::create::FileInferScan;
use glaredb_core::extension::{Extension, ExtensionTableFunction};

use crate::functions::read_csv::FUNCTION_SET_READ_CSV;

#[derive(Debug, Clone, Copy)]
pub struct CsvExtension;

impl Extension for CsvExtension {
    const NAME: &str = "csv";
    const FUNCTION_NAMESPACE: Option<&str> = None; // Place functions in default schema.

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        const FUNCTIONS: &[ExtensionTableFunction] = &[
            // Scan functions
            ExtensionTableFunction {
                infer_scan: Some(FileInferScan {
                    can_handle: |path| path.ends_with(".csv"), // TODO: See parquet... also tsv
                }),
                function: FUNCTION_SET_READ_CSV,
            },
        ];

        FUNCTIONS
    }
}

use glaredb_core::catalog::create::FileInferScan;
use glaredb_core::extension::{
    Extension,
    ExtensionFunctions,
    ExtensionTableFunction,
    TableFunctionAliasesInDefault,
};

use crate::functions::read_csv::FUNCTION_SET_READ_CSV;

#[derive(Debug, Clone, Copy)]
pub struct CsvExtension;

impl Extension for CsvExtension {
    const NAME: &str = "csv";

    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        namespace: "csv",
        scalar: &[],
        aggregate: &[],
        table: &[
            // Scan functions
            ExtensionTableFunction {
                aliases_in_default: Some(TableFunctionAliasesInDefault {
                    aliases: &["read_csv", "csv_scan"],
                    infer_scan: Some(FileInferScan {
                        can_handle: |path| path.ends_with(".csv") || path.ends_with(".tsv"), // TODO: See parquet...
                    }),
                }),
                function: &FUNCTION_SET_READ_CSV,
            },
        ],
    });
}

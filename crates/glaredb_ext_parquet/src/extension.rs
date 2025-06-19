use glaredb_core::catalog::create::FileInferScan;
use glaredb_core::extension::{
    Extension,
    ExtensionFunctions,
    ExtensionTableFunction,
    TableFunctionAliasesInDefault,
};

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

    const FUNCTIONS: Option<&'static ExtensionFunctions> = Some(&ExtensionFunctions {
        namespace: "parquet",
        scalar: &[],
        aggregate: &[],
        table: &[
            // Metadata functions
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_FILE_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_ROWGROUP_METADATA),
            ExtensionTableFunction::new(&FUNCTION_SET_PARQUET_COLUMN_METADATA),
            // Scan functions
            ExtensionTableFunction {
                aliases_in_default: Some(TableFunctionAliasesInDefault {
                    aliases: &["read_parquet", "parquet_scan"],
                    infer_scan: Some(FileInferScan {
                        can_handle: |path| path.ends_with(".parquet"), // TODO: Not sure what we want to do with compression extensions yet.
                    }),
                }),
                function: &FUNCTION_SET_READ_PARQUET,
            },
        ],
    });
}

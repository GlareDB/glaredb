use crate::catalog::create::FileInferScan;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

pub trait Extension {
    /// The name of the extension.
    const NAME: &str;

    /// Functions provided by this extension, if any.
    const FUNCTIONS: Option<&'static ExtensionFunctions>;
}

#[derive(Debug, Clone, Copy)]
pub struct ExtensionFunctions {
    /// Namespace (schema) to place all the functions.
    ///
    /// This will create a schema in the system catalog with this name. It must
    /// be unique.
    pub namespace: &'static str,
    /// Scalar functions provided by this extension.
    pub scalar: &'static [&'static ScalarFunctionSet],
    /// Aggregate functions provided by this extension.
    pub aggregate: &'static [&'static AggregateFunctionSet],
    /// Table functions provided by this extension.
    pub table: &'static [ExtensionTableFunction],
}

#[derive(Debug, Clone, Copy)]
pub struct ExtensionTableFunction {
    /// The table function.
    pub function: &'static TableFunctionSet,
    /// Optionally alias this function into the default schema.
    pub aliases_in_default: Option<TableFunctionAliasesInDefault>,
}

#[derive(Debug, Clone, Copy)]
pub struct TableFunctionAliasesInDefault {
    /// A set of aliases for this function that will be placed in the default
    /// schema.
    ///
    /// This should be used sparingly, and for the main entry points to an
    /// extension (e.g. scan functions).
    ///
    /// For example, we can alias 'parquet.scan' to `read_parquet` and
    /// `parquet_scan` in the default schema so the user won't have to qualify
    /// the function call.
    pub aliases: &'static [&'static str],
    /// Optionally allow automatic function selection by trying to match a
    /// string path.
    ///
    /// Allows for queries like `SELECT * FROM 'myfile.csv'` to automatically
    /// select the ReadCsv function.
    ///
    /// Note that this is only available to functions that are being aliases
    /// into the default schema.
    pub infer_scan: Option<FileInferScan>,
}

impl ExtensionTableFunction {
    pub const fn new(function: &'static TableFunctionSet) -> Self {
        ExtensionTableFunction {
            function,
            aliases_in_default: None,
        }
    }
}

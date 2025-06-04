use crate::catalog::create::FileInferScan;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

pub trait Extension {
    /// The name of the extension.
    const NAME: &str;

    /// Functions provided by this extension, if any.
    const FUNCTIONS: Option<&'static ExtensionFunctions>;

    /// An optional namespace for functions in this extension.
    ///
    /// This will create a schema in the system catalog with this name. It must
    /// be unique.
    ///
    /// If None, functions will be placed in the default schema.
    const FUNCTION_NAMESPACE: Option<&str>;

    fn scalar_functions(&self) -> &[&'static ScalarFunctionSet] {
        &[]
    }

    fn aggregate_functions(&self) -> &[&'static AggregateFunctionSet] {
        &[]
    }

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        &[]
    }
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
    /// Optionally allow automatic function selection by trying to match a
    /// string path.
    ///
    /// Allows for queries like `SELECT * FROM 'myfile.csv'` to automatically
    /// select the ReadCsv function.
    pub infer_scan: Option<FileInferScan>,
    /// An optional set of aliases for this function that will be placed in the
    /// default schema.
    ///
    /// This should be used sparingly, and for the main entry points to an
    /// extension (e.g. scan functions).
    ///
    /// For example, we can alias 'parquet.scan' to `read_parquet` and
    /// `parquet_scan` in the default schema so the user won't have to qualify
    /// the function call.
    pub aliases_in_default: &'static [&'static str],
    /// The table function.
    pub function: &'static TableFunctionSet,
}

impl ExtensionTableFunction {
    pub const fn new(function: &'static TableFunctionSet) -> Self {
        ExtensionTableFunction {
            infer_scan: None,
            aliases_in_default: &[],
            function,
        }
    }
}

use crate::catalog::create::FileInferScan;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

pub trait Extension {
    /// The name of the extension.
    const NAME: &str;

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
pub struct ExtensionTableFunction {
    pub infer_scan: Option<FileInferScan>,
    pub function: &'static TableFunctionSet,
}

impl ExtensionTableFunction {
    pub const fn new(function: &'static TableFunctionSet) -> Self {
        ExtensionTableFunction {
            infer_scan: None,
            function,
        }
    }
}

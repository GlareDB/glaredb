pub mod file_reference_handler;

use file_reference_handler::FileReferenceHandler;

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

    fn scalar_functions(&self) -> &[ScalarFunctionSet] {
        &[]
    }

    fn aggregate_functions(&self) -> &[AggregateFunctionSet] {
        &[]
    }

    fn table_functions(&self) -> &[TableFunctionSet] {
        &[]
    }

    fn file_reference_handlers(&self) -> &[FileReferenceHandler] {
        &[]
    }
}

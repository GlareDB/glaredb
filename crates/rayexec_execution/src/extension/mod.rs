pub mod file_reference_handler;

use file_reference_handler::FileReferenceHandler;

use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

pub trait Extension {
    const NAME: &str;

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

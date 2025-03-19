pub mod functions;

use functions::FUNCTION_SET_REGION;
use glaredb_execution::extension::Extension;
use glaredb_execution::functions::function_set::TableFunctionSet;

#[derive(Debug, Clone, Copy)]
pub struct TpchGenExtension;

impl Extension for TpchGenExtension {
    const NAME: &str = "tpch_gen";
    const FUNCTION_NAMESPACE: Option<&str> = Some("tpch_gen");

    fn table_functions(&self) -> &[TableFunctionSet] {
        &[FUNCTION_SET_REGION]
    }
}

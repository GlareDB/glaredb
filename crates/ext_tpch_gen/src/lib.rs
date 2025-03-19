pub mod functions;

use glaredb_execution::extension::Extension;

#[derive(Debug, Clone, Copy)]
pub struct TpchGenExtension;

impl Extension for TpchGenExtension {
    const NAME: &str = "tpch_gen";
    const FUNCTION_NAMESPACE: Option<&str> = Some("tpch_gen");
}

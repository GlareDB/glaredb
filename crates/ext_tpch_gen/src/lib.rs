pub mod functions;

use functions::{
    FUNCTION_SET_CUSTOMER,
    FUNCTION_SET_LINEITEM,
    FUNCTION_SET_NATION,
    FUNCTION_SET_ORDERS,
    FUNCTION_SET_PART,
    FUNCTION_SET_PARTSUPP,
    FUNCTION_SET_REGION,
    FUNCTION_SET_SUPPLIER,
};
use glaredb_core::extension::{Extension, ExtensionTableFunction};

#[derive(Debug, Clone, Copy)]
pub struct TpchGenExtension;

impl Extension for TpchGenExtension {
    const NAME: &str = "tpch_gen";
    const FUNCTION_NAMESPACE: Option<&str> = Some("tpch_gen");

    fn table_functions(&self) -> &[ExtensionTableFunction] {
        const FUNCTIONS: &[ExtensionTableFunction] = &[
            ExtensionTableFunction::new(FUNCTION_SET_REGION),
            ExtensionTableFunction::new(FUNCTION_SET_PART),
            ExtensionTableFunction::new(FUNCTION_SET_SUPPLIER),
            ExtensionTableFunction::new(FUNCTION_SET_CUSTOMER),
            ExtensionTableFunction::new(FUNCTION_SET_PARTSUPP),
            ExtensionTableFunction::new(FUNCTION_SET_ORDERS),
            ExtensionTableFunction::new(FUNCTION_SET_LINEITEM),
            ExtensionTableFunction::new(FUNCTION_SET_NATION),
        ];

        FUNCTIONS
    }
}

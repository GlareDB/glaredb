pub mod list_databases;
pub mod list_schemas;
pub mod memory_scan;
pub mod series;
pub mod unnest;

use list_databases::FUNCTION_SET_LIST_DATABASES;
use list_schemas::FUNCTION_SET_LIST_SCHEMAS;
use memory_scan::FUNCTION_SET_MEMORY_SCAN;
use series::FUNCTION_SET_GENERATE_SERIES;

use crate::functions::function_set::TableFunctionSet;

pub const BUILTIN_TABLE_FUNCTION_SETS: &[TableFunctionSet] = &[
    FUNCTION_SET_GENERATE_SERIES,
    // System functions.
    FUNCTION_SET_LIST_DATABASES,
    FUNCTION_SET_LIST_SCHEMAS,
    // Scan functions.
    FUNCTION_SET_MEMORY_SCAN,
];

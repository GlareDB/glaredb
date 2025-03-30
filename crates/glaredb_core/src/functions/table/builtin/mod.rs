pub mod list_databases;
pub mod list_entries;
pub mod list_schemas;
pub mod memory_scan;
pub mod profile;
pub mod series;
pub mod unnest;

use list_databases::FUNCTION_SET_LIST_DATABASES;
use list_entries::{
    FUNCTION_SET_LIST_FUNCTIONS,
    FUNCTION_SET_LIST_TABLES,
    FUNCTION_SET_LIST_VIEWS,
};
use list_schemas::FUNCTION_SET_LIST_SCHEMAS;
use memory_scan::FUNCTION_SET_MEMORY_SCAN;
use profile::{
    FUNCTION_SET_EXECUTION_PROFILE,
    FUNCTION_SET_OPTIMIZER_PROFILE,
    FUNCTION_SET_PLANNING_PROFILE,
    FUNCTION_SET_QUERY_INFO,
};
use series::FUNCTION_SET_GENERATE_SERIES;
use unnest::FUNCTION_SET_UNNEST;

use crate::functions::function_set::TableFunctionSet;

pub const BUILTIN_TABLE_FUNCTION_SETS: &[TableFunctionSet] = &[
    FUNCTION_SET_GENERATE_SERIES,
    FUNCTION_SET_UNNEST,
    // System functions.
    FUNCTION_SET_LIST_DATABASES,
    FUNCTION_SET_LIST_SCHEMAS,
    FUNCTION_SET_LIST_TABLES,
    FUNCTION_SET_LIST_VIEWS,
    FUNCTION_SET_LIST_FUNCTIONS,
    // Profile functions
    FUNCTION_SET_PLANNING_PROFILE,
    FUNCTION_SET_OPTIMIZER_PROFILE,
    FUNCTION_SET_EXECUTION_PROFILE,
    FUNCTION_SET_QUERY_INFO,
    // Scan functions.
    FUNCTION_SET_MEMORY_SCAN,
];

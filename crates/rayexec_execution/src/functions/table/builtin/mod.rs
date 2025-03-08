pub mod list_databases;
pub mod series;
pub mod unnest;

use list_databases::FUNCTION_SET_LIST_DATABASES;
use series::FUNCTION_SET_GENERATE_SERIES;

use crate::functions::function_set::TableFunctionSet;

pub const BUILTIN_TABLE_FUNCTION_SETS: &[TableFunctionSet] = &[
    FUNCTION_SET_GENERATE_SERIES,
    // System functions.
    FUNCTION_SET_LIST_DATABASES,
];

pub mod series;
pub mod system;
pub mod unnest;

use series::FUNCTION_SET_GENERATE_SERIES;

use crate::functions::function_set::TableFunctionSet;

pub const BUILTIN_TABLE_FUNCTION_SETS: &[TableFunctionSet] = &[FUNCTION_SET_GENERATE_SERIES];

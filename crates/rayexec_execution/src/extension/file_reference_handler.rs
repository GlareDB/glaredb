use regex::Regex;

use crate::functions::function_set::TableFunctionSet;

/// Allow handling of `SELECT * FROM my_file.csv` queries by replacing the file
/// reference with a call to a table function.
///
/// The table function should accept the file name as the first argument.
#[derive(Debug, Clone)]
pub struct FileReferenceHandler {
    /// Regex to use to determine if this handler should handle the file.
    pub regex: Regex,
    /// Table function to use to read the file.
    pub table_func: TableFunctionSet,
}

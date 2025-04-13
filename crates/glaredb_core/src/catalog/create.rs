//! Various create messages/structs.

use crate::arrays::field::Field;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet, TableFunctionSet};

/// Behavior on create conflict.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OnConflict {
    /// Ignore and return ok.
    ///
    /// CREATE IF NOT EXIST
    Ignore,
    /// Replace the original entry.
    ///
    /// CREATE OR REPLACE
    Replace,
    /// Error on conflict.
    #[default]
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableInfo {
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchemaInfo {
    pub name: String,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewInfo {
    pub name: String,
    pub column_aliases: Option<Vec<String>>,
    pub on_conflict: OnConflict,
    // TODO: Currently just stores the string, would be nice to store something
    // a bit more structured like a parsed or bound state.
    //
    // But that would required they be a bit more stable than they currently are.
    pub query_string: String,
}

#[derive(Debug)]
pub struct CreateScalarFunctionInfo {
    pub name: String,
    pub implementation: ScalarFunctionSet,
    pub on_conflict: OnConflict,
}

#[derive(Debug)]
pub struct CreateAggregateFunctionInfo {
    pub name: String,
    pub implementation: AggregateFunctionSet,
    pub on_conflict: OnConflict,
}

/// Information needed for adding a table function to the catalog.
#[derive(Debug)]
pub struct CreateTableFunctionInfo {
    pub name: String,
    pub implementation: TableFunctionSet,
    pub infer_scan: Option<FileInferScan>,
    pub on_conflict: OnConflict,
}

/// Allow inferring the table function to use for a file path.
///
/// If `can_handle` returns true, then the table function will be used in the
/// query. The table function should accept a single argument -- the path to the
/// file.
#[derive(Debug, Clone, Copy)]
pub struct FileInferScan {
    pub can_handle: fn(path: &str) -> bool,
}

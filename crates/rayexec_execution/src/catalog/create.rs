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

#[derive(Debug)]
pub struct CreateTableFunctionInfo {
    pub name: String,
    pub implementation: TableFunctionSet,
    pub on_conflict: OnConflict,
}

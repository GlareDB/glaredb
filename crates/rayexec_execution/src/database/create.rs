//! Various create messages/structs.
use crate::functions::{aggregate::GenericAggregateFunction, scalar::GenericScalarFunction};
use rayexec_bullet::field::Field;

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

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableInfo {
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSchemaInfo {
    pub name: String,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq)]
pub struct CreateScalarFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn GenericScalarFunction>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq)]
pub struct CreateAggregateFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn GenericAggregateFunction>,
    pub on_conflict: OnConflict,
}

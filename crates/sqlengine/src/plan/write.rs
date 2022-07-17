use super::read::ReadPlan;
use crate::catalog::{TableReference, TableSchema};

#[derive(Debug, PartialEq)]
pub enum WritePlan {
    Insert(Insert),
    CreateTable(CreateTable),
}

#[derive(Debug, PartialEq)]
pub struct Insert {
    pub table: TableReference,
    pub input: ReadPlan,
}

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub schema: TableSchema,
}

impl WritePlan {
    /// Get a mutable reference to the read portion of the plan if it exists.
    pub fn get_read_mut(&mut self) -> Option<&mut ReadPlan> {
        match self {
            WritePlan::Insert(insert) => Some(&mut insert.input),
            _ => None,
        }
    }
}

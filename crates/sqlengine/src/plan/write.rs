use super::read::ReadPlan;
use crate::catalog::{TableReference, TableSchema};
use anyhow::{Result};
use lemur::repr::expr::{self, MutateRelationExpr};

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

#[derive(Debug, PartialEq, Eq)]
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

    pub fn lower(self) -> Result<MutateRelationExpr> {
        Ok(match self {
            WritePlan::Insert(Insert { table, input }) => {
                MutateRelationExpr::Insert(expr::Insert {
                    table: table.to_string(),
                    input: input.lower()?,
                })
            }
            WritePlan::CreateTable(CreateTable { schema }) => {
                MutateRelationExpr::CreateTable(expr::CreateTable {
                    table: schema.name.clone(),
                    schema: schema.to_schema(),
                })
            }
        })
    }
}

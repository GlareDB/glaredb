use std::fmt;

use crate::arrays::datatype::DataType;
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

/// Reference to a table in a context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableRef {
    pub table_idx: usize,
}

impl From<usize> for TableRef {
    fn from(value: usize) -> Self {
        TableRef { table_idx: value }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.table_idx)
    }
}

/// Reference to a table inside a scope.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAlias {
    pub database: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

impl TableAlias {
    pub fn matches(&self, other: &TableAlias) -> bool {
        match (&self.database, &other.database) {
            (Some(a), Some(b)) if a != b => return false,
            _ => (),
        }
        match (&self.schema, &other.schema) {
            (Some(a), Some(b)) if a != b => return false,
            _ => (),
        }

        self.table == other.table
    }
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(f, "{database}.")?;
        }
        if let Some(schema) = &self.schema {
            write!(f, "{schema}.")?;
        }
        write!(f, "{}", self.table)
    }
}

/// A "table" in the context.
///
/// A table is a logical collection of columns that get output from a node in
/// the plan. A node may produce multiple tables.
///
/// For example, when a query has aggregates in the select list, a separate
/// "aggregates" table will be created for hold columns that produce aggregates,
/// and the original select list will have their expressions replaced with
/// column references that point to this table.
#[derive(Debug, Clone)]
pub struct Table {
    pub reference: TableRef,
    pub alias: Option<TableAlias>,
    pub column_types: Vec<DataType>,
    pub column_names: Vec<String>,
}

impl Table {
    pub fn num_columns(&self) -> usize {
        self.column_types.len()
    }
}

#[derive(Debug, Clone)]
pub struct TableList {
    /// All tables in the bind context. Tables may or may not be inside a scope.
    ///
    /// Referenced via `TableRef`.
    pub(super) tables: Vec<Table>,
}

impl TableList {
    pub const fn empty() -> Self {
        TableList { tables: Vec::new() }
    }

    /// Get a table by table ref.
    pub fn get(&self, table_ref: TableRef) -> Result<&Table> {
        self.tables
            .get(table_ref.table_idx)
            .ok_or_else(|| RayexecError::new(format!("Missing table in table list: {table_ref}")))
    }

    /// Get a mutable table by table ref.
    pub fn get_mut(&mut self, table_ref: TableRef) -> Result<&mut Table> {
        self.tables
            .get_mut(table_ref.table_idx)
            .ok_or_else(|| RayexecError::new(format!("Missing table in table list: {table_ref}")))
    }

    pub fn push_table(
        &mut self,
        alias: Option<TableAlias>,
        column_types: Vec<DataType>,
        column_names: Vec<String>,
    ) -> Result<TableRef> {
        if column_types.len() != column_names.len() {
            return Err(
                RayexecError::new("Column names and types have different lengths")
                    .with_fields([("types", column_types.len()), ("names", column_names.len())]),
            );
        }

        let table_idx = self.tables.len();
        let reference = TableRef { table_idx };
        let table = Table {
            reference,
            alias,
            column_types,
            column_names,
        };
        self.tables.push(table);

        Ok(reference)
    }

    pub fn get_column(&self, table_ref: TableRef, col_idx: usize) -> Result<(&str, &DataType)> {
        let table = self.get(table_ref)?;
        let name = table
            .column_names
            .get(col_idx)
            .map(|s| s.as_str())
            .ok_or_else(|| {
                RayexecError::new(format!("Missing column {col_idx} in table {table_ref}"))
            })?;
        let datatype = &table.column_types[col_idx];
        Ok((name, datatype))
    }
}

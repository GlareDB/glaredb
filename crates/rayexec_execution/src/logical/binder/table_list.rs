use std::fmt;

use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use crate::arrays::datatype::DataType;
use crate::expr::column_expr::{ColumnExpr, ColumnReference};

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

    pub fn iter_names_and_types(&self) -> impl Iterator<Item = (&str, &DataType)> + '_ {
        debug_assert_eq!(self.column_types.len(), self.column_names.len());
        self.column_names
            .iter()
            .map(|s| s.as_str())
            .zip(&self.column_types)
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

    pub fn push_table<S>(
        &mut self,
        alias: Option<TableAlias>,
        column_types: impl IntoIterator<Item = DataType>,
        column_names: impl IntoIterator<Item = S>,
    ) -> Result<TableRef>
    where
        S: Into<String>,
    {
        let column_types: Vec<_> = column_types.into_iter().collect();
        let column_names: Vec<_> = column_names.into_iter().map(|s| s.into()).collect();

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

    pub fn column_as_expr(&self, reference: impl Into<ColumnReference>) -> Result<ColumnExpr> {
        let reference = reference.into();
        let datatype = self.get_column_type(reference)?;

        Ok(ColumnExpr {
            reference,
            datatype,
        })
    }

    pub fn get_column(&self, reference: impl Into<ColumnReference>) -> Result<(&str, &DataType)> {
        let reference = reference.into();
        let table = self.get(reference.table_scope)?;
        let name = table
            .column_names
            .get(reference.column)
            .map(|s| s.as_str())
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Missing column {} in table {}",
                    reference.column, reference.table_scope
                ))
            })?;
        let datatype = &table.column_types[reference.column];
        Ok((name, datatype))
    }

    pub fn get_column_type(&self, reference: impl Into<ColumnReference>) -> Result<DataType> {
        let (_, datatype) = self.get_column(reference)?;
        Ok(datatype.clone())
    }
}

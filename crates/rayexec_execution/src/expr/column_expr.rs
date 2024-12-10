use std::fmt;

use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::logical::binder::bind_context::BindContext;
use crate::logical::binder::table_list::TableRef;

/// Reference to a column in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnExpr {
    /// Scope this column is in.
    pub table_scope: TableRef,
    /// Column index within the table.
    pub column: usize,
}

impl ColumnExpr {
    pub fn new(table: impl Into<TableRef>, column: usize) -> Self {
        ColumnExpr {
            table_scope: table.into(),
            column,
        }
    }

    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        let table = bind_context.get_table(self.table_scope)?;
        table
            .column_types
            .get(self.column)
            .cloned()
            .ok_or_else(|| RayexecError::new(format!("Missing column in bind context: {self}")))
    }
}

impl fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table_scope, self.column)
    }
}

impl ContextDisplay for ColumnExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match mode {
            ContextDisplayMode::Enriched(context) => match context.get_table(self.table_scope) {
                Ok(table) if table.num_columns() > self.column => {
                    write!(f, "{}", &table.column_names[self.column])
                }
                _ => write!(f, "<missing! {self}>"),
            },
            ContextDisplayMode::Raw => write!(f, "{self}"),
        }
    }
}

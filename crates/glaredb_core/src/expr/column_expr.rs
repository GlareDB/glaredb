use std::fmt::{self, Debug};

use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::logical::binder::table_list::TableRef;

/// References a column a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnReference {
    /// Scope this column is in.
    pub table_scope: TableRef,
    /// Column index within the table.
    pub column: usize,
}

/// Convert a (table, column) pair to a reference.
impl<T> From<(T, usize)> for ColumnReference
where
    T: Into<TableRef>,
{
    fn from(value: (T, usize)) -> Self {
        ColumnReference {
            table_scope: value.0.into(),
            column: value.1,
        }
    }
}

impl fmt::Display for ColumnReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.table_scope, self.column)
    }
}

impl ContextDisplay for ColumnReference {
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

/// A column reference that knows its datatype.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnExpr {
    pub reference: ColumnReference,
    pub datatype: DataType,
}

impl ColumnExpr {
    pub fn new(column: impl Into<ColumnReference>, datatype: DataType) -> Self {
        ColumnExpr {
            reference: column.into(),
            datatype,
        }
    }
}

impl fmt::Display for ColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.reference, f)
    }
}

impl ContextDisplay for ColumnExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        self.reference.fmt_using_context(mode, f)
    }
}

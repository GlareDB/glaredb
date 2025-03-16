use std::collections::HashSet;
use std::hash::Hash;

use crate::expr::column_expr::ColumnReference;
use crate::expr::Expression;
use crate::logical::binder::table_list::TableRef;

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedFilter {
    /// The filter expression.
    pub filter: Expression,
    /// Table refs this expression references.
    pub table_refs: HashSet<TableRef>,
    /// Columns in the filter.
    pub columns: Vec<ColumnReference>,
}

impl ExtractedFilter {
    pub fn from_expr(expr: Expression) -> Self {
        let table_refs = expr.get_table_references();
        let columns = expr.get_column_references();
        ExtractedFilter {
            filter: expr,
            table_refs,
            columns,
        }
    }

    pub fn into_expression(self) -> Expression {
        self.filter
    }
}

impl Hash for ExtractedFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state)
    }
}

use std::{collections::HashSet, hash::Hash};

use crate::{expr::Expression, logical::binder::bind_context::TableRef};

/// Holds a filtering expression and all table refs the expression references.
#[derive(Debug, PartialEq, Eq)]
pub struct ExtractedFilter {
    /// The filter expression.
    pub filter: Expression,
    /// Tables refs this expression references.
    pub tables_refs: HashSet<TableRef>,
}

impl ExtractedFilter {
    pub fn from_expr(expr: Expression) -> Self {
        fn inner(child: &Expression, refs: &mut HashSet<TableRef>) {
            match child {
                Expression::Column(col) => {
                    refs.insert(col.table_scope);
                }
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, refs);
                        Ok(())
                    })
                    .expect("getting table refs to not fail"),
            }
        }

        let mut refs = HashSet::new();
        inner(&expr, &mut refs);

        ExtractedFilter {
            filter: expr,
            tables_refs: refs,
        }
    }
}

impl Hash for ExtractedFilter {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.filter.hash(state)
    }
}

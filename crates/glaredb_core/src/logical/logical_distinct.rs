use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

/// DISTINCTs all input rows.
///
/// Does not introduce a new table ref.
// TODO: It might introduce a new table ref with ON, not sure if we'd want those
// expressions to be referencable anywhere else.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDistinct {}

impl Explainable for LogicalDistinct {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Distinct")
    }
}

impl LogicalNode for Node<LogicalDistinct> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<F>(&self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, _func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        Ok(())
    }
}

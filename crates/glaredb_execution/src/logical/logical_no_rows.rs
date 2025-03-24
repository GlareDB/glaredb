use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

/// Emit no rows with some number of columns.
///
/// This is used when we detect a filter is always false. Instead of executing
/// the child plans, we replace it with this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalNoRows {
    pub table_refs: Vec<TableRef>,
}

impl LogicalNode for Node<LogicalNoRows> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        self.node.table_refs.clone()
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

impl Explainable for LogicalNoRows {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("NoRows");
        if conf.verbose {
            ent = ent.with_values("table_refs", &self.table_refs);
        }
        ent
    }
}

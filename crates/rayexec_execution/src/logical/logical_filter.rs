use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub filter: Expression,
}

impl Explainable for LogicalFilter {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value_context("predicate", conf, &self.filter)
    }
}

impl LogicalNode for Node<LogicalFilter> {
    fn get_output_table_refs(&self, bind_context: &BindContext) -> Vec<TableRef> {
        self.get_children_table_refs(bind_context)
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        func(&self.node.filter)
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        func(&mut self.node.filter)
    }
}

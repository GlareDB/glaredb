use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

use super::operator::LogicalNode;
use super::{binder::bind_context::TableRef, operator::Node};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalFilter {
    pub filter: Expression,
}

impl Explainable for LogicalFilter {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", &self.filter)
    }
}

impl LogicalNode for Node<LogicalFilter> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}

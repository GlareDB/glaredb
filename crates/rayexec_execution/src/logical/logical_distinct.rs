use crate::{
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    expr::Expression,
};

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDistinct {
    pub on: Vec<Expression>,
}

impl Explainable for LogicalDistinct {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Distinct").with_values("on", &self.on)
    }
}

impl LogicalNode for Node<LogicalDistinct> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}

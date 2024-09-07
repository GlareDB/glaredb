use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalLimit {
    pub offset: Option<usize>,
    pub limit: usize,
}

impl Explainable for LogicalLimit {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Limit").with_value("limit", self.limit);
        if let Some(offset) = self.offset {
            ent = ent.with_value("offset", offset);
        }
        ent
    }
}

impl LogicalNode for Node<LogicalLimit> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.get_children_table_refs()
    }
}

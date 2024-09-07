use rayexec_bullet::field::Schema;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDescribe {
    pub schema: Schema,
    pub table_ref: TableRef,
}

impl Explainable for LogicalDescribe {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Describe")
    }
}

impl LogicalNode for Node<LogicalDescribe> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }
}

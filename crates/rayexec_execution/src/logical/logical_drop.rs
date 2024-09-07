use crate::database::drop::DropInfo;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDrop {
    pub catalog: String,
    pub info: DropInfo,
}

impl Explainable for LogicalDrop {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Drop")
    }
}

impl LogicalNode for Node<LogicalDrop> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        Vec::new()
    }
}

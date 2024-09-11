use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::{
    binder::bind_context::{MaterializationRef, TableRef},
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalMaterializationScan {
    pub mat: MaterializationRef,
    // TODO: I'm not sure if this should always be a single ref.
    pub table_ref: TableRef,
}

impl Explainable for LogicalMaterializationScan {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent =
            ExplainEntry::new("MaterializationScan").with_value("materialization_ref", self.mat);
        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref);
        }
        ent
    }
}

impl LogicalNode for Node<LogicalMaterializationScan> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }
}

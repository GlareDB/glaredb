use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};

use super::{
    binder::bind_context::{MaterializationRef, TableRef},
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalMaterializationScan {
    /// Reference to the materialization in the bind context.
    pub mat: MaterializationRef,
    /// Table references of the output of the materialization.
    ///
    /// These should match the references that are stored on the materialization
    /// in the bind context. They are duplicated here for convenience.
    pub table_refs: Vec<TableRef>,
}

impl Explainable for LogicalMaterializationScan {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent =
            ExplainEntry::new("MaterializationScan").with_value("materialization_ref", self.mat);
        if conf.verbose {
            ent = ent.with_values("table_refs", &self.table_refs)
        }
        ent
    }
}

impl LogicalNode for Node<LogicalMaterializationScan> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        self.node.table_refs.clone()
    }
}

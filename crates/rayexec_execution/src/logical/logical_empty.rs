use rayexec_error::Result;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};
use crate::{
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    expr::Expression,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalEmpty;

impl Explainable for LogicalEmpty {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Empty")
    }
}

impl LogicalNode for Node<LogicalEmpty> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        Vec::new()
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

use rayexec_error::Result;

use crate::{
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    expr::Expression,
};
use std::sync::Arc;

use crate::database::catalog_entry::CatalogEntry;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalInsert {
    pub catalog: String,
    pub schema: String,
    pub table: Arc<CatalogEntry>,
}

impl Explainable for LogicalInsert {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert")
    }
}

impl LogicalNode for Node<LogicalInsert> {
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

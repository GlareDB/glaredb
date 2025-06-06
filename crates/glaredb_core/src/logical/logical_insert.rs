use std::sync::Arc;

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::catalog::entry::CatalogEntry;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone)]
pub struct LogicalInsert {
    pub catalog: String,
    pub schema: String,
    pub table: Arc<CatalogEntry>,
}

impl Explainable for LogicalInsert {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Insert", conf).build()
    }
}

impl LogicalNode for Node<LogicalInsert> {
    fn name(&self) -> &'static str {
        "Insert"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        Vec::new()
    }

    fn for_each_expr<'a, F>(&'a self, _func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, _func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        Ok(())
    }
}

impl PartialEq for LogicalInsert {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.table.name == other.table.name
    }
}

impl Eq for LogicalInsert {}

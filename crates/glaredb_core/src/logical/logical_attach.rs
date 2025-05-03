use std::collections::HashMap;

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::arrays::scalar::ScalarValue;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAttachDatabase {
    pub datasource: String,
    pub name: String,
    pub options: HashMap<String, ScalarValue>,
}

impl Explainable for LogicalAttachDatabase {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("AttachDatabase", conf).build()
    }
}

impl LogicalNode for Node<LogicalAttachDatabase> {
    fn name(&self) -> &'static str {
        "AttachDatabase"
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

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDetachDatabase {
    pub name: String,
}

impl Explainable for LogicalDetachDatabase {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("DetachDatabase", conf).build()
    }
}

impl LogicalNode for Node<LogicalDetachDatabase> {
    fn name(&self) -> &'static str {
        "DetachDatabase"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        Vec::new()
    }

    fn for_each_expr<'a, F>(&self, _func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&mut self, _func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        Ok(())
    }
}

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::arrays::field::Field;
use crate::catalog::create::OnConflict;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalCreateSchema {
    pub catalog: String,
    pub name: String,
    pub on_conflict: OnConflict,
}

impl Explainable for LogicalCreateSchema {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateSchema")
    }
}

impl LogicalNode for Node<LogicalCreateSchema> {
    fn name(&self) -> &'static str {
        "CreateSchema"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalCreateTable {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

impl Explainable for LogicalCreateTable {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable")
    }
}

impl LogicalNode for Node<LogicalCreateTable> {
    fn name(&self) -> &'static str {
        "CreateTable"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalCreateView {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub column_aliases: Option<Vec<String>>,
    pub on_conflict: OnConflict,
    pub query_string: String,
}

impl Explainable for LogicalCreateView {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateView")
    }
}

impl LogicalNode for Node<LogicalCreateView> {
    fn name(&self) -> &'static str {
        "CreateView"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
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

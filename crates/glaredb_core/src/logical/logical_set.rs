use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::arrays::scalar::ScalarValue;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSetVar {
    pub name: String,
    pub value: ScalarValue,
}

impl Explainable for LogicalSetVar {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Set", conf).build()
    }
}

impl LogicalNode for Node<LogicalSetVar> {
    fn name(&self) -> &'static str {
        "SetVar"
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableOrAll {
    Variable(String),
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalResetVar {
    pub var: VariableOrAll,
}

impl Explainable for Node<LogicalResetVar> {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Reset", conf).build()
    }
}

impl LogicalNode for Node<LogicalResetVar> {
    fn name(&self) -> &'static str {
        "ResetVar"
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalShowVar {
    pub name: String,
    pub value: ScalarValue,
}

impl Explainable for LogicalShowVar {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Show", conf).build()
    }
}

impl LogicalNode for Node<LogicalShowVar> {
    fn name(&self) -> &'static str {
        "ShowVar"
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

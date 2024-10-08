use rayexec_error::Result;
use std::collections::HashMap;

use crate::{
    explain::explainable::{ExplainConfig, ExplainEntry, Explainable},
    expr::Expression,
};
use rayexec_bullet::scalar::OwnedScalarValue;

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalAttachDatabase {
    pub datasource: String,
    pub name: String,
    pub options: HashMap<String, OwnedScalarValue>,
}

impl Explainable for LogicalAttachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("AttachDatabase")
    }
}

impl LogicalNode for Node<LogicalAttachDatabase> {
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

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalDetachDatabase {
    pub name: String,
}

impl Explainable for LogicalDetachDatabase {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("DetachDatabase")
    }
}

impl LogicalNode for Node<LogicalDetachDatabase> {
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

use std::fmt;

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Except,
    Intersect,
}

impl fmt::Display for SetOpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Union => write!(f, "UNION"),
            Self::Except => write!(f, "EXCEPT"),
            Self::Intersect => write!(f, "INTERSECT"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalSetop {
    pub kind: SetOpKind,
    pub all: bool,
    pub table_ref: TableRef,
}

impl Explainable for LogicalSetop {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let kind = format!("{}{}", self.kind, if self.all { " ALL" } else { "" });

        EntryBuilder::new("Setop", conf)
            .with_value("kind", kind)
            .with_value_if_verbose("table_ref", self.table_ref)
            .build()
    }
}

impl LogicalNode for Node<LogicalSetop> {
    fn name(&self) -> &'static str {
        "Setop"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.table_ref]
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

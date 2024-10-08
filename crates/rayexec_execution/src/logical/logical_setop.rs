use std::fmt;

use rayexec_error::Result;

use super::binder::bind_context::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
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

        let mut ent = ExplainEntry::new("Setop").with_value("kind", kind);
        if conf.verbose {
            ent = ent.with_value("table_ref", self.table_ref);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalSetop> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.table_ref]
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

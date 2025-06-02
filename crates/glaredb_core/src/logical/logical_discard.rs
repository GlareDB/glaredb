use std::fmt;

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardType {
    All,
    Plans,
    Temp,
    Metadata,
}

impl fmt::Display for DiscardType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "ALL"),
            Self::Plans => write!(f, "PLANS"),
            Self::Temp => write!(f, "TEMP"),
            Self::Metadata => write!(f, "METADATA"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDiscard {
    pub discard_type: DiscardType,
}

impl Explainable for LogicalDiscard {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        EntryBuilder::new("Discard", conf)
            .with_value("type", self.discard_type)
            .build()
    }
}

impl LogicalNode for Node<LogicalDiscard> {
    fn name(&self) -> &'static str {
        "Discard"
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

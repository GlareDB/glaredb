use glaredb_error::Result;
use glaredb_parser::ast;

use super::bind_context::BindScopeRef;
use crate::config::session::SessionConfig;
use crate::logical::logical_discard::{DiscardType, LogicalDiscard};
use crate::logical::operator::{LocationRequirement, Node};
use crate::statistics::value::StatisticsValue;

#[derive(Debug)]
pub struct DiscardBinder<'a> {
    pub current: BindScopeRef,
    pub config: &'a SessionConfig,
}

impl<'a> DiscardBinder<'a> {
    pub fn new(current: BindScopeRef, config: &'a SessionConfig) -> Self {
        DiscardBinder { current, config }
    }

    pub fn bind_discard(&self, discard: ast::DiscardStatement) -> Result<Node<LogicalDiscard>> {
        Ok(Node {
            node: LogicalDiscard {
                discard_type: match discard.discard_type {
                    ast::DiscardType::All => DiscardType::All,
                    ast::DiscardType::Plans => DiscardType::Plans,
                    ast::DiscardType::Temp => DiscardType::Temp,
                    ast::DiscardType::Metadata => DiscardType::Metadata,
                },
            },
            location: LocationRequirement::ClientLocal,
            children: Vec::new(),
            estimated_cardinality: StatisticsValue::Unknown,
        })
    }
}

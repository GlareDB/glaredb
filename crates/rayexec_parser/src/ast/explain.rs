use crate::statement::Statement;

use super::QueryNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainOutput {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainNode {
    pub body: Statement,
    pub output: Option<ExplainOutput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainBody {
    Query(QueryNode),
}

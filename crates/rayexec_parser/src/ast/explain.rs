use crate::{keywords::Keyword, parser::Parser, statement::Statement};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr, QueryNode};

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

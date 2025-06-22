use glaredb_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, FromNode, QueryNode};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Summarize<T: AstMeta> {
    /// SUMMARIZE <query>
    Query(QueryNode<T>),
    /// SUMMARIZE <table>
    /// SUMMARIZE <table-function>
    /// SUMMARIZE <file>
    FromNode(FromNode<T>),
}

impl Summarize<Raw> {
    pub fn is_summarize_start(parser: &mut Parser) -> bool {
        let start = parser.idx;
        let result = parser.next_keyword();
        let is_start = matches!(result, Ok(Keyword::SUMMARIZE));
        parser.idx = start;

        is_start
    }
}

impl AstParseable for Summarize<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::SUMMARIZE)?;

        if QueryNode::is_query_node_start(parser) {
            let query = QueryNode::parse(parser)?;
            Ok(Summarize::Query(query))
        } else {
            let from = FromNode::parse_base_from(parser)?;
            Ok(Summarize::FromNode(from))
        }
    }
}

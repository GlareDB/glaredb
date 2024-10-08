use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, FromNode, QueryNode};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Describe<T: AstMeta> {
    /// DESCRIBE <query>
    Query(QueryNode<T>),
    /// DESCRIBE <table>
    /// DESCRIBE <table-function>
    /// DESCRIBE <file>
    FromNode(FromNode<T>),
}

impl AstParseable for Describe<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DESCRIBE)?;

        if QueryNode::is_query_node_start(parser) {
            let query = QueryNode::parse(parser)?;
            Ok(Describe::Query(query))
        } else {
            let from = FromNode::parse_base_from(parser)?;
            Ok(Describe::FromNode(from))
        }
    }
}

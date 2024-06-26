use crate::meta::{AstMeta, Raw};
use crate::{keywords::Keyword, parser::Parser};
use rayexec_error::Result;

use super::{AstParseable, FromNode, QueryNode};

#[derive(Debug, Clone, PartialEq)]
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

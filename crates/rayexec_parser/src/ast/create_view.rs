use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
};
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateView<T: AstMeta> {
    pub or_replace: bool,
    pub temp: bool,
    pub name: T::ItemReference,
    pub column_aliases: Option<Vec<Ident>>,
    pub query_sql: String,
    pub query: QueryNode<T>,
}

impl AstParseable for CreateView<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::CREATE)?;

        let or_replace = parser.parse_keyword_sequence(&[Keyword::OR, Keyword::REPLACE]);
        let temp = parser
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();

        parser.expect_keyword(Keyword::VIEW)?;

        let name = ObjectReference::parse(parser)?;

        let column_aliases = if parser.parse_keyword(Keyword::AS) {
            // No aliases specified.
            //
            // `alias AS <query>`
            None
        } else {
            // Aliases specified.
            //
            // `alias(c1, c2) AS <query>`
            let column_aliases = parser.parse_parenthesized_comma_separated(Ident::parse)?;
            parser.expect_keyword(Keyword::AS)?;
            Some(column_aliases)
        };

        let query_tok = match parser.peek() {
            Some(tok) => tok.clone(),
            None => {
                return Err(RayexecError::new(
                    "Unexpected end of statement, expect view body",
                ))
            }
        };

        let query = QueryNode::parse(parser)?;

        let query_sql = parser.sql_slice_starting_at(&query_tok)?.to_string();

        Ok(CreateView {
            or_replace,
            temp,
            name,
            column_aliases,
            query_sql,
            query,
        })
    }
}

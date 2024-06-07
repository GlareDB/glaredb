use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::Result;

use super::{AstParseable, Ident, QueryNode};

#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExprDefs<T: AstMeta> {
    pub recursive: bool,
    pub ctes: Vec<T::CteReference>,
}

impl AstParseable for CommonTableExprDefs<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let recursive = parser.parse_keyword(Keyword::RECURSIVE);
        Ok(CommonTableExprDefs {
            recursive,
            ctes: parser.parse_comma_separated(CommonTableExpr::parse)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr<T: AstMeta> {
    pub alias: Ident,
    pub column_aliases: Option<Vec<Ident>>,
    pub materialized: bool,
    pub body: Box<QueryNode<T>>,
}

impl AstParseable for CommonTableExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let alias = Ident::parse(parser)?;

        let column_aliases = if parser.parse_keyword(Keyword::AS) {
            // No aliases specified.
            //
            // `alias AS (<subquery>)`
            None
        } else {
            // Aliases specified.
            //
            // `alias(c1, c2) AS (<subquery>)`
            parser.expect_token(&Token::LeftParen)?;
            let column_aliases = parser.parse_parenthesized_comma_separated(Ident::parse)?;
            parser.expect_token(&Token::RightParen)?;
            parser.expect_keyword(Keyword::AS)?;
            Some(column_aliases)
        };

        let materialized = parser.parse_keyword(Keyword::MATERIALIZED);

        // Parse the subquery.
        parser.expect_token(&Token::LeftParen)?;
        let body = QueryNode::parse(parser)?;
        parser.expect_token(&Token::RightParen)?;

        Ok(CommonTableExpr {
            alias,
            column_aliases,
            materialized,
            body: Box::new(body),
        })
    }
}

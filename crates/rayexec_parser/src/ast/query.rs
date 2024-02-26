use crate::{keywords::Keyword, parser::Parser, tokens::Token};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr, FromAlias, Ident, LimitModifier, OrderByNode, SelectNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryNode<'a> {
    pub ctes: Option<Ctes<'a>>,
    pub body: QueryNodeBody<'a>,
    pub order_by: Option<OrderByNode<'a>>,
    pub limit: LimitModifier<'a>,
}

impl<'a> AstParseable<'a> for QueryNode<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let ctes = if parser.parse_keyword(Keyword::WITH) {
            Some(Ctes::parse(parser)?)
        } else {
            None
        };

        let body = QueryNodeBody::parse(parser)?;

        let order_by = if parser.parse_keyword_sequence(&[Keyword::ORDER, Keyword::BY]) {
            Some(OrderByNode::parse(parser)?)
        } else {
            None
        };

        let limit = LimitModifier::parse(parser)?;

        Ok(QueryNode {
            ctes,
            body,
            order_by,
            limit,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNodeBody<'a> {
    Select(Box<SelectNode<'a>>),
    Set {
        left: Box<QueryNodeBody<'a>>,
        right: Box<QueryNodeBody<'a>>,
        operation: SetOperation,
    },
    Values(Values<'a>),
}

impl<'a> AstParseable<'a> for QueryNodeBody<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        // TODO: Set operations.

        if parser.parse_keyword(Keyword::SELECT) {
            Ok(QueryNodeBody::Select(Box::new(SelectNode::parse(parser)?)))
        } else if parser.parse_keyword(Keyword::VALUES) {
            Ok(QueryNodeBody::Values(Values::parse(parser)?))
        } else {
            return Err(RayexecError::new("Exepected SELECT or VALUES"));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperation {
    Union,
    Except,
    Intersect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Values<'a> {
    pub rows: Vec<Vec<Expr<'a>>>,
}

impl<'a> AstParseable<'a> for Values<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let rows = parser.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LeftParen)?;
            let exprs = parser.parse_comma_separated(Expr::parse)?;
            parser.expect_token(&Token::RightParen)?;
            Ok(exprs)
        })?;

        Ok(Values { rows })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ctes<'a> {
    pub recursive: bool,
    pub ctes: Vec<Cte<'a>>,
}

impl<'a> AstParseable<'a> for Ctes<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let recursive = parser.parse_keyword(Keyword::RECURSIVE);
        Ok(Ctes {
            recursive,
            ctes: parser.parse_comma_separated(Cte::parse)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cte<'a> {
    pub alias: Ident<'a>,
    pub column_aliases: Option<Vec<Ident<'a>>>,
    pub body: Box<QueryNode<'a>>,
}

impl<'a> AstParseable<'a> for Cte<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
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

        // Parse the subquery.
        parser.expect_token(&Token::LeftParen)?;
        let body = QueryNode::parse(parser)?;
        parser.expect_token(&Token::RightParen)?;

        Ok(Cte {
            alias,
            column_aliases,
            body: Box::new(body),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{testutil::parse_ast, Literal};
    use pretty_assertions::assert_eq;

    #[test]
    fn values_one_row() {
        let values: Values = parse_ast("(1, 2)").unwrap();
        let expected = Values {
            rows: vec![vec![
                Expr::Literal(Literal::Number("1")),
                Expr::Literal(Literal::Number("2")),
            ]],
        };
        assert_eq!(expected, values);
    }

    #[test]
    fn values_many_rows() {
        let values: Values = parse_ast("(1, 2), (3, 4), (5, 6)").unwrap();
        let expected = Values {
            rows: vec![
                vec![
                    Expr::Literal(Literal::Number("1")),
                    Expr::Literal(Literal::Number("2")),
                ],
                vec![
                    Expr::Literal(Literal::Number("3")),
                    Expr::Literal(Literal::Number("4")),
                ],
                vec![
                    Expr::Literal(Literal::Number("5")),
                    Expr::Literal(Literal::Number("6")),
                ],
            ],
        };
        assert_eq!(expected, values);
    }
}

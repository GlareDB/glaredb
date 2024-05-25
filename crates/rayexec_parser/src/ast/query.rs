use crate::{keywords::Keyword, parser::Parser, tokens::Token};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr, Ident, LimitModifier, OrderByNode, SelectNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryNode {
    pub ctes: Option<Ctes>,
    pub body: QueryNodeBody,
    pub order_by: Vec<OrderByNode>,
    pub limit: LimitModifier,
}

impl AstParseable for QueryNode {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let ctes = if parser.parse_keyword(Keyword::WITH) {
            Some(Ctes::parse(parser)?)
        } else {
            None
        };

        let body = QueryNodeBody::parse(parser)?;

        let order_by = if parser.parse_keyword_sequence(&[Keyword::ORDER, Keyword::BY]) {
            parser.parse_comma_separated(OrderByNode::parse)?
        } else {
            Vec::new()
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
pub enum QueryNodeBody {
    Select(Box<SelectNode>),
    Set {
        left: Box<QueryNodeBody>,
        right: Box<QueryNodeBody>,
        operation: SetOperation,
    },
    Values(Values),
}

impl AstParseable for QueryNodeBody {
    fn parse(parser: &mut Parser) -> Result<Self> {
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
pub struct Values {
    pub rows: Vec<Vec<Expr>>,
}

impl AstParseable for Values {
    fn parse(parser: &mut Parser) -> Result<Self> {
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
pub struct Ctes {
    pub recursive: bool,
    pub ctes: Vec<Cte>,
}

impl AstParseable for Ctes {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let recursive = parser.parse_keyword(Keyword::RECURSIVE);
        Ok(Ctes {
            recursive,
            ctes: parser.parse_comma_separated(Cte::parse)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cte {
    pub alias: Ident,
    pub column_aliases: Option<Vec<Ident>>,
    pub body: Box<QueryNode>,
}

impl AstParseable for Cte {
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
                Expr::Literal(Literal::Number("1".to_string())),
                Expr::Literal(Literal::Number("2".to_string())),
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
                    Expr::Literal(Literal::Number("1".to_string())),
                    Expr::Literal(Literal::Number("2".to_string())),
                ],
                vec![
                    Expr::Literal(Literal::Number("3".to_string())),
                    Expr::Literal(Literal::Number("4".to_string())),
                ],
                vec![
                    Expr::Literal(Literal::Number("5".to_string())),
                    Expr::Literal(Literal::Number("6".to_string())),
                ],
            ],
        };
        assert_eq!(expected, values);
    }
}

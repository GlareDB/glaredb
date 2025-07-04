use glaredb_error::{DbError, Result};
use serde::{Deserialize, Serialize};

use super::{
    AstParseable,
    CommonTableExprs,
    Describe,
    Expr,
    LimitModifier,
    OrderByModifier,
    OrderByNode,
    SelectNode,
};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;
use crate::tokens::Token;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryNode<T: AstMeta> {
    pub ctes: Option<CommonTableExprs<T>>,
    pub body: QueryNodeBody<T>,
    pub order_by: Option<OrderByModifier<T>>,
    pub limit: LimitModifier<T>,
}

impl AstParseable for QueryNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let ctes = if parser.parse_keyword(Keyword::WITH) {
            Some(CommonTableExprs::parse(parser)?)
        } else {
            None
        };

        let body = QueryNodeBody::parse(parser)?;

        let order_by = if parser.parse_keyword_sequence(&[Keyword::ORDER, Keyword::BY]) {
            Some(OrderByModifier {
                order_by_nodes: parser.parse_comma_separated(OrderByNode::parse)?,
            })
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

impl QueryNode<Raw> {
    /// Returns if the parser is at the start of a query node by checking
    /// keywords.
    ///
    /// The parser's state is reset before this function returns.
    pub fn is_query_node_start(parser: &mut Parser) -> bool {
        let start = parser.idx;
        let result = parser.next_keyword();
        let is_start = matches!(
            result,
            Ok(Keyword::SELECT) | Ok(Keyword::WITH) | Ok(Keyword::VALUES)
        );
        parser.idx = start;

        is_start
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryNodeBody<T: AstMeta> {
    Select(Box<SelectNode<T>>),
    Nested(Box<QueryNode<T>>),
    Set(SetOp<T>),
    Values(Values<T>),
    Describe(Box<Describe<T>>),
}

impl AstParseable for QueryNodeBody<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Self::parse_inner(parser, 0)
    }
}

impl QueryNodeBody<Raw> {
    fn parse_inner(parser: &mut Parser, precedence: u8) -> Result<Self> {
        let mut body = if parser.parse_keyword(Keyword::SELECT) {
            QueryNodeBody::Select(Box::new(SelectNode::parse(parser)?))
        } else if parser.parse_keyword(Keyword::VALUES) {
            QueryNodeBody::Values(Values::parse(parser)?)
        } else if parser.parse_keyword(Keyword::DESCRIBE) {
            QueryNodeBody::Describe(Box::new(Describe::parse(parser)?))
        } else if parser.consume_token(&Token::LeftParen) {
            let nested = QueryNode::parse(parser)?;
            parser.expect_token(&Token::RightParen)?;
            QueryNodeBody::Nested(Box::new(nested))
        } else {
            return Err(DbError::new("Expected SELECT or VALUES"));
        };

        // Parse set operation(s)
        while let Some(tok) = parser.peek() {
            let (op, next_precedence) = match tok.keyword() {
                Some(Keyword::UNION) => (SetOperation::Union, 10),
                Some(Keyword::EXCEPT) => (SetOperation::Except, 10),
                Some(Keyword::INTERSECT) => (SetOperation::Intersect, 20),
                _ => break,
            };

            if precedence >= next_precedence {
                break;
            }

            let _ = parser.next();
            let all = parser.parse_keyword(Keyword::ALL);

            body = QueryNodeBody::Set(SetOp {
                left: Box::new(body),
                right: Box::new(Self::parse_inner(parser, next_precedence)?),
                operation: op,
                all,
            });
        }

        Ok(body)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetOp<T: AstMeta> {
    pub left: Box<QueryNodeBody<T>>,
    pub right: Box<QueryNodeBody<T>>,
    pub operation: SetOperation,
    pub all: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperation {
    Union,
    Except,
    Intersect,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Values<T: AstMeta> {
    pub rows: Vec<Vec<Expr<T>>>,
}

impl AstParseable for Values<Raw> {
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

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ast::Literal;
    use crate::ast::testutil::parse_ast;

    #[test]
    fn values_one_row() {
        let values: Values<_> = parse_ast("(1, 2)").unwrap();
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
        let values: Values<_> = parse_ast("(1, 2), (3, 4), (5, 6)").unwrap();
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

use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, CommonTableExprDefs, Expr, LimitModifier, OrderByNode, SelectNode};

#[derive(Debug, Clone, PartialEq)]
pub struct QueryNode<T: AstMeta> {
    pub ctes: Option<CommonTableExprDefs<T>>,
    pub body: QueryNodeBody<T>,
    pub order_by: Vec<OrderByNode<T>>,
    pub limit: LimitModifier<T>,
}

impl AstParseable for QueryNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let ctes = if parser.parse_keyword(Keyword::WITH) {
            Some(CommonTableExprDefs::parse(parser)?)
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

#[derive(Debug, Clone, PartialEq)]
pub enum QueryNodeBody<T: AstMeta> {
    Select(Box<SelectNode<T>>),
    Set {
        left: Box<QueryNodeBody<T>>,
        right: Box<QueryNodeBody<T>>,
        operation: SetOperation,
    },
    Values(Values<T>),
}

impl AstParseable for QueryNodeBody<Raw> {
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

#[derive(Debug, Clone, PartialEq)]
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
    use super::*;
    use crate::ast::{testutil::parse_ast, Literal};
    use pretty_assertions::assert_eq;

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

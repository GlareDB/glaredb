use crate::{keywords::Keyword, parser::Parser};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr};

/// A single node in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByNode<'a> {
    pub typ: Option<OrderByType>,
    pub nulls: Option<OrderByNulls>,
    pub expr: Expr<'a>,
}

impl<'a> AstParseable<'a> for OrderByNode<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let expr = Expr::parse(parser)?;

        let typ = if parser.parse_keyword(Keyword::ASC) {
            Some(OrderByType::Asc)
        } else if parser.parse_keyword(Keyword::DESC) {
            Some(OrderByType::Desc)
        } else {
            None
        };

        let nulls = if parser.parse_keyword_sequence(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(OrderByNulls::First)
        } else if parser.parse_keyword_sequence(&[Keyword::NULLS, Keyword::LAST]) {
            Some(OrderByNulls::Last)
        } else {
            None
        };

        Ok(OrderByNode { typ, nulls, expr })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByType {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByNulls {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitModifier<'a> {
    pub limit: Option<Expr<'a>>,
    pub offset: Option<Expr<'a>>,
}

impl<'a> AstParseable<'a> for LimitModifier<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let mut limit = None;
        let mut offset = None;

        if parser.parse_keyword(Keyword::LIMIT) {
            limit = Some(Expr::parse(parser)?)
        }

        if parser.parse_keyword(Keyword::OFFSET) {
            offset = Some(Expr::parse(parser)?)
        }

        // Try limit again since LIMIT and OFFSET can be specified in any order.
        if parser.parse_keyword(Keyword::LIMIT) {
            limit = Some(Expr::parse(parser)?)
        }

        Ok(LimitModifier { limit, offset })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistinctModifier<'a> {
    On(Vec<Expr<'a>>),
    All,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::testutil::parse_ast;
    use insta::assert_debug_snapshot;

    #[test]
    fn order_by_node() {
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1").unwrap(), @r###"
        OrderByNode {
            typ: None,
            nulls: None,
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1 desc").unwrap(), @r###"
        OrderByNode {
            typ: Some(
                Desc,
            ),
            nulls: None,
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1 asc").unwrap(), @r###"
        OrderByNode {
            typ: Some(
                Asc,
            ),
            nulls: None,
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1 nulls last").unwrap(), @r###"
        OrderByNode {
            typ: None,
            nulls: Some(
                Last,
            ),
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1 nulls first").unwrap(), @r###"
        OrderByNode {
            typ: None,
            nulls: Some(
                First,
            ),
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
        assert_debug_snapshot!(parse_ast::<OrderByNode>("col1 desc nulls last").unwrap(), @r###"
        OrderByNode {
            typ: Some(
                Desc,
            ),
            nulls: Some(
                Last,
            ),
            expr: Ident(
                Ident {
                    value: "col1",
                },
            ),
        }
        "###);
    }
}

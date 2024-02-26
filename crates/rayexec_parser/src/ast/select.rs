use crate::{
    keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, DistinctModifier, Expr, FromNode, Ident, ObjectReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectNode<'a> {
    /// DISTINCT [ON]
    pub distinct: Option<DistinctModifier<'a>>,
    /// Projection list. May included wildcards.
    pub projections: Vec<SelectExpr<'a>>,
    /// FROM
    pub from: Option<FromNode<'a>>,
    /// WHERE
    pub where_expr: Option<Expr<'a>>,
    /// GROUP BY
    pub group_by: Option<GroupByNode<'a>>,
    /// HAVING
    pub having: Option<Expr<'a>>,
}

impl<'a> AstParseable<'a> for SelectNode<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        // TODO: distinct

        // Select list
        let projections = parser.parse_comma_separated(SelectExpr::parse)?;

        // FROM
        let from = if parser.parse_keyword(Keyword::FROM) {
            Some(FromNode::parse(parser)?)
        } else {
            None
        };

        // WHERE
        let where_expr = if parser.parse_keyword(Keyword::WHERE) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if parser.parse_keyword_sequence(&[Keyword::GROUP, Keyword::BY]) {
            Some(GroupByNode::parse(parser)?)
        } else {
            None
        };

        // HAVING
        let having = if parser.parse_keyword(Keyword::HAVING) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        Ok(SelectNode {
            distinct: None,
            projections,
            from,
            where_expr,
            group_by,
            having,
        })
    }
}

impl<'a> SelectNode<'a> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectExpr<'a> {
    /// An unaliases expression.
    Expr(Expr<'a>),
    /// An aliased expression.
    ///
    /// `<expr> AS <ident>`
    AliasedExpr(Expr<'a>, Ident<'a>),
    /// A qualified wild card.
    ///
    /// `<reference>.*`
    QualifiedWildcard(ObjectReference<'a>, Wildcard<'a>),
    /// An unqualifed wild card.
    Wildcard(Wildcard<'a>),
}

impl<'a> AstParseable<'a> for SelectExpr<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let expr = WildcardExpr::parse(parser)?;

        match expr {
            WildcardExpr::Wildcard => {
                // TODO: Replace, exclude
                Ok(SelectExpr::Wildcard(Wildcard::default()))
            }
            WildcardExpr::QualifiedWildcard(name) => {
                // TODO: Replace, exclude
                Ok(SelectExpr::QualifiedWildcard(name, Wildcard::default()))
            }
            WildcardExpr::Expr(expr) => {
                let alias = parser.parse_alias(RESERVED_FOR_COLUMN_ALIAS)?;
                match alias {
                    Some(alias) => Ok(SelectExpr::AliasedExpr(expr, alias)),
                    None => Ok(SelectExpr::Expr(expr)),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Wildcard<'a> {
    /// Columns to exclude in the star select.
    ///
    /// `SELECT * EXCLUDE col1, col2 ...`
    pub exclude_cols: Vec<Ident<'a>>,
    /// Columns to replace in the star select.
    ///
    /// `SELECT * REPLACE (col1 / 100 AS col1) ...`
    pub replace_cols: Vec<ReplaceColumn<'a>>,
    // TODO: `SELECT COLUMNS(...)`
}

/// A wildcard, qualified wildcard, or an expression.
///
/// Parsed from the select list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WildcardExpr<'a> {
    Wildcard,
    QualifiedWildcard(ObjectReference<'a>),
    Expr(Expr<'a>),
}

impl<'a> AstParseable<'a> for WildcardExpr<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let idx = parser.idx; // Needed for resetting the position if this is just an expression.

        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(RayexecError::new(
                    "Expected wild card expression, found end of statement",
                ))
            }
        };

        // `*`
        if matches!(tok, Token::Mul) {
            return Ok(WildcardExpr::Wildcard);
        }

        // Possibly qualified wildcard.
        //
        // `table.*` or `'table'.*`
        if matches!(tok, Token::Word(_) | Token::SingleQuotedString(_)) {
            let ident = match tok {
                Token::Word(w) => Ident {
                    value: w.value.into(),
                },
                Token::SingleQuotedString(s) => Ident { value: (*s).into() },
                _ => unreachable!("token variants previously matched on"),
            };

            if parser.peek().is_some_and(|tok| tok.token == Token::Period) {
                let mut idents = vec![ident];

                while parser.consume_token(&Token::Period) {
                    let next =
                        match parser.next() {
                            Some(tok) => &tok.token,
                            None => return Err(RayexecError::new(
                                "Expected an identifier or '*' after '.', found end of statement",
                            )),
                        };

                    match next {
                        Token::Word(w) => idents.push(Ident {
                            value: w.value.into(),
                        }),
                        Token::SingleQuotedString(s) => idents.push(Ident { value: (*s).into() }),
                        Token::Mul => {
                            return Ok(WildcardExpr::QualifiedWildcard(ObjectReference(idents)))
                        }
                        other => {
                            return Err(RayexecError::new(format!(
                                "Expected an identifier or '*' after '.', found {other:?}"
                            )))
                        }
                    }
                }
            }
        }

        // None of the above. Parse as an expression.
        parser.idx = idx;
        let expr = Expr::parse(parser)?;
        Ok(WildcardExpr::Expr(expr))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceColumn<'a> {
    pub col: Ident<'a>,
    pub expr: Expr<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByNode<'a> {
    All,
    Exprs { exprs: Vec<GroupByExpr<'a>> },
}

impl<'a> AstParseable<'a> for GroupByNode<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        if parser.parse_keyword(Keyword::ALL) {
            Ok(GroupByNode::All)
        } else {
            let exprs = parser.parse_comma_separated(GroupByExpr::parse)?;
            Ok(GroupByNode::Exprs { exprs })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByExpr<'a> {
    /// `GROUP BY <expr>[, ...]`
    Expr(Expr<'a>),
    /// `GROUP BY CUBE (<expr>)`
    Cube(Vec<Expr<'a>>),
    /// `GROUP BY ROLLUP (<expr>)`
    Rollup(Vec<Expr<'a>>),
    /// `GROUP BY GROUPING SETS (<expr>)`
    GroupingSets(Vec<Expr<'a>>),
}

impl<'a> AstParseable<'a> for GroupByExpr<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let tok = match parser.peek() {
            Some(tok) => tok,
            None => {
                return Err(RayexecError::new(
                    "Expected expression for GROUP BY, found end of statement",
                ))
            }
        };

        if let Some(kw) = tok.keyword() {
            match kw {
                Keyword::CUBE => {
                    parser.next();
                    let exprs = parser.parse_parenthesized_comma_separated(Expr::parse)?;
                    return Ok(GroupByExpr::Cube(exprs));
                }
                Keyword::ROLLUP => {
                    parser.next();
                    let exprs = parser.parse_parenthesized_comma_separated(Expr::parse)?;
                    return Ok(GroupByExpr::Rollup(exprs));
                }
                Keyword::GROUPING => {
                    parser.next();
                    parser.expect_keyword(Keyword::SETS)?;
                    let exprs = parser.parse_parenthesized_comma_separated(Expr::parse)?;
                    return Ok(GroupByExpr::GroupingSets(exprs));
                }
                _ => (), // Fallthrough, need to parse as an expression.
            }
        }

        let expr = Expr::parse(parser)?;
        Ok(GroupByExpr::Expr(expr))
    }
}

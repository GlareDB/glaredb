use crate::{
    keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, DistinctModifier, Expr, FromNode, Ident, ObjectReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectNode {
    /// DISTINCT [ON]
    pub distinct: Option<DistinctModifier>,
    /// Projection list. May included wildcards.
    pub projections: Vec<SelectExpr>,
    /// FROM
    pub from: Option<FromNode>,
    /// WHERE
    pub where_expr: Option<Expr>,
    /// GROUP BY
    pub group_by: Option<GroupByNode>,
    /// HAVING
    pub having: Option<Expr>,
}

impl AstParseable for SelectNode {
    fn parse(parser: &mut Parser) -> Result<Self> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectExpr {
    /// An unaliases expression.
    Expr(Expr),
    /// An aliased expression.
    ///
    /// `<expr> AS <ident>`
    AliasedExpr(Expr, Ident),
    /// A qualified wild card.
    ///
    /// `<reference>.*`
    QualifiedWildcard(ObjectReference, Wildcard),
    /// An unqualifed wild card.
    Wildcard(Wildcard),
}

impl AstParseable for SelectExpr {
    fn parse(parser: &mut Parser) -> Result<Self> {
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
pub struct Wildcard {
    /// Columns to exclude in the star select.
    ///
    /// `SELECT * EXCLUDE col1, col2 ...`
    pub exclude_cols: Vec<Ident>,
    /// Columns to replace in the star select.
    ///
    /// `SELECT * REPLACE (col1 / 100 AS col1) ...`
    pub replace_cols: Vec<ReplaceColumn>,
    // TODO: `SELECT COLUMNS(...)`
}

/// A wildcard, qualified wildcard, or an expression.
///
/// Parsed from the select list.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WildcardExpr {
    Wildcard,
    QualifiedWildcard(ObjectReference),
    Expr(Expr),
}

impl AstParseable for WildcardExpr {
    fn parse(parser: &mut Parser) -> Result<Self> {
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
                    value: w.value.clone(),
                },
                Token::SingleQuotedString(s) => Ident { value: s.clone() },
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
                            value: w.value.clone(),
                        }),
                        Token::SingleQuotedString(s) => idents.push(Ident { value: s.clone() }),
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
pub struct ReplaceColumn {
    pub col: Ident,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByNode {
    All,
    Exprs { exprs: Vec<GroupByExpr> },
}

impl AstParseable for GroupByNode {
    fn parse(parser: &mut Parser) -> Result<Self> {
        if parser.parse_keyword(Keyword::ALL) {
            Ok(GroupByNode::All)
        } else {
            let exprs = parser.parse_comma_separated(GroupByExpr::parse)?;
            Ok(GroupByNode::Exprs { exprs })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByExpr {
    /// `GROUP BY <expr>[, ...]`
    Expr(Expr),
    /// `GROUP BY CUBE (<expr>)`
    Cube(Vec<Expr>),
    /// `GROUP BY ROLLUP (<expr>)`
    Rollup(Vec<Expr>),
    /// `GROUP BY GROUPING SETS (<expr>)`
    GroupingSets(Vec<Expr>),
}

impl AstParseable for GroupByExpr {
    fn parse(parser: &mut Parser) -> Result<Self> {
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

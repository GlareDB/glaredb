use glaredb_error::{DbError, Result, not_implemented};
use serde::{Deserialize, Serialize};

use super::{AstParseable, DistinctModifier, Expr, FromNode, Ident, ObjectReference};
use crate::keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS};
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;
use crate::tokens::Token;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectNode<T: AstMeta> {
    /// DISTINCT [ON]
    pub distinct: Option<DistinctModifier<T>>,
    /// Projection list. May included wildcards.
    pub projections: Vec<SelectExpr<T>>,
    /// FROM
    pub from: Option<FromNode<T>>,
    /// WHERE
    pub where_expr: Option<Expr<T>>,
    /// GROUP BY
    pub group_by: Option<GroupByNode<T>>,
    /// HAVING
    pub having: Option<Expr<T>>,
}

impl AstParseable for SelectNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let mut distinct = None;
        if parser.parse_keyword(Keyword::ALL) {
            distinct = Some(DistinctModifier::All);
        }

        if parser.parse_keyword(Keyword::DISTINCT) {
            if distinct.is_some() {
                return Err(DbError::new("Cannot specify both ALL and DISTINCT"));
            }

            if parser.parse_keyword(Keyword::ON) {
                //  SELECT DISTINCT ON (<expr>) <select-list>
                not_implemented!("DISTINCT ON")
            } else {
                //  SELECT DISTINCT <select-list>
                distinct = Some(DistinctModifier::Distinct)
            }
        }

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
            distinct,
            projections,
            from,
            where_expr,
            group_by,
            having,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectExpr<T: AstMeta> {
    /// An unaliases expression.
    Expr(Expr<T>),
    /// An aliased expression.
    ///
    /// `<expr> AS <ident>`
    AliasedExpr(Expr<T>, Ident),
    /// A qualified wild card.
    ///
    /// `<reference>.*`
    QualifiedWildcard(ObjectReference, WildcardModifier<T>),
    /// An unqualifed wild card.
    Wildcard(WildcardModifier<T>),
}

impl<T: AstMeta> SelectExpr<T> {
    pub fn get_alias(&self) -> Option<&Ident> {
        match self {
            Self::AliasedExpr(_, alias) => Some(alias),
            _ => None,
        }
    }
}

impl AstParseable for SelectExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let expr = WildcardExpr::parse(parser)?;

        match expr {
            WildcardExpr::Wildcard => {
                let modifier = WildcardModifier::parse(parser)?;
                Ok(SelectExpr::Wildcard(modifier))
            }
            WildcardExpr::QualifiedWildcard(name) => {
                let modifier = WildcardModifier::parse(parser)?;
                Ok(SelectExpr::QualifiedWildcard(name, modifier))
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WildcardModifier<T: AstMeta> {
    /// Columns to exclude in the star select.
    ///
    /// `SELECT * EXCLUDE (col1, col2 ...)`
    pub exclude_cols: Vec<Ident>,
    /// Columns to replace in the star select.
    ///
    /// `SELECT * REPLACE (col1 / 100 AS col1) ...`
    pub replace_cols: Vec<ReplaceColumn<T>>,
}

impl AstParseable for WildcardModifier<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let exclude_cols = match parser.parse_one_of_keywords(&[Keyword::EXCEPT, Keyword::EXCLUDE])
        {
            Some(Keyword::EXCEPT) | Some(Keyword::EXCLUDE) => {
                parser.parse_parenthesized_comma_separated(Ident::parse)?
            }
            _ => Vec::new(),
        };

        let replace_cols = if parser.parse_keyword(Keyword::REPLACE) {
            parser.parse_parenthesized_comma_separated(ReplaceColumn::parse)?
        } else {
            Vec::new()
        };

        Ok(WildcardModifier {
            exclude_cols,
            replace_cols,
        })
    }
}

/// A wildcard, qualified wildcard, or an expression.
///
/// Parsed from the select list.
#[derive(Debug, Clone, PartialEq)]
pub enum WildcardExpr<T: AstMeta> {
    Wildcard,
    QualifiedWildcard(ObjectReference),
    Expr(Expr<T>),
}

impl AstParseable for WildcardExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let idx = parser.idx; // Needed for resetting the position if this is just an expression.

        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(DbError::new(
                    "Expected wild card expression, found end of statement",
                ));
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
                Token::Word(w) => w.clone().into(),
                Token::SingleQuotedString(s) => Ident {
                    value: s.clone(),
                    quoted: false,
                },
                _ => unreachable!("token variants previously matched on"),
            };

            if parser.peek().is_some_and(|tok| tok.token == Token::Period) {
                let mut idents = vec![ident];

                while parser.consume_token(&Token::Period) {
                    let next = match parser.next() {
                        Some(tok) => &tok.token,
                        None => {
                            return Err(DbError::new(
                                "Expected an identifier or '*' after '.', found end of statement",
                            ));
                        }
                    };

                    match next {
                        Token::Word(w) => idents.push(w.clone().into()),
                        Token::SingleQuotedString(s) => idents.push(Ident {
                            value: s.clone(),
                            quoted: false,
                        }),
                        Token::Mul => {
                            return Ok(WildcardExpr::QualifiedWildcard(ObjectReference(idents)));
                        }
                        other => {
                            return Err(DbError::new(format!(
                                "Expected an identifier or '*' after '.', found {other:?}"
                            )));
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceColumn<T: AstMeta> {
    pub col: Ident,
    pub expr: Expr<T>,
}

impl AstParseable for ReplaceColumn<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let expr = Expr::parse(parser)?;
        let alias = match parser.parse_alias(RESERVED_FOR_COLUMN_ALIAS)? {
            Some(alias) => alias,
            None => return Err(DbError::new("REPLACE requires column name")),
        };

        Ok(ReplaceColumn { col: alias, expr })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupByNode<T: AstMeta> {
    All,
    Exprs { exprs: Vec<GroupByExpr<T>> },
}

impl AstParseable for GroupByNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        if parser.parse_keyword(Keyword::ALL) {
            Ok(GroupByNode::All)
        } else {
            let exprs = parser.parse_comma_separated(GroupByExpr::parse)?;
            Ok(GroupByNode::Exprs { exprs })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupByExpr<T: AstMeta> {
    /// `GROUP BY <expr>[, ...]`
    Expr(Vec<Expr<T>>),
    /// `GROUP BY CUBE (<expr>)`
    Cube(Vec<Expr<T>>),
    /// `GROUP BY ROLLUP (<expr>)`
    Rollup(Vec<Expr<T>>),
    /// `GROUP BY GROUPING SETS (<expr>)`
    GroupingSets(Vec<Expr<T>>), // TODO: vec vec
}

impl AstParseable for GroupByExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let tok = match parser.peek() {
            Some(tok) => tok,
            None => {
                return Err(DbError::new(
                    "Expected expression for GROUP BY, found end of statement",
                ));
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

        let exprs = parser.parse_comma_separated(Expr::parse)?;
        Ok(GroupByExpr::Expr(exprs))
    }
}

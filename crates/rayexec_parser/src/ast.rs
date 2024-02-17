use crate::keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS};
use crate::parser::Parser;
use crate::tokens::Token;
use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::hash::Hash;

pub trait AstParseable<'a>: Sized {
    /// Parse an instance of Self from the provided parser.
    ///
    /// It's assumed that the parser is in the correct state for parsing Self,
    /// and if it isn't, an error should be returned.
    fn parse(parser: &mut Parser<'a>) -> Result<Self>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident<'a> {
    pub value: &'a str,
}

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectReference<'a>(pub Vec<Ident<'a>>);

impl<'a> fmt::Display for ObjectReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let strings: Vec<_> = self.0.iter().map(|ident| ident.value.to_string()).collect();
        write!(f, "{}", strings.join("."))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal<'a> {
    /// Unparsed number literal.
    Number(&'a str),
    /// String literal.
    SingleQuotedString(&'a str),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal
    Null,
    /// Struct literal.
    ///
    /// Lengths of keys and values must be the same.
    Struct {
        keys: Vec<&'a str>,
        values: Vec<Expr<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr<'a> {
    /// Column or table identifier.
    Ident(Ident<'a>),
    /// Compound identifier.
    ///
    /// `table.col`
    CompoundIdent(Vec<Ident<'a>>),
    /// An expression literal,
    Literal(Literal<'a>),
}

impl<'a> AstParseable<'a> for Expr<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let expr = Expr::parse_prefix(parser)?;

        // TODO: Infix

        Ok(expr)
    }
}

impl<'a> Expr<'a> {
    fn parse_prefix(parser: &mut Parser<'a>) -> Result<Self> {
        // TODO: Typed string

        let tok = match parser.next() {
            Some(tok) => tok,
            None => {
                return Err(RayexecError::new(
                    "Expected prefix expression, found end of statement",
                ))
            }
        };

        let expr = match &tok.token {
            Token::Word(w) => match w.keyword {
                Some(kw) => match kw {
                    Keyword::TRUE => Expr::Literal(Literal::Boolean(true)),
                    Keyword::FALSE => Expr::Literal(Literal::Boolean(false)),
                    Keyword::NULL => Expr::Literal(Literal::Null),
                    _ => unimplemented!(),
                },
                None => {
                    // TODO: Extend, compound idents.
                    Expr::Ident(Ident { value: w.value })
                }
            },
            Token::SingleQuotedString(s) => Expr::Literal(Literal::SingleQuotedString(s)),
            Token::Number(s) => Expr::Literal(Literal::Number(s)),
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected token '{other:?}'. Expected expression."
                )))
            }
        };

        Ok(expr)
    }
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
                Token::Word(w) => Ident { value: w.value },
                Token::SingleQuotedString(s) => Ident { value: s },
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
                        Token::Word(w) => idents.push(Ident { value: w.value }),
                        Token::SingleQuotedString(s) => idents.push(Ident { value: s }),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT(true)`
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    /// Plus, e.g. `a + b`
    Plus,
    /// Minus, e.g. `a - b`
    Minus,
    /// Multiply, e.g. `a * b`
    Multiply,
    /// Divide, e.g. `a / b`
    Divide,
    /// Integer division, e.g. `a // b`
    IntDiv,
    /// Modulo, e.g. `a % b`
    Modulo,
    /// String/Array Concat operator, e.g. `a || b`
    StringConcat,
    /// Greater than, e.g. `a > b`
    Gt,
    /// Less than, e.g. `a < b`
    Lt,
    /// Greater equal, e.g. `a >= b`
    GtEq,
    /// Less equal, e.g. `a <= b`
    LtEq,
    /// Spaceship, e.g. `a <=> b`
    Spaceship,
    /// Equal, e.g. `a = b`
    Eq,
    /// Not equal, e.g. `a <> b`
    NotEq,
    /// And, e.g. `a AND b`
    And,
    /// Or, e.g. `a OR b`
    Or,
    /// XOR, e.g. `a XOR b`
    Xor,
    /// Bitwise or, e.g. `a | b`
    BitwiseOr,
    /// Bitwise and, e.g. `a & b`
    BitwiseAnd,
    /// Bitwise XOR, e.g. `a ^ b`
    BitwiseXor,
}

/// A full SELECT statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectNode<'a> {
    /// `SELECT [DISTINCT ON (<expr>) | ALL] <expr>, ...,`
    pub modifier: SelectModifer<'a>,
    /// Items being selected.
    pub projections: SelectList<'a>,
    /// A FROM clause including joins.
    ///
    /// `FROM <table|function|subquery> [, | <join> <select-node>]
    pub from: Vec<FromItem<'a>>,
    /// Group by expression.
    pub group_by: Option<GroupBy<'a>>,
    /// Having expression.
    ///
    /// May exist even if group by isn't provided.
    pub having: Option<Expr<'a>>,
    /// Order by expression.
    pub order_by: Option<OrderBy<'a>>,
    pub limit: Option<Expr<'a>>,
    pub offset: Option<Expr<'a>>,
    // TODO: Window
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SelectModifer<'a> {
    /// No modifier specified.
    #[default]
    None,
    /// `ALL`
    All,
    /// `DISTINCT`
    Distinct,
    /// `DISTINCT ON (<expr>)`
    DistinctOn(Vec<Expr<'a>>),
}

impl<'a> AstParseable<'a> for SelectModifer<'a> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let all = parser.parse_keyword(Keyword::ALL);
        let distinct = parser.parse_keyword(Keyword::DISTINCT);
        if all && distinct {
            return Err(RayexecError::new("Cannot specifiy both ALL and DISTINCT"));
        }

        if !all && !distinct {
            return Ok(SelectModifer::None);
        }

        if all {
            return Ok(SelectModifer::All);
        }

        if !parser.parse_keyword(Keyword::ON) {
            return Ok(SelectModifer::Distinct);
        }

        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectList<'a>(pub Vec<SelectItem<'a>>);

impl<'a> AstParseable<'a> for SelectList<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let items = parser.parse_comma_separated(SelectItem::parse)?;
        Ok(SelectList(items))
    }
}

/// An item in a SELECT clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectItem<'a> {
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

impl<'a> AstParseable<'a> for SelectItem<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let expr = WildcardExpr::parse(parser)?;

        match expr {
            WildcardExpr::Wildcard => {
                // TODO: Replace, exclude
                Ok(SelectItem::Wildcard(Wildcard::default()))
            }
            WildcardExpr::QualifiedWildcard(name) => {
                // TODO: Replace, exclude
                Ok(SelectItem::QualifiedWildcard(name, Wildcard::default()))
            }
            WildcardExpr::Expr(expr) => {
                let alias = parser.parse_alias(RESERVED_FOR_COLUMN_ALIAS)?;
                match alias {
                    Some(alias) => Ok(SelectItem::AliasedExpr(expr, alias)),
                    None => Ok(SelectItem::Expr(expr)),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceColumn<'a> {
    pub col: Ident<'a>,
    pub expr: Expr<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromItem<'a> {
    pub join: Option<JoinOperation<'a>>,
    pub table: TableLike<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOperation<'a> {
    pub join_modifier: Option<JoinModifier>,
    pub join_type: JoinType<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinModifier {
    Natural,
    AsOf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType<'a> {
    Inner(Option<JoinConstraint<'a>>),
    LeftOuter(Option<JoinConstraint<'a>>),
    RightOuter(Option<JoinConstraint<'a>>),
    FullOuter(Option<JoinConstraint<'a>>),
    LeftSemi(Option<JoinConstraint<'a>>),
    RightSemi(Option<JoinConstraint<'a>>),
    LeftAnti(Option<JoinConstraint<'a>>),
    RightAnti(Option<JoinConstraint<'a>>),
    Anti,
    Lateral,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinConstraint<'a> {
    On(Expr<'a>),
    Using(Vec<Ident<'a>>),
    Natural,
}

/// A table or subquery with optional table and column aliases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOrSubquery<'a> {
    pub item: TableLike<'a>,
    /// FROM <table> AS <alias>
    pub alias: Option<Ident<'a>>,
    /// FROM <table> AS <alias>(<col-alias>, ...)
    pub col_aliases: Option<Vec<Ident<'a>>>,
}

/// A table-like item in the query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableLike<'a> {
    /// FROM <table>
    Table(ObjectReference<'a>),
    /// A function with expressions.
    ///
    /// FROM <function>(<expr>)
    Function {
        name: ObjectReference<'a>,
        args: Vec<Expr<'a>>,
    },
    /// FROM <subquery>
    Subquery(
        // TODO
    ),
    Values(
        // TODO
    ),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupBy<'a> {
    /// `GROUP BY ALL`
    All,
    /// `GROUP BY <expr>[, ...]`
    Exprs(Vec<Expr<'a>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderBy<'a> {
    /// `ORDER BY ALL`
    All {
        asc_desc: Option<OrderByAscDesc>,
        nulls: Option<OrderByNulls>,
    },
    /// `ORDER BY <expr>[, ...]`
    Exprs(Vec<OrderByItem<'a>>),
}

/// A single expression in an order by clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByItem<'a> {
    pub expr: Expr<'a>,
    pub asc: Option<OrderByAscDesc>,
    pub nulls: Option<OrderByNulls>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByAscDesc {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByNulls {
    First,
    Last,
}

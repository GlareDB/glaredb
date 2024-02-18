use crate::keywords::{Keyword, RESERVED_FOR_COLUMN_ALIAS, RESERVED_FOR_TABLE_ALIAS};
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

impl<'a> AstParseable<'a> for Ident<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(RayexecError::new(
                    "Expected identifier, found end of statement",
                ))
            }
        };

        match tok {
            Token::Word(w) => Ok(Ident { value: w.value }),
            other => Err(RayexecError::new(format!(
                "Unexpected token: {other:?}. Expected an identifier.",
            ))),
        }
    }
}

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectReference<'a>(pub Vec<Ident<'a>>);

impl<'a> AstParseable<'a> for ObjectReference<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let mut idents = Vec::new();
        loop {
            let tok = match parser.next() {
                Some(tok) => tok,
                None => break,
            };
            let ident = match &tok.token {
                Token::Word(w) => Ident { value: w.value },
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected token: {other:?}. Expected an object reference.",
                    )))
                }
            };
            idents.push(ident);

            // Check if the next token is a period for possible compound
            // identifiers. If not, we're done.
            if !parser.consume_token(&Token::Period) {
                break;
            }
        }

        Ok(ObjectReference(idents))
    }
}

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
                    _ => Expr::Ident(Ident { value: w.value }),
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
    pub from: TableList<'a>,
    /// WHERE
    pub where_expr: Option<Expr<'a>>,
    /// Group by expression.
    pub group_by: Option<GroupByList<'a>>,
    /// Having expression.
    ///
    /// May exist even if group by isn't provided.
    pub having: Option<Expr<'a>>,
    /// Order by expression.
    pub order_by: Option<OrderByList<'a>>,
    pub limit: Option<Expr<'a>>,
    pub offset: Option<Expr<'a>>,
    // TODO: Window
}

impl<'a> AstParseable<'a> for SelectNode<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        parser.expect_keyword(Keyword::SELECT)?;

        // DISTINCT/DISTINCT ON
        let modifier = SelectModifer::parse(parser)?;

        // Projection list
        let projections = SelectList::parse(parser)?;

        // FROM
        let from = if parser.parse_keyword(Keyword::FROM) {
            TableList::parse(parser)?
        } else {
            TableList::empty()
        };

        // WHERE
        let where_expr = if parser.parse_keyword(Keyword::WHERE) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        // GROUP BY
        let group_by = if parser.parse_keyword_sequence(&[Keyword::GROUP, Keyword::BY]) {
            Some(GroupByList::parse(parser)?)
        } else {
            None
        };

        // HAVING
        let having = if parser.parse_keyword(Keyword::HAVING) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        // TODO: Window
        // TODO: Qualify

        // ORDER BY
        let order_by = if parser.parse_keyword_sequence(&[Keyword::ORDER, Keyword::BY]) {
            Some(OrderByList::parse(parser)?)
        } else {
            None
        };

        // LIMIT
        let limit = if parser.parse_keyword(Keyword::LIMIT) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        // OFFSET
        let offset = if parser.parse_keyword(Keyword::OFFSET) {
            Some(Expr::parse(parser)?)
        } else {
            None
        };

        Ok(SelectNode {
            modifier,
            projections,
            from,
            where_expr,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }
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

/// A list of tables with their joins.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableList<'a>(pub Vec<TableWithJoins<'a>>);

impl<'a> TableList<'a> {
    pub fn empty() -> Self {
        Self::default()
    }
}

impl<'a> AstParseable<'a> for TableList<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let tables = parser.parse_comma_separated(TableWithJoins::parse)?;
        Ok(TableList(tables))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableWithJoins<'a> {
    pub table: TableLike<'a>,
    pub joins: Vec<Join<'a>>,
}

impl<'a> AstParseable<'a> for TableWithJoins<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        // TODO: Natural, asof

        let table = TableLike::parse(parser)?;

        let mut joins = Vec::new();
        loop {
            let join = if parser.parse_keyword_sequence(&[Keyword::CROSS, Keyword::JOIN]) {
                Join {
                    join: JoinOperation {
                        join_type: JoinType::Cross,
                        join_modifier: None,
                    },
                    table: TableLike::parse(parser)?,
                }
            } else if parser.parse_keyword_sequence(&[Keyword::JOIN])
                || parser.parse_keyword_sequence(&[Keyword::INNER, Keyword::JOIN])
            {
                let table = TableLike::parse(parser)?;
                let join = JoinOperation {
                    join_type: JoinType::Inner(JoinConstraint::parse(parser)?),
                    join_modifier: None,
                };
                Join { join, table }
            } else if parser.parse_keyword_sequence(&[Keyword::LEFT, Keyword::JOIN])
                || parser.parse_keyword_sequence(&[Keyword::LEFT, Keyword::OUTER, Keyword::JOIN])
            {
                let table = TableLike::parse(parser)?;
                let join = JoinOperation {
                    join_type: JoinType::Left(JoinConstraint::parse(parser)?),
                    join_modifier: None,
                };
                Join { join, table }
            } else if parser.parse_keyword_sequence(&[Keyword::RIGHT, Keyword::JOIN])
                || parser.parse_keyword_sequence(&[Keyword::RIGHT, Keyword::OUTER, Keyword::JOIN])
            {
                let table = TableLike::parse(parser)?;
                let join = JoinOperation {
                    join_type: JoinType::Right(JoinConstraint::parse(parser)?),
                    join_modifier: None,
                };
                Join { join, table }
            } else if parser.parse_keyword_sequence(&[Keyword::FULL, Keyword::JOIN])
                || parser.parse_keyword_sequence(&[Keyword::FULL, Keyword::OUTER, Keyword::JOIN])
            {
                let table = TableLike::parse(parser)?;
                let join = JoinOperation {
                    join_type: JoinType::Full(JoinConstraint::parse(parser)?),
                    join_modifier: None,
                };
                Join { join, table }
            } else {
                break;
            };

            joins.push(join)
        }

        Ok(TableWithJoins { table, joins })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join<'a> {
    pub join: JoinOperation<'a>,
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
    Inner(JoinConstraint<'a>),
    Left(JoinConstraint<'a>),
    Right(JoinConstraint<'a>),
    Full(JoinConstraint<'a>),
    LeftSemi(JoinConstraint<'a>),
    RightSemi(JoinConstraint<'a>),
    LeftAnti(JoinConstraint<'a>),
    RightAnti(JoinConstraint<'a>),
    Anti,
    Lateral,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinConstraint<'a> {
    On(Expr<'a>),
    Using(Vec<Ident<'a>>),
    Natural,
    None,
}

impl<'a> AstParseable<'a> for JoinConstraint<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let tok = match parser.peek() {
            Some(tok) => tok,
            None => return Ok(JoinConstraint::None),
        };

        let kw = match tok.keyword() {
            Some(kw) => kw,
            None => return Ok(JoinConstraint::None),
        };

        match kw {
            Keyword::ON => {
                parser.next();
                let expr = Expr::parse(parser)?;
                Ok(JoinConstraint::On(expr))
            }
            Keyword::USING => {
                parser.next();
                let idents = parser.parse_parenthesized_comma_separated(|parser| {
                    let expr = Expr::parse(parser)?;
                    match expr {
                        Expr::Ident(ident) => Ok(ident),
                        other => Err(RayexecError::new(format!(
                            "Expected column identifier, found {other:?}"
                        ))),
                    }
                })?;
                Ok(JoinConstraint::Using(idents))
            }
            _ => Ok(JoinConstraint::None),
        }
    }
}

/// A table or subquery with optional table and column aliases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOrSubquery<'a> {
    pub table: TableLike<'a>,
    /// FROM <table> AS <alias>
    pub alias: Option<Ident<'a>>,
    /// FROM <table> AS <alias>(<col-alias>, ...)
    pub col_aliases: Option<Vec<Ident<'a>>>,
}

impl<'a> AstParseable<'a> for TableOrSubquery<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let table = TableLike::parse(parser)?;

        let alias = parser.parse_alias(RESERVED_FOR_TABLE_ALIAS)?;
        let col_aliases = if alias.is_some() && parser.consume_token(&Token::LeftParen) {
            let aliases = parser.parse_comma_separated(Ident::parse)?;
            parser.expect_token(&Token::RightParen)?;
            Some(aliases)
        } else {
            None
        };

        Ok(TableOrSubquery {
            table,
            alias,
            col_aliases,
        })
    }
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
        args: Vec<FunctionArg<'a>>,
    },
    Derived {
        // TODO
    },
}

impl<'a> AstParseable<'a> for TableLike<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        // TODO: Few others as well.
        if parser.consume_token(&Token::LeftParen) {
            // TODO: derived table
            unimplemented!()
        } else {
            // Normal table or table function.
            let name = ObjectReference::parse(parser)?;

            if parser.consume_token(&Token::LeftParen) {
                // Table function
                //
                // `table_func(<exprs>, ...)`

                // Maybe be a function with no args.
                let args = if parser.consume_token(&Token::RightParen) {
                    Vec::new()
                } else {
                    let args = parser.parse_comma_separated(FunctionArg::parse)?;
                    parser.expect_token(&Token::RightParen)?;
                    args
                };

                Ok(TableLike::Function { name, args })
            } else {
                // Just a table.
                Ok(TableLike::Table(name))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionArg<'a> {
    /// A named argument. Allows use of either `=>` or `=` for assignment.
    ///
    /// `ident => <expr>` or `ident = <expr>`
    Named { name: Ident<'a>, arg: Expr<'a> },
    /// `<expr>`
    Unnamed { arg: Expr<'a> },
}

impl<'a> AstParseable<'a> for FunctionArg<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let is_named = match parser.peek_nth(1) {
            Some(tok) => matches!(tok.token, Token::RightArrow | Token::Eq),
            None => false,
        };

        if is_named {
            let ident = Ident::parse(parser)?;
            parser.expect_one_of_tokens(&[&Token::RightArrow, &Token::Eq])?;
            let expr = Expr::parse(parser)?;

            Ok(FunctionArg::Named {
                name: ident,
                arg: expr,
            })
        } else {
            let expr = Expr::parse(parser)?;
            Ok(FunctionArg::Unnamed { arg: expr })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupByList<'a> {
    All,
    Exprs { exprs: Vec<GroupByExpr<'a>> },
}

impl<'a> AstParseable<'a> for GroupByList<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        if parser.parse_keyword(Keyword::ALL) {
            Ok(GroupByList::All)
        } else {
            let exprs = parser.parse_comma_separated(GroupByExpr::parse)?;
            Ok(GroupByList::Exprs { exprs })
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderByList<'a> {
    All { options: OrderByOptions },
    Exprs { exprs: Vec<OrderByExpr<'a>> },
}

impl<'a> AstParseable<'a> for OrderByList<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        if parser.parse_keyword(Keyword::ALL) {
            let options = OrderByOptions::parse(parser)?;
            Ok(OrderByList::All { options })
        } else {
            let exprs = parser.parse_comma_separated(OrderByExpr::parse)?;
            Ok(OrderByList::Exprs { exprs })
        }
    }
}

/// A single expression in an order by clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByExpr<'a> {
    pub expr: Expr<'a>,
    pub options: OrderByOptions,
}

impl<'a> AstParseable<'a> for OrderByExpr<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let expr = Expr::parse(parser)?;
        let options = OrderByOptions::parse(parser)?;

        Ok(OrderByExpr { expr, options })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderByOptions {
    pub asc: Option<OrderByAscDesc>,
    pub nulls: Option<OrderByNulls>,
}

impl<'a> AstParseable<'a> for OrderByOptions {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let asc = if parser.parse_keyword(Keyword::ASC) {
            Some(OrderByAscDesc::Ascending)
        } else if parser.parse_keyword(Keyword::DESC) {
            Some(OrderByAscDesc::Descending)
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

        Ok(OrderByOptions { asc, nulls })
    }
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

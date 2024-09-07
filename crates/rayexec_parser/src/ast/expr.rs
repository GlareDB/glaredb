use std::ops::Neg;

use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::{Token, Word},
};

use super::{AstParseable, DataType, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT(true)`
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    /// String starts with operator, e.g. `a ^@ b`
    StringStartsWith,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal<T: AstMeta> {
    /// Unparsed number literal.
    Number(String),
    /// String literal.
    SingleQuotedString(String),
    /// Boolean literal.
    Boolean(bool),
    /// Null literal
    Null,
    /// Struct literal.
    ///
    /// Lengths of keys and values must be the same.
    Struct {
        keys: Vec<String>,
        values: Vec<Expr<T>>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Function<T: AstMeta> {
    pub reference: T::FunctionReference,
    pub args: Vec<FunctionArg<T>>,
    /// Filter part of `COUNT(col) FILTER (WHERE col > 5)`
    pub filter: Option<Box<Expr<T>>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FunctionArg<T: AstMeta> {
    /// A named argument. Allows use of either `=>` or `=` for assignment.
    ///
    /// `ident => <expr>` or `ident = <expr>`
    Named {
        name: Ident,
        arg: FunctionArgExpr<T>,
    },
    /// `<expr>`
    Unnamed { arg: FunctionArgExpr<T> },
}

impl AstParseable for FunctionArg<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let is_named = match parser.peek_nth(1) {
            Some(tok) => matches!(tok.token, Token::RightArrow | Token::Eq),
            None => false,
        };

        if is_named {
            let ident = Ident::parse(parser)?;
            parser.expect_one_of_tokens(&[&Token::RightArrow, &Token::Eq])?;
            let expr = FunctionArgExpr::parse(parser)?;

            Ok(FunctionArg::Named {
                name: ident,
                arg: expr,
            })
        } else {
            let expr = FunctionArgExpr::parse(parser)?;
            Ok(FunctionArg::Unnamed { arg: expr })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FunctionArgExpr<T: AstMeta> {
    Wildcard,
    Expr(Expr<T>),
}

impl AstParseable for FunctionArgExpr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        match parser.peek() {
            Some(tok) if tok.token == Token::Mul => {
                let _ = parser.next(); // Consume.
                Ok(Self::Wildcard)
            }
            _ => Ok(Self::Expr(Expr::parse(parser)?)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr<T: AstMeta> {
    /// Column or table identifier.
    Ident(Ident),
    /// Compound identifier.
    ///
    /// `table.col`
    CompoundIdent(Vec<Ident>),
    /// Identifier followed by '*'.
    ///
    /// `table.*`
    QualifiedWildcard(Vec<Ident>),
    /// An expression literal,
    Literal(Literal<T>),
    /// [<expr1>, <expr2>, ...]
    Array(Vec<Expr<T>>),
    /// my_array[1]
    ArraySubscript {
        expr: Box<Expr<T>>,
        subscript: Box<ArraySubscript<T>>,
    },
    /// Unary expression.
    UnaryExpr {
        op: UnaryOperator,
        expr: Box<Expr<T>>,
    },
    /// A binary expression.
    BinaryExpr {
        left: Box<Expr<T>>,
        op: BinaryOperator,
        right: Box<Expr<T>>,
    },
    /// A function call.
    Function(Function<T>),
    /// Scalar subquery.
    Subquery(Box<QueryNode<T>>),
    /// Nested expression wrapped in parenthesis.
    ///
    /// (1 + 2)
    Nested(Box<Expr<T>>),
    /// Tuple of expressions.
    ///
    /// (1, 2)
    Tuple(Vec<Expr<T>>),
    /// A colation.
    ///
    /// `<expr> COLLATE <collation>`
    Collate {
        expr: Box<Expr<T>>,
        collation: ObjectReference,
    },
    /// EXISTS/NOT EXISTS
    Exists {
        subquery: Box<QueryNode<T>>,
        not_exists: bool,
    },
    /// DATE '1992-10-11'
    TypedString {
        datatype: T::DataType,
        value: String,
    },
    /// Cast expression.
    ///
    /// `CAST(<expr> AS <datatype>)`
    /// `<expr>::<datatype>`
    Cast {
        datatype: T::DataType,
        expr: Box<Expr<T>>,
    },
    /// LIKE/NOT LIKE
    /// ILIKE/NOT ILIKE
    Like {
        expr: Box<Expr<T>>,
        pattern: Box<Expr<T>>,
        not_like: bool,
        case_insensitive: bool,
    },
    /// Interval
    ///
    /// `INTERVAL '1 year 2 months'`
    /// `INTERVAL 1 YEAR`
    Interval(Interval<T>),
}

impl AstParseable for Expr<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Self::parse_subexpr(parser, 0)
    }
}

impl Expr<Raw> {
    // Precdences, ordered low to high.
    const PREC_OR: u8 = 10;
    const PREC_AND: u8 = 20;
    const PREC_NOT: u8 = 30;
    const PREC_IS: u8 = 40;
    const PREC_COMPARISON: u8 = 50; // <=, =, etc
    const PREC_CONTAINMENT: u8 = 60; // BETWEEN, IN, LIKE, etc
    const PREC_EVERYTHING_ELSE: u8 = 70; // Anything without a specific precedence.
    const PREC_ADD_SUB: u8 = 80;
    const PREC_MUL_DIV_MOD: u8 = 90;
    const _PREC_EXPONENTIATION: u8 = 100;
    const PREC_UNARY_MINUS: u8 = 105;
    const _PREC_AT: u8 = 110; // AT TIME ZONE
    const _PREC_COLLATE: u8 = 120;
    const PREC_ARRAY_ELEM: u8 = 130; // []
    const PREC_CAST: u8 = 140; // ::

    fn parse_subexpr(parser: &mut Parser, precendence: u8) -> Result<Self> {
        let mut expr = Expr::parse_prefix(parser)?;

        loop {
            let next_precedence = Self::get_infix_precedence(parser)?;
            if precendence >= next_precedence {
                break;
            }

            expr = Self::parse_infix(parser, expr, next_precedence)?;
        }

        Ok(expr)
    }

    fn parse_prefix(parser: &mut Parser) -> Result<Self> {
        // Try to parse a possibly typed string.
        //
        // DATE '1992-10-11'
        // BOOL 'true'
        match parser.maybe_parse(DataType::parse) {
            // INTERVAL is a special case.
            Some(DataType::Interval) => {
                let interval = Interval::parse(parser)?;
                return Ok(Expr::Interval(interval));
            }
            Some(dt) => {
                let s = Self::parse_string_literal(parser)?;
                return Ok(Expr::TypedString {
                    datatype: dt,
                    value: s,
                });
            }
            None => (), // Continue trying to parse a normal expression.
        }

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
                    Keyword::EXISTS => {
                        parser.expect_token(&Token::LeftParen)?;
                        let subquery = QueryNode::parse(parser)?;
                        parser.expect_token(&Token::RightParen)?;
                        Expr::Exists {
                            subquery: Box::new(subquery),
                            not_exists: false,
                        }
                    }
                    Keyword::NOT => match parser.peek().map(|t| &t.token) {
                        Some(Token::Word(w)) if w.keyword == Some(Keyword::EXISTS) => {
                            parser.expect_keyword(Keyword::EXISTS)?;
                            parser.expect_token(&Token::LeftParen)?;
                            let subquery = QueryNode::parse(parser)?;
                            parser.expect_token(&Token::RightParen)?;
                            Expr::Exists {
                                subquery: Box::new(subquery),
                                not_exists: true,
                            }
                        }
                        _ => Expr::UnaryExpr {
                            op: UnaryOperator::Not,
                            expr: Box::new(Expr::parse_subexpr(parser, Self::PREC_NOT)?),
                        },
                    },
                    Keyword::CAST => {
                        parser.expect_token(&Token::LeftParen)?;
                        let expr = Expr::parse(parser)?;
                        parser.expect_keyword(Keyword::AS)?;
                        let datatype = DataType::parse(parser)?;
                        parser.expect_token(&Token::RightParen)?;
                        Expr::Cast {
                            datatype,
                            expr: Box::new(expr),
                        }
                    }
                    _ => Self::parse_ident_expr(w.clone(), parser)?,
                },
                None => Self::parse_ident_expr(w.clone(), parser)?,
            },
            Token::LeftBracket => {
                if parser.consume_token(&Token::RightBracket) {
                    Expr::Array(Vec::new())
                } else {
                    let expr = Expr::Array(parser.parse_comma_separated(Expr::parse)?);
                    parser.expect_token(&Token::RightBracket)?;
                    expr
                }
            }
            Token::SingleQuotedString(s) => Expr::Literal(Literal::SingleQuotedString(s.clone())),
            Token::Number(s) => Expr::Literal(Literal::Number(s.clone())),
            Token::LeftParen => {
                let expr = if QueryNode::is_query_node_start(parser) {
                    let subquery = QueryNode::parse(parser)?;
                    Expr::Subquery(Box::new(subquery))
                } else {
                    let mut exprs = parser.parse_comma_separated(Expr::parse)?;
                    match exprs.len() {
                        0 => return Err(RayexecError::new("No expressions")),
                        1 => Expr::Nested(Box::new(exprs.pop().unwrap())),
                        _ => Expr::Tuple(exprs),
                    }
                };
                parser.expect_token(&Token::RightParen)?;
                expr
            }
            Token::Minus => Expr::UnaryExpr {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::parse_subexpr(parser, Self::PREC_UNARY_MINUS)?),
            },
            Token::Plus => Expr::UnaryExpr {
                op: UnaryOperator::Plus,
                expr: Box::new(Expr::parse_subexpr(parser, Self::PREC_UNARY_MINUS)?),
            },
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected token '{other:?}'. Expected expression."
                )))
            }
        };

        Ok(expr)
    }

    fn parse_infix(parser: &mut Parser, prefix: Expr<Raw>, precendence: u8) -> Result<Self> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(RayexecError::new(
                    "Expected infix expression, found end of statement",
                ))
            }
        };

        let bin_op: Option<BinaryOperator> = match tok {
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Div => Some(BinaryOperator::Divide),
            Token::IntDiv => Some(BinaryOperator::IntDiv),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::Concat => Some(BinaryOperator::StringConcat),
            Token::CaretAt => Some(BinaryOperator::StringStartsWith),
            Token::Word(w) => match w.keyword {
                Some(Keyword::AND) => Some(BinaryOperator::And),
                Some(Keyword::OR) => Some(BinaryOperator::Or),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = bin_op {
            if let Some(_kw) = parser.parse_one_of_keywords(&[Keyword::ALL, Keyword::ANY]) {
                unimplemented!()
            } else {
                Ok(Expr::BinaryExpr {
                    left: Box::new(prefix),
                    op,
                    right: Box::new(Expr::parse_subexpr(parser, precendence)?),
                })
            }
        } else if let Token::Word(w) = &tok {
            let kw = match w.keyword {
                Some(kw) => kw,
                None => {
                    return Err(RayexecError::new(format!(
                        "Unexpected token in infix expression: {w}"
                    )))
                }
            };

            match kw {
                Keyword::IS => {
                    unimplemented!()
                }
                Keyword::NOT => match parser.next_keyword()? {
                    Keyword::LIKE => Ok(Expr::Like {
                        not_like: true,
                        case_insensitive: false,
                        expr: Box::new(prefix),
                        pattern: Box::new(Expr::parse_subexpr(parser, Self::PREC_CONTAINMENT)?),
                    }),
                    Keyword::ILIKE => Ok(Expr::Like {
                        not_like: true,
                        case_insensitive: true,
                        expr: Box::new(prefix),
                        pattern: Box::new(Expr::parse_subexpr(parser, Self::PREC_CONTAINMENT)?),
                    }),
                    other => {
                        return Err(RayexecError::new(format!(
                            "Unexpected keyword in infix expression: {other}"
                        )))
                    }
                },
                Keyword::LIKE => Ok(Expr::Like {
                    not_like: false,
                    case_insensitive: false,
                    expr: Box::new(prefix),
                    pattern: Box::new(Expr::parse_subexpr(parser, Self::PREC_CONTAINMENT)?),
                }),
                Keyword::ILIKE => Ok(Expr::Like {
                    not_like: false,
                    case_insensitive: true,
                    expr: Box::new(prefix),
                    pattern: Box::new(Expr::parse_subexpr(parser, Self::PREC_CONTAINMENT)?),
                }),
                other => {
                    return Err(RayexecError::new(format!(
                        "Unexpected keyword in infix expression: {other}"
                    )))
                }
            }
        } else if tok == &Token::LeftBracket {
            let subscript = ArraySubscript::parse(parser)?;
            Ok(Expr::ArraySubscript {
                expr: Box::new(prefix),
                subscript: Box::new(subscript),
            })
        } else if tok == &Token::DoubleColon {
            Ok(Expr::Cast {
                datatype: DataType::parse(parser)?,
                expr: Box::new(prefix),
            })
        } else {
            Err(RayexecError::new(format!(
                "Unable to parse token {:?} as an expression",
                tok
            )))
        }
    }

    /// Get the relative precedence of the next operator.
    ///
    /// If the operator is right associative, it's not considered an infix
    /// operator and zero will be returned.
    ///
    /// See <https://www.postgresql.org/docs/16/sql-syntax-lexical.html#SQL-PRECEDENCE>
    fn get_infix_precedence(parser: &mut Parser) -> Result<u8> {
        let tok = match parser.peek() {
            Some(tok) => &tok.token,
            None => return Ok(0),
        };

        match tok {
            Token::Word(w) if w.keyword == Some(Keyword::OR) => Ok(Self::PREC_OR),
            Token::Word(w) if w.keyword == Some(Keyword::AND) => Ok(Self::PREC_AND),

            Token::Word(w) if w.keyword == Some(Keyword::NOT) => {
                // Precedence depends on keyword following it.
                let next_kw = match parser.peek_nth(1) {
                    Some(tok) => match tok.keyword() {
                        Some(kw) => kw,
                        None => return Ok(0),
                    },
                    None => return Ok(0),
                };

                match next_kw {
                    Keyword::IN => Ok(Self::PREC_CONTAINMENT),
                    Keyword::BETWEEN => Ok(Self::PREC_CONTAINMENT),
                    Keyword::LIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::ILIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::RLIKE => Ok(Self::PREC_CONTAINMENT),
                    Keyword::REGEXP => Ok(Self::PREC_CONTAINMENT),
                    Keyword::SIMILAR => Ok(Self::PREC_CONTAINMENT),
                    _ => Ok(0),
                }
            }

            Token::Word(w) if w.keyword == Some(Keyword::IS) => {
                let next_kw = match parser.peek_nth(1) {
                    Some(tok) => match tok.keyword() {
                        Some(kw) => kw,
                        None => return Ok(0),
                    },
                    None => return Ok(0),
                };

                match next_kw {
                    Keyword::NULL => Ok(Self::PREC_IS),
                    _ => Ok(Self::PREC_IS),
                }
            }
            Token::Word(w) if w.keyword == Some(Keyword::IN) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::BETWEEN) => Ok(Self::PREC_CONTAINMENT),

            // "LIKE"
            Token::Word(w) if w.keyword == Some(Keyword::LIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::ILIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::RLIKE) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::REGEXP) => Ok(Self::PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::SIMILAR) => Ok(Self::PREC_CONTAINMENT),

            // Equalities
            Token::Eq
            | Token::DoubleEq
            | Token::Neq
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq => Ok(Self::PREC_COMPARISON),

            // Numeric operators
            Token::Plus | Token::Minus => Ok(Self::PREC_ADD_SUB),
            Token::Mul | Token::Div | Token::IntDiv | Token::Mod => Ok(Self::PREC_MUL_DIV_MOD),

            // Cast
            Token::DoubleColon => Ok(Self::PREC_CAST),

            // Concat
            Token::Concat => Ok(Self::PREC_EVERYTHING_ELSE),

            // Starts with
            Token::CaretAt => Ok(Self::PREC_EVERYTHING_ELSE),

            // Array, struct literals
            Token::LeftBrace | Token::LeftBracket => Ok(Self::PREC_ARRAY_ELEM),

            _ => Ok(0),
        }
    }

    /// Handle parsing expressions containing identifiers, starting with a word
    /// that is known to already be part of an identifier.
    fn parse_ident_expr(w: Word, parser: &mut Parser) -> Result<Expr<Raw>> {
        let mut wildcard = false;
        let mut idents = vec![Ident::from(w)];

        // Possibly compound identifier.
        while parser.consume_token(&Token::Period) {
            match parser.next() {
                Some(tok) => match &tok.token {
                    Token::Word(w) => idents.push(w.clone().into()),
                    Token::Mul => wildcard = true,
                    other => {
                        return Err(RayexecError::new(format!(
                            "Unexpected token in compound identifier: {other:?}"
                        )))
                    }
                },
                None => return Err(RayexecError::new("Expected identifier after '.'")),
            };
        }

        // Function call if left paren.
        if parser.consume_token(&Token::LeftParen) {
            if wildcard {
                // Someone trying to do this:
                // `namespace.*()`
                return Err(RayexecError::new("Cannot have wildcard function call"));
            }

            let args = if parser.consume_token(&Token::RightParen) {
                Vec::new()
            } else {
                let args = parser.parse_comma_separated(FunctionArg::parse)?;
                parser.expect_token(&Token::RightParen)?;
                args
            };

            // FILTER (WHERE <expr>)
            let filter = if parser.parse_keyword(Keyword::FILTER) {
                parser.expect_token(&Token::LeftParen)?;
                parser.expect_keyword(Keyword::WHERE)?;
                let filter = Expr::parse(parser)?;
                parser.expect_token(&Token::RightParen)?;
                Some(Box::new(filter))
            } else {
                None
            };

            // TODO: Windows

            Ok(Expr::Function(Function {
                reference: ObjectReference(idents),
                args,
                filter,
            }))
        } else {
            Ok(match idents.len() {
                1 if !wildcard => Expr::Ident(idents.pop().unwrap()),
                _ => {
                    if wildcard {
                        Expr::QualifiedWildcard(idents)
                    } else {
                        Expr::CompoundIdent(idents)
                    }
                }
            })
        }
    }

    pub fn parse_string_literal(parser: &mut Parser) -> Result<String> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => return Err(RayexecError::new("Unexpected end of statement")),
        };

        match tok {
            Token::SingleQuotedString(s) => Ok(s.clone()),
            other => Err(RayexecError::new(format!(
                "Expected string literal, got {other:?}"
            ))),
        }
    }

    pub fn parse_i64_literal(parser: &mut Parser) -> Result<i64> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => return Err(RayexecError::new("Unexpected end of statement")),
        };

        let parse = |s: &str| {
            s.parse::<i64>()
                .map_err(|_| RayexecError::new(format!("Unable to parse '{s}' as an integer")))
        };

        match tok {
            Token::Minus => {
                let tok = match parser.next() {
                    Some(tok) => &tok.token,
                    None => return Err(RayexecError::new("Unexpected end of statement")),
                };

                if let Token::Number(s) = tok {
                    return parse(s).map(|v| v.neg());
                }

                Err(RayexecError::new(format!(
                    "Expected integer literal, got {tok:?}"
                )))
            }
            Token::Number(s) => parse(s),
            other => Err(RayexecError::new(format!(
                "Expected integer literal, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntervalUnit {
    Millenium,
    Century,
    Decade,
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl AstParseable for IntervalUnit {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Ok(match parser.next_keyword()? {
            Keyword::MILLENIUM | Keyword::MILLENIUMS => Self::Millenium,
            Keyword::CENTURY | Keyword::CENTURIES => Self::Century,
            Keyword::DECADE | Keyword::DECADES => Self::Decade,
            Keyword::YEAR | Keyword::YEARS => Self::Year,
            Keyword::MONTH | Keyword::MONTHS => Self::Month,
            Keyword::WEEK | Keyword::WEEKS => Self::Week,
            Keyword::DAY | Keyword::DAYS => Self::Day,
            Keyword::HOUR | Keyword::HOURS => Self::Hour,
            Keyword::MINUTE | Keyword::MINUTES => Self::Minute,
            Keyword::SECOND | Keyword::SECONDS => Self::Second,
            Keyword::MILLISECOND | Keyword::MILLISECONDS => Self::Millisecond,
            Keyword::MICROSECOND | Keyword::MICROSECONDS => Self::Microsecond,
            Keyword::NANOSECOND | Keyword::NANOSECONDS => Self::Nanosecond,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected interval unit, got '{other}'"
                )))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Interval<T: AstMeta> {
    pub value: Box<Expr<T>>,
    pub leading: Option<IntervalUnit>,
    pub trailing: Option<IntervalUnit>,
}

impl AstParseable for Interval<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        // TODO: Determine if this is the right precedence. It should be pretty
        // high, but how high?
        let expr = Expr::parse_subexpr(parser, Expr::PREC_CAST)?;

        let trailing = parser.maybe_parse(IntervalUnit::parse);

        Ok(Interval {
            value: Box::new(expr),
            leading: None,
            trailing,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ArraySubscript<T: AstMeta> {
    Index(Expr<T>),
    Slice {
        lower: Option<Expr<T>>,
        upper: Option<Expr<T>>,
        stride: Option<Expr<T>>,
    },
}

impl AstParseable for ArraySubscript<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let lower = if parser.consume_token(&Token::Colon) {
            None
        } else {
            Some(Expr::parse(parser)?)
        };

        if parser.consume_token(&Token::RightBracket) {
            if let Some(lower) = lower {
                return Ok(ArraySubscript::Index(lower));
            }
            return Ok(ArraySubscript::Slice {
                lower,
                upper: None,
                stride: None,
            });
        }

        if lower.is_some() {
            parser.expect_token(&Token::Colon)?;
        }

        if parser.consume_token(&Token::RightBracket) {
            return Ok(ArraySubscript::Slice {
                lower,
                upper: None,
                stride: None,
            });
        }

        let upper = Some(Expr::parse(parser)?);

        if parser.consume_token(&Token::RightBracket) {
            return Ok(ArraySubscript::Slice {
                lower,
                upper,
                stride: None,
            });
        }

        parser.expect_token(&Token::Colon)?;

        let stride = Some(Expr::parse(parser)?);
        parser.expect_token(&Token::RightBracket)?;

        Ok(ArraySubscript::Slice {
            lower,
            upper,
            stride,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn literal() {
        let expr: Expr<_> = parse_ast("5").unwrap();
        let expected = Expr::Literal(Literal::Number("5".to_string()));
        assert_eq!(expected, expr);
    }

    #[test]
    fn compound() {
        let expr: Expr<_> = parse_ast("my_schema.t1").unwrap();
        let expected = Expr::CompoundIdent(vec![
            Ident::from_string("my_schema"),
            Ident::from_string("t1"),
        ]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn compound_with_keyword() {
        let expr: Expr<_> = parse_ast("schema.table").unwrap();
        let expected = Expr::CompoundIdent(vec![
            Ident::from_string("schema"),
            Ident::from_string("table"),
        ]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn qualified_wildcard() {
        let expr: Expr<_> = parse_ast("schema.*").unwrap();
        let expected = Expr::QualifiedWildcard(vec![Ident::from_string("schema")]);
        assert_eq!(expected, expr);
    }

    #[test]
    fn binary_op() {
        let expr: Expr<_> = parse_ast("5 + 8").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("5".to_string()))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Literal(Literal::Number("8".to_string()))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn function_call_simple() {
        let expr: Expr<_> = parse_ast("sum(my_col)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference(vec![Ident::from_string("sum")]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Expr(Expr::Ident(Ident::from_string("my_col"))),
            }],
            filter: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn function_call_no_args() {
        let expr: Expr<_> = parse_ast("random()").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference(vec![Ident::from_string("random")]),
            args: Vec::new(),
            filter: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn function_call_with_over() {
        let expr: Expr<_> = parse_ast("count(x) filter (where x > 5)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference(vec![Ident::from_string("count")]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Expr(Expr::Ident(Ident::from_string("x"))),
            }],
            filter: Some(Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::Ident(Ident::from_string("x"))),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Literal::Number("5".to_string()))),
            })),
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn nested_expr() {
        let expr: Expr<_> = parse_ast("(1 + 2)").unwrap();
        let expected = Expr::Nested(Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("1".to_string()))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Literal(Literal::Number("2".to_string()))),
        }));
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star() {
        let expr: Expr<_> = parse_ast("count(*)").unwrap();
        let expected = Expr::Function(Function {
            reference: ObjectReference::from_strings(["count"]),
            args: vec![FunctionArg::Unnamed {
                arg: FunctionArgExpr::Wildcard,
            }],
            filter: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star_precedence_before() {
        let expr: Expr<_> = parse_ast("111 * count(*)").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(Literal::Number("111".to_string()))),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Function(Function {
                reference: ObjectReference::from_strings(["count"]),
                args: vec![FunctionArg::Unnamed {
                    arg: FunctionArgExpr::Wildcard,
                }],
                filter: None,
            })),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn count_star_precedence_after() {
        let expr: Expr<_> = parse_ast("count(*) * 111").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Function(Function {
                reference: ObjectReference::from_strings(["count"]),
                args: vec![FunctionArg::Unnamed {
                    arg: FunctionArgExpr::Wildcard,
                }],
                filter: None,
            })),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Literal(Literal::Number("111".to_string()))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn date_typed_string() {
        let expr: Expr<_> = parse_ast("date '1992-10-11'").unwrap();
        let expected = Expr::TypedString {
            datatype: DataType::Date,
            value: "1992-10-11".to_string(),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn double_colon_cast() {
        let expr: Expr<_> = parse_ast("4::TEXT").unwrap();
        let expected = Expr::Cast {
            datatype: DataType::Varchar(None),
            expr: Box::new(Expr::Literal(Literal::Number("4".to_string()))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn cast_function() {
        let expr: Expr<_> = parse_ast("CAST('4.0' AS REAL)").unwrap();
        let expected = Expr::Cast {
            datatype: DataType::Real,
            expr: Box::new(Expr::Literal(Literal::SingleQuotedString(
                "4.0".to_string(),
            ))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn interval_typed_string() {
        let expr: Expr<_> = parse_ast("INTERVAL '1 year 2 months'").unwrap();
        let expected = Expr::Interval(Interval {
            value: Box::new(Expr::Literal(Literal::SingleQuotedString(
                "1 year 2 months".to_string(),
            ))),
            leading: None,
            trailing: None,
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn interval_literal() {
        let expr: Expr<_> = parse_ast("INTERVAL 2 YEARS").unwrap();
        let expected = Expr::Interval(Interval {
            value: Box::new(Expr::Literal(Literal::Number("2".to_string()))),
            leading: None,
            trailing: Some(IntervalUnit::Year),
        });
        assert_eq!(expected, expr);
    }

    #[test]
    fn interval_binary_expr() {
        let expr: Expr<_> = parse_ast("INTERVAL '1 year' * 3").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Interval(Interval {
                value: Box::new(Expr::Literal(Literal::SingleQuotedString(
                    "1 year".to_string(),
                ))),
                leading: None,
                trailing: None,
            })),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::Literal(Literal::Number("3".to_string()))),
        };
        assert_eq!(expected, expr);
    }

    #[test]
    fn unary_minus() {
        let expr: Expr<_> = parse_ast("-12").unwrap();
        let expected = Expr::UnaryExpr {
            op: UnaryOperator::Minus,
            expr: Box::new(Expr::Literal(Literal::Number("12".to_string()))),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn unary_minus_bind_right() {
        let expr: Expr<_> = parse_ast("-12 * -23").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::UnaryExpr {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Literal(Literal::Number("12".to_string()))),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expr::UnaryExpr {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Literal(Literal::Number("23".to_string()))),
            }),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_literal_basic() {
        let expr: Expr<_> = parse_ast("[a, b]").unwrap();
        let expected = Expr::Array(vec![
            Expr::Ident(Ident::from_string("a")),
            Expr::Ident(Ident::from_string("b")),
        ]);
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_literal_empty() {
        let expr: Expr<_> = parse_ast("[]").unwrap();
        let expected = Expr::Array(Vec::new());
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_subscript_index() {
        let expr: Expr<_> = parse_ast("my_array[2]").unwrap();
        let expected = Expr::ArraySubscript {
            expr: Box::new(Expr::Ident(Ident::from_string("my_array"))),
            subscript: Box::new(ArraySubscript::Index(Expr::Literal(Literal::Number(
                "2".to_string(),
            )))),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_subscript_slice() {
        let expr: Expr<_> = parse_ast("my_array[1:2]").unwrap();
        let expected = Expr::ArraySubscript {
            expr: Box::new(Expr::Ident(Ident::from_string("my_array"))),
            subscript: Box::new(ArraySubscript::Slice {
                lower: Some(Expr::Literal(Literal::Number("1".to_string()))),
                upper: Some(Expr::Literal(Literal::Number("2".to_string()))),
                stride: None,
            }),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_subscript_slice_no_upper() {
        let expr: Expr<_> = parse_ast("my_array[1:]").unwrap();
        let expected = Expr::ArraySubscript {
            expr: Box::new(Expr::Ident(Ident::from_string("my_array"))),
            subscript: Box::new(ArraySubscript::Slice {
                lower: Some(Expr::Literal(Literal::Number("1".to_string()))),
                upper: None,
                stride: None,
            }),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_subscript_slice_no_lower() {
        let expr: Expr<_> = parse_ast("my_array[:2]").unwrap();
        let expected = Expr::ArraySubscript {
            expr: Box::new(Expr::Ident(Ident::from_string("my_array"))),
            subscript: Box::new(ArraySubscript::Slice {
                lower: None,
                upper: Some(Expr::Literal(Literal::Number("2".to_string()))),
                stride: None,
            }),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn array_subscript_slice_with_stride() {
        let expr: Expr<_> = parse_ast("my_array[1:2:3]").unwrap();
        let expected = Expr::ArraySubscript {
            expr: Box::new(Expr::Ident(Ident::from_string("my_array"))),
            subscript: Box::new(ArraySubscript::Slice {
                lower: Some(Expr::Literal(Literal::Number("1".to_string()))),
                upper: Some(Expr::Literal(Literal::Number("2".to_string()))),
                stride: Some(Expr::Literal(Literal::Number("3".to_string()))),
            }),
        };
        assert_eq!(expected, expr)
    }

    #[test]
    fn string_contains_sugar() {
        let expr: Expr<_> = parse_ast("s1 ^@ s2").unwrap();
        let expected = Expr::BinaryExpr {
            left: Box::new(Expr::Ident(Ident::from_string("s1"))),
            op: BinaryOperator::StringStartsWith,
            right: Box::new(Expr::Ident(Ident::from_string("s2"))),
        };
        assert_eq!(expected, expr);
    }
}

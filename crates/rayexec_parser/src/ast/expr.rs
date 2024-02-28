use rayexec_error::{RayexecError, Result};

use crate::{keywords::Keyword, parser::Parser, tokens::Token};

use super::{AstParseable, Ident, ObjectReference};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperator {
    /// Plus, e.g. `+9`
    Plus,
    /// Minus, e.g. `-9`
    Minus,
    /// Not, e.g. `NOT(true)`
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Literal {
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
        values: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// Column or table identifier.
    Ident(Ident),
    /// Compound identifier.
    ///
    /// `table.col`
    CompoundIdent(Vec<Ident>),
    /// An expression literal,
    Literal(Literal),
    /// A binary expression.
    BinaryExpr {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// A colation.
    ///
    /// `<expr> COLLATE <collation>`
    Collate {
        expr: Box<Expr>,
        collation: ObjectReference,
    },
}

impl AstParseable for Expr {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Self::parse_subexpr(parser, 0)
    }
}

impl Expr {
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
                    _ => Expr::Ident(Ident {
                        value: w.value.clone(),
                    }),
                },
                None => {
                    // TODO: Extend, compound idents.
                    Expr::Ident(Ident {
                        value: w.value.clone(),
                    })
                }
            },
            Token::SingleQuotedString(s) => Expr::Literal(Literal::SingleQuotedString(s.clone())),
            Token::Number(s) => Expr::Literal(Literal::Number(s.clone())),
            other => {
                return Err(RayexecError::new(format!(
                    "Unexpected token '{other:?}'. Expected expression."
                )))
            }
        };

        Ok(expr)
    }

    fn parse_infix(parser: &mut Parser, prefix: Expr, precendence: u8) -> Result<Self> {
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
            Token::Word(w) => match w.keyword {
                Some(Keyword::AND) => Some(BinaryOperator::And),
                Some(Keyword::OR) => Some(BinaryOperator::Or),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = bin_op {
            if let Some(kw) = parser.parse_one_of_keywords(&[Keyword::ALL, Keyword::ANY]) {
                unimplemented!()
            } else {
                Ok(Expr::BinaryExpr {
                    left: Box::new(prefix),
                    op,
                    right: Box::new(Expr::parse_subexpr(parser, precendence)?),
                })
            }
        } else if tok == &Token::LeftBracket {
            // Array index
            unimplemented!()
        } else if tok == &Token::DoubleColon {
            // Cast
            unimplemented!()
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
        // Precdences, ordered low to high.
        const PREC_OR: u8 = 10;
        const PREC_AND: u8 = 20;
        const _PREC_NOT: u8 = 30;
        const PREC_IS: u8 = 40;
        const PREC_COMPARISON: u8 = 50; // <=, =, etc
        const PREC_CONTAINMENT: u8 = 60; // BETWEEN, IN, LIKE, etc
        const PREC_EVERYTHING_ELSE: u8 = 70; // Anything without a specific precedence.
        const PREC_ADD_SUB: u8 = 80;
        const PREC_MUL_DIV_MOD: u8 = 90;
        const _PREC_EXPONENTIATION: u8 = 100;
        const _PREC_AT: u8 = 110; // AT TIME ZONE
        const _PREC_COLLATE: u8 = 120;
        const PREC_ARRAY_ELEM: u8 = 130; // []
        const PREC_CAST: u8 = 140; // ::

        let tok = match parser.peek() {
            Some(tok) => &tok.token,
            None => return Ok(0),
        };

        match tok {
            Token::Word(w) if w.keyword == Some(Keyword::OR) => Ok(PREC_OR),
            Token::Word(w) if w.keyword == Some(Keyword::AND) => Ok(PREC_AND),

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
                    Keyword::IN => Ok(PREC_CONTAINMENT),
                    Keyword::BETWEEN => Ok(PREC_CONTAINMENT),
                    Keyword::LIKE => Ok(PREC_CONTAINMENT),
                    Keyword::ILIKE => Ok(PREC_CONTAINMENT),
                    Keyword::RLIKE => Ok(PREC_CONTAINMENT),
                    Keyword::REGEXP => Ok(PREC_CONTAINMENT),
                    Keyword::SIMILAR => Ok(PREC_CONTAINMENT),
                    _ => return Ok(0),
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
                    Keyword::NULL => Ok(PREC_IS),
                    _ => Ok(PREC_IS),
                }
            }
            Token::Word(w) if w.keyword == Some(Keyword::IN) => Ok(PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::BETWEEN) => Ok(PREC_CONTAINMENT),

            // "LIKE"
            Token::Word(w) if w.keyword == Some(Keyword::LIKE) => Ok(PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::ILIKE) => Ok(PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::RLIKE) => Ok(PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::REGEXP) => Ok(PREC_CONTAINMENT),
            Token::Word(w) if w.keyword == Some(Keyword::SIMILAR) => Ok(PREC_CONTAINMENT),

            // Equalities
            Token::Eq
            | Token::DoubleEq
            | Token::Neq
            | Token::Lt
            | Token::LtEq
            | Token::Gt
            | Token::GtEq => Ok(PREC_COMPARISON),

            // Numeric operators
            Token::Plus | Token::Minus => Ok(PREC_ADD_SUB),
            Token::Mul | Token::Div | Token::IntDiv | Token::Mod => Ok(PREC_MUL_DIV_MOD),

            // Cast
            Token::DoubleColon => Ok(PREC_CAST),

            // Concat
            Token::Concat => Ok(PREC_EVERYTHING_ELSE),

            // Array, struct literals
            Token::LeftBrace | Token::LeftBracket => Ok(PREC_ARRAY_ELEM),

            _ => Ok(0),
        }
    }
}

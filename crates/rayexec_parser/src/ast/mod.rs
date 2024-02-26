pub mod expr;
pub use expr::*;
pub mod from;
pub use from::*;
pub mod query;
pub use query::*;
pub mod modifiers;
pub use modifiers::*;
pub mod select;
pub use select::*;

use crate::parser::Parser;
use crate::tokens::Token;
use rayexec_error::{RayexecError, Result};
use std::borrow::Cow;
use std::fmt;
use std::hash::Hash;

pub trait AstParseable<'a>: Sized {
    /// Parse an instance of Self from the provided parser.
    ///
    /// It's assumed that the parser is in the correct state for parsing Self,
    /// and if it isn't, an error should be returned.
    fn parse(parser: &mut Parser<'a>) -> Result<Self>;
}

#[cfg(test)]
mod testutil {
    use crate::tokens::Tokenizer;

    use super::*;

    /// Parse an AST node directly from a string.
    pub(crate) fn parse_ast<'a, A: AstParseable<'a>>(s: &'a str) -> Result<A> {
        let toks = Tokenizer::new(s).tokenize()?;
        let mut parser = Parser::with_tokens(toks);
        A::parse(&mut parser)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident<'a> {
    pub value: Cow<'a, str>,
}

impl<'a> Ident<'a> {
    pub fn from_string(s: impl Into<Cow<'a, str>>) -> Self {
        Ident { value: s.into() }
    }
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
            Token::Word(w) => Ok(Ident {
                value: w.value.into(),
            }),
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

impl<'a> ObjectReference<'a> {
    /// Create an object from an iterator of strings.
    ///
    /// Useful in tests, probably unlikely that it should be used anywhere else.
    pub fn from_strings<S>(strings: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<Cow<'a, str>>,
    {
        let mut idents = Vec::new();
        for s in strings {
            idents.push(Ident { value: s.into() })
        }
        ObjectReference(idents)
    }

    pub fn base(&self) -> Result<Ident<'a>> {
        match self.0.last() {
            Some(ident) => Ok(ident.clone()),
            None => Err(RayexecError::new("Empty object reference")),
        }
    }
}

impl<'a> AstParseable<'a> for ObjectReference<'a> {
    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let mut idents = Vec::new();
        loop {
            let tok = match parser.next() {
                Some(tok) => tok,
                None => break,
            };
            let ident = match &tok.token {
                Token::Word(w) => Ident {
                    value: w.value.into(),
                },
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

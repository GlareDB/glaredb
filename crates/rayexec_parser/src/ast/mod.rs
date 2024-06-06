pub mod create_table;
pub use create_table::*;
pub mod create_schema;
pub use create_schema::*;
pub mod datatype;
pub use datatype::*;
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
pub mod explain;
pub use explain::*;
pub mod insert;
pub use insert::*;
pub mod variable;
pub use variable::*;
pub mod cte;
pub use cte::*;
pub mod drop;
pub use drop::*;
pub mod attach;
pub use attach::*;

use crate::parser::Parser;
use crate::tokens::{Token, Word};
use rayexec_error::{RayexecError, Result};

use std::fmt;
use std::hash::Hash;

pub trait AstParseable: Sized {
    /// Parse an instance of Self from the provided parser.
    ///
    /// It's assumed that the parser is in the correct state for parsing Self,
    /// and if it isn't, an error should be returned.
    fn parse(parser: &mut Parser) -> Result<Self>;
}

#[cfg(test)]
mod testutil {
    use crate::tokens::Tokenizer;

    use super::*;

    /// Parse an AST node directly from a string.
    pub(crate) fn parse_ast<A: AstParseable>(s: &str) -> Result<A> {
        let toks = Tokenizer::new(s).tokenize()?;
        let mut parser = Parser::with_tokens(toks);
        A::parse(&mut parser)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident {
    pub(crate) value: String,
    pub(crate) quoted: bool,
}

impl Ident {
    pub fn from_string(s: impl Into<String>) -> Self {
        Ident {
            value: s.into(),
            quoted: false,
        }
    }

    /// Returns the string representation of this ident, taking into account if
    /// it's quoted.
    ///
    /// If an identifier is quoted, its case is preserved. Otherwise it's all
    /// lowercase.
    pub fn into_normalized_string(self) -> String {
        if self.quoted {
            self.value
        } else {
            self.value.to_lowercase()
        }
    }

    pub fn as_normalized_string(&self) -> String {
        self.clone().into_normalized_string()
    }
}

impl AstParseable for Ident {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let tok = match parser.next() {
            Some(tok) => &tok.token,
            None => {
                return Err(RayexecError::new(
                    "Expected identifier, found end of statement",
                ))
            }
        };

        match tok {
            Token::Word(w) => Ok(w.clone().into()),
            other => Err(RayexecError::new(format!(
                "Unexpected token: {other:?}. Expected an identifier.",
            ))),
        }
    }
}

impl From<Word> for Ident {
    fn from(w: Word) -> Self {
        Ident {
            value: w.value,
            quoted: w.quote == Some('"'),
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectReference(pub Vec<Ident>);

impl ObjectReference {
    /// Create an object from an iterator of strings.
    ///
    /// Useful in tests, probably unlikely that it should be used anywhere else.
    pub fn from_strings<S>(strings: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<String>,
    {
        let mut idents = Vec::new();
        for s in strings {
            idents.push(Ident {
                value: s.into(),
                quoted: false,
            })
        }
        ObjectReference(idents)
    }

    pub fn base(&self) -> Result<Ident> {
        match self.0.last() {
            Some(ident) => Ok(ident.clone()),
            None => Err(RayexecError::new("Empty object reference")),
        }
    }
}

impl AstParseable for ObjectReference {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let mut idents = Vec::new();
        while let Some(tok) = parser.next() {
            let ident = match &tok.token {
                Token::Word(w) => w.clone().into(),
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

impl fmt::Display for ObjectReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let strings: Vec<_> = self.0.iter().map(|ident| ident.value.to_string()).collect();
        write!(f, "{}", strings.join("."))
    }
}

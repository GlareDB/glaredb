use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr, ObjectReference};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;
use crate::tokens::Token;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetVariable<T: AstMeta> {
    pub reference: T::ItemReference,
    pub value: Expr<T>,
}

impl AstParseable for SetVariable<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::SET)?;

        let name = ObjectReference::parse(parser)?;
        if parser.parse_keyword(Keyword::TO) || parser.consume_token(&Token::Eq) {
            let expr = Expr::parse(parser)?;
            return Ok(SetVariable {
                reference: name,
                value: expr,
            });
        }

        Err(RayexecError::new(format!(
            "Expected 'SET {name} TO <value>' or SET {name} = <value>'"
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShowVariable<T: AstMeta> {
    pub reference: T::ItemReference,
}

impl AstParseable for ShowVariable<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::SHOW)?;
        let name = ObjectReference::parse(parser)?;
        Ok(ShowVariable { reference: name })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VariableOrAll<T: AstMeta> {
    Variable(T::ItemReference),
    All,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResetVariable<T: AstMeta> {
    pub var: VariableOrAll<T>,
}

impl AstParseable for ResetVariable<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::RESET)?;
        let var = if parser.parse_keyword(Keyword::ALL) {
            VariableOrAll::All
        } else {
            VariableOrAll::Variable(ObjectReference::parse(parser)?)
        };
        Ok(ResetVariable { var })
    }
}

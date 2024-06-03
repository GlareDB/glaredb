use crate::{keywords::Keyword, parser::Parser, tokens::Token};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr, ObjectReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetVariable {
    pub reference: ObjectReference,
    pub value: Expr,
}

impl AstParseable for SetVariable {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowVariable {
    pub reference: ObjectReference,
}

impl AstParseable for ShowVariable {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::SHOW)?;
        let name = ObjectReference::parse(parser)?;
        Ok(ShowVariable { reference: name })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableOrAll {
    Variable(ObjectReference),
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetVariable {
    pub var: VariableOrAll,
}

impl AstParseable for ResetVariable {
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

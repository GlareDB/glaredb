use glaredb_error::{DbError, Result};
use serde::{Deserialize, Serialize};

use super::AstParseable;
use crate::keywords::Keyword;
use crate::parser::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscardType {
    All,
    Plans,
    Temp,
    Metadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscardStatement {
    pub discard_type: DiscardType,
}

impl AstParseable for DiscardStatement {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DISCARD)?;

        let discard_type = match parser.next_keyword()? {
            Keyword::ALL => DiscardType::All,
            Keyword::PLANS => DiscardType::Plans,
            Keyword::TEMP | Keyword::TEMPORARY => DiscardType::Temp,
            Keyword::METADATA => DiscardType::Metadata,
            other => {
                return Err(DbError::new(format!(
                    "Got unexpected keyword for discard type: {other}"
                )));
            }
        };

        Ok(DiscardStatement { discard_type })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::testutil::parse_ast;

    #[test]
    fn drop_all() {
        let got = parse_ast::<DiscardStatement>("discard all").unwrap();
        let expected = DiscardStatement {
            discard_type: DiscardType::All,
        };
        assert_eq!(expected, got);
    }
}

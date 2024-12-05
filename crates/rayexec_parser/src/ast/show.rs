use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, ObjectReference};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Show<T: AstMeta> {
    pub reference: T::ShowReference,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShowReference {
    Variable(ObjectReference),
    /// SHOW DATABASES or SHOW CATALOGS.
    Databases,
    /// SHOW SCHEMAS
    Schemas,
    /// SHOW TABLES
    Tables,
}

impl AstParseable for Show<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::SHOW)?;

        let reference = if parser
            .parse_one_of_keywords(&[Keyword::DATABASES, Keyword::CATALOGS])
            .is_some()
        {
            ShowReference::Databases
        } else if parser.parse_keyword(Keyword::SCHEMAS) {
            ShowReference::Schemas
        } else if parser.parse_keyword(Keyword::TABLES) {
            ShowReference::Tables
        } else {
            let name = ObjectReference::parse(parser)?;
            ShowReference::Variable(name)
        };

        Ok(Show { reference })
    }
}

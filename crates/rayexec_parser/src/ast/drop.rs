use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
};
use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, ObjectReference};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DropType {
    Index,
    Function,
    Table,
    View,
    Schema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DropDependents {
    #[default]
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DropStatement<T: AstMeta> {
    pub drop_type: DropType,
    pub if_exists: bool,
    pub name: T::ItemReference,
    pub deps: Option<DropDependents>,
}

impl AstParseable for DropStatement<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DROP)?;

        let drop_type = match parser.next_keyword()? {
            Keyword::TABLE => DropType::Table,
            Keyword::INDEX => DropType::Index,
            Keyword::FUNCTION => DropType::Function,
            Keyword::SCHEMA => DropType::Schema,
            Keyword::VIEW => DropType::View,
            other => {
                return Err(RayexecError::new(format!(
                    "Got unexpected keyword for drop type: {other}"
                )))
            }
        };

        let if_exists = parser.parse_keyword_sequence(&[Keyword::IF, Keyword::EXISTS]);
        let name = ObjectReference::parse(parser)?;

        let deps = if parser.parse_keyword(Keyword::CASCADE) {
            Some(DropDependents::Cascade)
        } else if parser.parse_keyword(Keyword::RESTRICT) {
            Some(DropDependents::Restrict)
        } else {
            None
        };

        Ok(DropStatement {
            drop_type,
            if_exists,
            name,
            deps,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    #[test]
    fn basic() {
        let got = parse_ast::<DropStatement<_>>("drop schema my_schema").unwrap();
        let expected = DropStatement {
            drop_type: DropType::Schema,
            if_exists: false,
            name: ObjectReference::from_strings(["my_schema"]),
            deps: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn drop_table_cascade() {
        let got = parse_ast::<DropStatement<_>>("drop table my_schema.t1 cascade").unwrap();
        let expected = DropStatement {
            drop_type: DropType::Table,
            if_exists: false,
            name: ObjectReference::from_strings(["my_schema", "t1"]),
            deps: Some(DropDependents::Cascade),
        };
        assert_eq!(expected, got);
    }
}

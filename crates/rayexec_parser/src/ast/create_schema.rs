use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
};
use rayexec_error::Result;

use super::{AstParseable, ObjectReference};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchema<T: AstMeta> {
    pub if_not_exists: bool,
    pub name: T::ItemReference,
}

impl AstParseable for CreateSchema<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::CREATE)?;
        parser.expect_keyword(Keyword::SCHEMA)?;

        let if_not_exists =
            parser.parse_keyword_sequence(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = ObjectReference::parse(parser)?;

        Ok(CreateSchema {
            if_not_exists,
            name,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::testutil::parse_ast;

    use super::*;

    #[test]
    fn basic() {
        let got = parse_ast::<CreateSchema<_>>("create schema s1").unwrap();
        let expected = CreateSchema {
            if_not_exists: false,
            name: ObjectReference::from_strings(["s1"]),
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn qualified() {
        let got = parse_ast::<CreateSchema<_>>("create schema db1.s1").unwrap();
        let expected = CreateSchema {
            if_not_exists: false,
            name: ObjectReference::from_strings(["db1", "s1"]),
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn if_not_exists() {
        let got = parse_ast::<CreateSchema<_>>("create schema if not exists s1").unwrap();
        let expected = CreateSchema {
            if_not_exists: true,
            name: ObjectReference::from_strings(["s1"]),
        };
        assert_eq!(expected, got);
    }
}

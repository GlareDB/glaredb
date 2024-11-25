use std::collections::HashMap;

use rayexec_error::{RayexecError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr, Ident, ObjectReference};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttachType {
    Database,
    Table,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attach<T: AstMeta> {
    pub datasource_name: Ident,
    pub attach_type: AttachType,
    pub alias: T::ItemReference,
    pub options: HashMap<Ident, Expr<T>>,
}

impl AstParseable for Attach<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::ATTACH)?;
        let datasource_name = Ident::parse(parser)?;

        let attach_type = match parser.next_keyword()? {
            Keyword::DATABASE => AttachType::Database,
            Keyword::TABLE => AttachType::Table,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected DATABASE or TABLE for attach type, got '{other}'"
                )))
            }
        };

        parser.expect_keyword(Keyword::AS)?;
        let alias = ObjectReference::parse(parser)?;

        let mut options = HashMap::new();
        loop {
            let key = match Ident::parse(parser) {
                Ok(ident) => ident,
                Err(_) => break,
            };

            let val = Expr::parse(parser)?;
            options.insert(key, val);
        }

        Ok(Attach {
            datasource_name,
            attach_type,
            alias,
            options,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Detach<T: AstMeta> {
    pub attach_type: AttachType,
    pub alias: T::ItemReference,
}

impl AstParseable for Detach<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::DETACH)?;
        let attach_type = match parser.next_keyword()? {
            Keyword::DATABASE => AttachType::Database,
            Keyword::TABLE => AttachType::Table,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected DATABASE or TABLE for attach type, got '{other}'"
                )))
            }
        };

        let alias = ObjectReference::parse(parser)?;

        Ok(Detach { alias, attach_type })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::ast::testutil::parse_ast;
    use crate::ast::Literal;

    #[test]
    fn attach_pg_database() {
        let got = parse_ast::<Attach<_>>("ATTACH POSTGRES DATABASE AS my_pg CONNECTION_STRING 'postgres://sean:pass@localhost/db'").unwrap();
        let expected = Attach {
            datasource_name: Ident::new_unquoted("POSTGRES"),
            attach_type: AttachType::Database,
            alias: ObjectReference::from_strings(["my_pg"]),
            options: [(
                Ident::new_unquoted("CONNECTION_STRING"),
                Expr::Literal(Literal::SingleQuotedString(
                    "postgres://sean:pass@localhost/db".to_string(),
                )),
            )]
            .into_iter()
            .collect(),
        };

        assert_eq!(expected, got);
    }

    #[test]
    fn attach_pg_table() {
        let got = parse_ast::<Attach<_>>("ATTACH POSTGRES TABLE AS my_pg_table CONNECTION_STRING 'postgres://sean:pass@localhost/db' SCHEMA 'public' TABLE 'users'").unwrap();
        let expected = Attach {
            datasource_name: Ident::new_unquoted("POSTGRES"),
            attach_type: AttachType::Table,
            alias: ObjectReference::from_strings(["my_pg_table"]),
            options: [
                (
                    Ident::new_unquoted("CONNECTION_STRING"),
                    Expr::Literal(Literal::SingleQuotedString(
                        "postgres://sean:pass@localhost/db".to_string(),
                    )),
                ),
                (
                    Ident::new_unquoted("SCHEMA"),
                    Expr::Literal(Literal::SingleQuotedString("public".to_string())),
                ),
                (
                    Ident::new_unquoted("TABLE"),
                    Expr::Literal(Literal::SingleQuotedString("users".to_string())),
                ),
            ]
            .into_iter()
            .collect(),
        };

        assert_eq!(expected, got);
    }

    #[test]
    fn detach_db() {
        let got = parse_ast::<Detach<_>>("detach database my_pg").unwrap();
        let expected = Detach {
            attach_type: AttachType::Database,
            alias: ObjectReference::from_strings(["my_pg"]),
        };
        assert_eq!(expected, got);
    }
}

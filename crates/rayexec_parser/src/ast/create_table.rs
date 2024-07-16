use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, DataType, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTable<T: AstMeta> {
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub temp: bool,
    pub external: bool,
    pub name: T::ItemReference,
    pub columns: Vec<ColumnDef<T>>,
    pub source: Option<QueryNode<T>>,
}

impl AstParseable for CreateTable<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::CREATE)?;

        let or_replace = parser.parse_keyword_sequence(&[Keyword::OR, Keyword::REPLACE]);
        let temp = parser
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        let external = parser.parse_keyword(Keyword::EXTERNAL);

        parser.expect_keyword(Keyword::TABLE)?;

        let if_not_exists =
            parser.parse_keyword_sequence(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        let name = ObjectReference::parse(parser)?;

        let columns = if parser.consume_token(&Token::LeftParen) {
            let columns = parser.parse_comma_separated(ColumnDef::parse)?;
            parser.expect_token(&Token::RightParen)?;
            columns
        } else {
            Vec::new()
        };

        let source = if parser.parse_keyword(Keyword::AS) {
            Some(QueryNode::parse(parser)?)
        } else {
            None
        };

        Ok(CreateTable {
            or_replace,
            if_not_exists,
            temp,
            external,
            name,
            columns,
            source,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef<T: AstMeta> {
    pub name: T::ColumnReference,
    pub datatype: T::DataType,
    pub opts: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnOption {
    Null,
    NotNull,
}

impl AstParseable for ColumnDef<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        let name = Ident::parse(parser)?;
        let datatype = DataType::parse(parser)?;

        let mut opts = Vec::new();

        if parser.parse_keyword_sequence(&[Keyword::NOT, Keyword::NULL]) {
            opts.push(ColumnOption::NotNull)
        }
        if parser.parse_keyword(Keyword::NULL) {
            opts.push(ColumnOption::Null)
        }

        Ok(ColumnDef {
            name,
            datatype,
            opts,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{testutil::parse_ast, Expr, LimitModifier, Literal, QueryNodeBody, Values};

    use super::*;

    /// Query node for 'values (1)'
    fn query_node_values_1() -> QueryNode<Raw> {
        QueryNode {
            ctes: None,
            order_by: Vec::new(),
            body: QueryNodeBody::Values(Values {
                rows: vec![vec![Expr::Literal(Literal::Number("1".to_string()))]],
            }),
            limit: LimitModifier {
                limit: None,
                offset: None,
            },
        }
    }

    #[test]
    fn basic() {
        let got = parse_ast::<CreateTable<_>>("create table hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
            source: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn two_columns() {
        let got = parse_ast::<CreateTable<_>>("create table hello (a int, world text)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![
                ColumnDef {
                    name: Ident::from_string("a"),
                    datatype: DataType::Integer,
                    opts: Vec::new(),
                },
                ColumnDef {
                    name: Ident::from_string("world"),
                    datatype: DataType::Varchar(None),
                    opts: Vec::new(),
                },
            ],
            source: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn two_columns_trailing_comma() {
        let got = parse_ast::<CreateTable<_>>("create table hello (a int, world text,)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: false,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![
                ColumnDef {
                    name: Ident::from_string("a"),
                    datatype: DataType::Integer,
                    opts: Vec::new(),
                },
                ColumnDef {
                    name: Ident::from_string("world"),
                    datatype: DataType::Varchar(None),
                    opts: Vec::new(),
                },
            ],
            source: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn temp() {
        let got = parse_ast::<CreateTable<_>>("create temp table hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: true,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
            source: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn temp_if_not_exists() {
        let got =
            parse_ast::<CreateTable<_>>("create temp table if not exists hello (a int)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: true,
            temp: true,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: vec![ColumnDef {
                name: Ident::from_string("a"),
                datatype: DataType::Integer,
                opts: Vec::new(),
            }],
            source: None,
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn temp_ctas() {
        let got = parse_ast::<CreateTable<_>>("create temp table hello as values (1)").unwrap();
        let expected = CreateTable {
            or_replace: false,
            if_not_exists: false,
            temp: true,
            external: false,
            name: ObjectReference::from_strings(["hello"]),
            columns: Vec::new(),
            source: Some(query_node_values_1()),
        };
        assert_eq!(expected, got);
    }
}

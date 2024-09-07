use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CopyToSource<T: AstMeta> {
    Query(QueryNode<T>),
    // TODO: Include optional columns.
    Table(T::TableReference),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CopyToTarget {
    File(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyOption<T: AstMeta> {
    pub key: Ident,
    pub val: Expr<T>,
}

impl AstParseable for CopyOption<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        Ok(CopyOption {
            key: Ident::parse(parser)?,
            val: Expr::parse(parser)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyTo<T: AstMeta> {
    pub source: CopyToSource<T>,
    pub target: T::CopyToDestination,
    pub options: T::CopyToOptions,
}

impl AstParseable for CopyTo<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::COPY)?;

        let source = if parser.consume_token(&Token::LeftParen) {
            let query = QueryNode::parse(parser)?;
            let source = CopyToSource::Query(query);
            parser.expect_token(&Token::RightParen)?;
            source
        } else {
            let reference = ObjectReference::parse(parser)?;
            CopyToSource::Table(reference)
        };

        parser.expect_keyword(Keyword::TO)?;

        let target = CopyToTarget::File(Expr::parse_string_literal(parser)?);

        let options = if parser.consume_token(&Token::LeftParen) {
            let options = parser.parse_comma_separated(CopyOption::parse)?;
            parser.expect_token(&Token::RightParen)?;
            options
        } else {
            Vec::new()
        };

        Ok(CopyTo {
            source,
            target,
            options,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{
        testutil::parse_ast, LimitModifier, Literal, QueryNodeBody, SelectExpr, SelectNode,
    };

    use super::*;

    #[test]
    fn copy_to_from_table() {
        let node: CopyTo<_> = parse_ast("COPY my_schema.my_table TO 'myfile.csv'").unwrap();
        let expected = CopyTo {
            source: CopyToSource::Table(ObjectReference::from_strings(["my_schema", "my_table"])),
            target: CopyToTarget::File("myfile.csv".to_string()),
            options: Vec::new(),
        };
        assert_eq!(expected, node);
    }

    #[test]
    fn copy_to_from_table_with_single_option() {
        let node: CopyTo<_> =
            parse_ast("COPY my_schema.my_table TO 'myfile' (FORMAT parquet)").unwrap();
        let expected = CopyTo {
            source: CopyToSource::Table(ObjectReference::from_strings(["my_schema", "my_table"])),
            target: CopyToTarget::File("myfile".to_string()),
            options: vec![CopyOption {
                key: Ident::from_string("FORMAT"),
                val: Expr::Ident(Ident::from_string("parquet")),
            }],
        };
        assert_eq!(expected, node);
    }

    #[test]
    fn copy_to_from_query() {
        let node: CopyTo<_> = parse_ast("COPY (select 1) TO 'myfile.csv'").unwrap();
        let expected = CopyTo {
            source: CopyToSource::Query(QueryNode {
                ctes: None,
                body: QueryNodeBody::Select(Box::new(SelectNode {
                    distinct: None,
                    projections: vec![SelectExpr::Expr(Expr::Literal(Literal::Number(
                        "1".to_string(),
                    )))],
                    from: None,
                    where_expr: None,
                    group_by: None,
                    having: None,
                })),
                order_by: None,
                limit: LimitModifier {
                    limit: None,
                    offset: None,
                },
            }),
            target: CopyToTarget::File("myfile.csv".to_string()),
            options: Vec::new(),
        };
        assert_eq!(expected, node);
    }
}

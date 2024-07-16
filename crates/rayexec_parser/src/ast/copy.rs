use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

use super::{AstParseable, Expr, ObjectReference, QueryNode};

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
pub struct CopyTo<T: AstMeta> {
    pub source: CopyToSource<T>,
    pub target: T::CopyToDestination,
    // TODO: Options
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

        // TODO: Options

        Ok(CopyTo { source, target })
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
                order_by: Vec::new(),
                limit: LimitModifier {
                    limit: None,
                    offset: None,
                },
            }),
            target: CopyToTarget::File("myfile.csv".to_string()),
        };
        assert_eq!(expected, node);
    }
}

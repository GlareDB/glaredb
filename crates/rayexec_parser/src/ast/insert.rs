use super::{AstParseable, Ident, ObjectReference, QueryNode};
use crate::{
    keywords::Keyword,
    meta::{AstMeta, Raw},
    parser::Parser,
    tokens::Token,
};
use rayexec_error::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Insert<T: AstMeta> {
    pub table: T::TableReference,
    pub columns: Vec<Ident>,
    pub source: QueryNode<T>,
}

impl AstParseable for Insert<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::INSERT)?;
        parser.expect_keyword(Keyword::INTO)?;

        let table = ObjectReference::parse(parser)?;

        let columns = if parser.consume_token(&Token::LeftParen) {
            let columns = parser.parse_comma_separated(Ident::parse)?;
            parser.expect_token(&Token::RightParen)?;
            columns
        } else {
            Vec::new()
        };

        let source = QueryNode::parse(parser)?;

        Ok(Insert {
            table,
            columns,
            source,
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
            order_by: None,
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
        let got = parse_ast("insert into t1 values (1)").unwrap();
        let expected = Insert {
            table: ObjectReference::from_strings(["t1"]),
            columns: Vec::new(),
            source: query_node_values_1(),
        };
        assert_eq!(expected, got);
    }

    #[test]
    fn with_columns() {
        let got = parse_ast("insert into t1(c1, c2) values (1)").unwrap();
        let expected = Insert {
            table: ObjectReference::from_strings(["t1"]),
            columns: vec![Ident::from_string("c1"), Ident::from_string("c2")],
            source: query_node_values_1(),
        };
        assert_eq!(expected, got);
    }
}

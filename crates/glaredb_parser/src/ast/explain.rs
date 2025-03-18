use glaredb_error::{DbError, Result};
use serde::{Deserialize, Serialize};

use super::{AstParseable, QueryNode};
use crate::keywords::Keyword;
use crate::meta::{AstMeta, Raw};
use crate::parser::Parser;
use crate::statement::Statement;
use crate::tokens::Token;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ExplainOutput {
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainNode<T: AstMeta> {
    pub analyze: bool,
    pub verbose: bool,
    pub body: ExplainBody<T>,
    pub output: Option<ExplainOutput>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExplainBody<T: AstMeta> {
    Query(QueryNode<T>),
}

impl AstParseable for ExplainNode<Raw> {
    fn parse(parser: &mut Parser) -> Result<Self> {
        parser.expect_keyword(Keyword::EXPLAIN)?;

        let analyze = parser.parse_keyword(Keyword::ANALYZE);
        let verbose = parser.parse_keyword(Keyword::VERBOSE);

        let output = if parser.consume_token(&Token::LeftParen) {
            // Just FORMAT for now.
            parser.expect_keyword(Keyword::FORMAT)?;
            let format = if parser.parse_keyword(Keyword::JSON) {
                ExplainOutput::Json
            } else if parser.parse_keyword(Keyword::TEXT) {
                ExplainOutput::Text
            } else {
                return Err(DbError::new("Expect JSON or TEXT for explain format"));
            };
            parser.expect_token(&Token::RightParen)?;
            Some(format)
        } else {
            None
        };

        let body = match parser.parse_statement()? {
            Statement::Query(query) => ExplainBody::Query(query),
            other => {
                return Err(DbError::new(format!(
                    "Unexpected body in EXPLAIN: {other:?}"
                )));
            }
        };

        Ok(ExplainNode {
            analyze,
            verbose,
            body,
            output,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::testutil::parse_ast;
    use crate::ast::{Expr, LimitModifier, Literal, QueryNodeBody, SelectExpr, SelectNode};

    /// Query node for 'select 1'
    fn query_node_select_1() -> QueryNode<Raw> {
        QueryNode {
            ctes: None,
            body: QueryNodeBody::Select(Box::new(SelectNode {
                distinct: None,
                projections: vec![SelectExpr::Expr(Expr::Literal(Literal::Number("1".into())))],
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
        }
    }

    #[test]
    fn no_options() {
        let explain: ExplainNode<_> = parse_ast("explain select 1").unwrap();
        let expected = ExplainNode {
            analyze: false,
            verbose: false,
            body: ExplainBody::Query(query_node_select_1()),
            output: None,
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn format_json() {
        let explain: ExplainNode<_> = parse_ast("explain (format json) select 1").unwrap();
        let expected = ExplainNode {
            analyze: false,
            verbose: false,
            body: ExplainBody::Query(query_node_select_1()),
            output: Some(ExplainOutput::Json),
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn format_text() {
        let explain: ExplainNode<_> = parse_ast("explain (format text) select 1").unwrap();
        let expected = ExplainNode {
            analyze: false,
            verbose: false,
            body: ExplainBody::Query(query_node_select_1()),
            output: Some(ExplainOutput::Text),
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn format_unknown() {
        let _ = parse_ast::<ExplainNode<_>>("explain (format exemel) select 1").unwrap_err();
    }

    #[test]
    fn analyze() {
        let explain: ExplainNode<_> = parse_ast("explain analyze select 1").unwrap();
        let expected = ExplainNode {
            analyze: true,
            verbose: false,
            body: ExplainBody::Query(query_node_select_1()),
            output: None,
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn verbose() {
        let explain: ExplainNode<_> = parse_ast("explain verbose select 1").unwrap();
        let expected = ExplainNode {
            analyze: false,
            verbose: true,
            body: ExplainBody::Query(query_node_select_1()),
            output: None,
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn analyze_verbose() {
        let explain: ExplainNode<_> = parse_ast("explain analyze verbose select 1").unwrap();
        let expected = ExplainNode {
            analyze: true,
            verbose: true,
            body: ExplainBody::Query(query_node_select_1()),
            output: None,
        };
        assert_eq!(expected, explain)
    }

    #[test]
    fn verbose_analyze() {
        let _ = parse_ast::<ExplainNode<_>>("explain verbose analyze select 1").unwrap_err();
    }
}

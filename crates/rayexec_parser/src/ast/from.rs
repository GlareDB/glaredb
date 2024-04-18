use crate::{
    keywords::{Keyword, RESERVED_FOR_TABLE_ALIAS},
    parser::Parser,
    tokens::{Token, TokenWithLocation},
};
use rayexec_error::{RayexecError, Result};

use super::{AstParseable, Expr, FunctionArg, Ident, ObjectReference, QueryNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromNode {
    pub alias: Option<FromAlias>,
    pub body: FromNodeBody,
}

impl AstParseable for FromNode {
    fn parse(parser: &mut Parser) -> Result<Self> {
        // Build the first part of the FROM clause.
        let node = if parser.consume_token(&Token::LeftParen) {
            // Subquery
            //
            // `FROM (SELECT * FROM my_table) AS alias`
            let subquery = QueryNode::parse(parser)?;
            parser.expect_token(&Token::RightParen)?;
            let alias = Self::maybe_parse_alias(parser)?;
            FromNode {
                alias,
                body: FromNodeBody::Subquery(FromSubquery { query: subquery }),
            }
        } else {
            // Table or table function.
            let reference = ObjectReference::parse(parser)?;

            let body = match parser.peek() {
                Some(TokenWithLocation { token, .. }) if token == &Token::LeftParen => {
                    let args = parser.parse_parenthesized_comma_separated(FunctionArg::parse)?;
                    FromNodeBody::TableFunction(FromTableFunction { reference, args })
                }
                _ => FromNodeBody::BaseTable(FromBaseTable { reference }),
            };

            let alias = Self::maybe_parse_alias(parser)?;
            FromNode { alias, body }
        };

        // If followed by a join, recursively build up the FROM node using the
        // original node build above as the left part.

        let node = if parser.parse_keyword(Keyword::CROSS) {
            // <left> CROSS JOIN <right>
            parser.expect_keyword(Keyword::JOIN)?;
            let right = FromNode::parse(parser)?;
            let alias = Self::maybe_parse_alias(parser)?;
            FromNode {
                alias,
                body: FromNodeBody::Join(FromJoin {
                    left: Box::new(node),
                    right: Box::new(right),
                    join_type: JoinType::Cross,
                    join_condition: JoinCondition::None,
                }),
            }
        } else if parser.consume_token(&Token::Comma) {
            // <left>, <right>
            let right = FromNode::parse(parser)?;
            let alias = Self::maybe_parse_alias(parser)?;
            FromNode {
                alias,
                body: FromNodeBody::Join(FromJoin {
                    left: Box::new(node),
                    right: Box::new(right),
                    join_type: JoinType::Cross,
                    join_condition: JoinCondition::None,
                }),
            }
        } else {
            let kw = match parser.peek() {
                Some(tok) => match tok.keyword() {
                    Some(kw) => kw,
                    None => return Ok(node), // Probably an error, but that can be handled higher up with more context.
                },
                None => return Ok(node), // End of statement, FROM node is last part of the query.
            };

            let join_type = match kw {
                Keyword::JOIN | Keyword::INNER => {
                    parser.parse_keyword(Keyword::INNER); // Optional INNER
                    parser.expect_keyword(Keyword::JOIN)?;
                    JoinType::Inner
                }
                Keyword::LEFT => {
                    parser.expect_keyword(Keyword::LEFT)?;
                    let kw = parser.parse_one_of_keywords(&[
                        Keyword::JOIN,
                        Keyword::OUTER,
                        Keyword::ANTI,
                        Keyword::SEMI,
                    ]);
                    match kw {
                        Some(Keyword::JOIN) => JoinType::Left,
                        Some(Keyword::OUTER) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::Left
                        }
                        Some(Keyword::ANTI) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::LeftAnti
                        }
                        Some(Keyword::SEMI) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::LeftSemi
                        }
                        _ => return Err(RayexecError::new("Expected one of OUTER, SEMI, or JOIN")),
                    }
                }
                Keyword::RIGHT => {
                    parser.expect_keyword(Keyword::RIGHT)?;
                    let kw = parser.parse_one_of_keywords(&[
                        Keyword::JOIN,
                        Keyword::OUTER,
                        Keyword::ANTI,
                        Keyword::SEMI,
                    ]);
                    match kw {
                        Some(Keyword::JOIN) => JoinType::Right,
                        Some(Keyword::OUTER) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::Right
                        }
                        Some(Keyword::ANTI) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::RightAnti
                        }
                        Some(Keyword::SEMI) => {
                            parser.expect_keyword(Keyword::JOIN)?;
                            JoinType::RightSemi
                        }
                        _ => return Err(RayexecError::new("Expected one of OUTER, SEMI, or JOIN")),
                    }
                }
                Keyword::FULL => {
                    parser.expect_keyword(Keyword::FULL)?;
                    parser.parse_keyword(Keyword::OUTER); // Optional OUTER
                    parser.expect_keyword(Keyword::JOIN)?;
                    JoinType::Outer
                }
                _ => return Ok(node), // Unknown join keyword, probably time to start working on a different part of the query.
            };

            let right = FromNode::parse(parser)?;

            let kw: Option<Keyword> = parser.peek().and_then(|t| t.keyword());

            let join_condition = match kw {
                Some(Keyword::ON) => {
                    parser.parse_keyword(Keyword::ON);
                    let has_paren = parser.consume_token(&Token::LeftParen);
                    let condition = JoinCondition::On(Expr::parse(parser)?);
                    if has_paren {
                        parser.expect_token(&Token::RightParen)?;
                    }
                    condition
                }
                Some(Keyword::USING) => {
                    parser.parse_keyword(Keyword::USING);
                    JoinCondition::Using(parser.parse_parenthesized_comma_separated(Ident::parse)?)
                }
                _ => JoinCondition::None,
            };

            FromNode {
                alias: None, // TODO: Join alias?
                body: FromNodeBody::Join(FromJoin {
                    left: Box::new(node),
                    right: Box::new(right),
                    join_type,
                    join_condition,
                }),
            }
        };

        Ok(node)
    }
}

impl FromNode {
    fn maybe_parse_alias(parser: &mut Parser) -> Result<Option<FromAlias>> {
        let alias = match parser.parse_alias(RESERVED_FOR_TABLE_ALIAS)? {
            Some(alias) => alias,
            None => return Ok(None),
        };
        let columns = if parser.consume_token(&Token::LeftParen) {
            let aliases = parser.parse_comma_separated(Ident::parse)?;
            parser.expect_token(&Token::RightParen)?;
            Some(aliases)
        } else {
            None
        };

        Ok(Some(FromAlias { alias, columns }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromAlias {
    pub alias: Ident,
    pub columns: Option<Vec<Ident>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromNodeBody {
    BaseTable(FromBaseTable),
    Subquery(FromSubquery),
    TableFunction(FromTableFunction),
    Join(FromJoin),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromBaseTable {
    pub reference: ObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromSubquery {
    pub query: QueryNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromTableFunction {
    pub reference: ObjectReference,
    pub args: Vec<FunctionArg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromJoin {
    pub left: Box<FromNode>,
    pub right: Box<FromNode>,
    pub join_type: JoinType,
    pub join_condition: JoinCondition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Outer,
    LeftAnti,
    LeftSemi,
    RightAnti,
    RightSemi,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<Ident>),
    Natural,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{testutil::parse_ast, BinaryOperator, Literal};
    use pretty_assertions::assert_eq;

    #[test]
    fn base_table() {
        let node: FromNode = parse_ast("my_table").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::BaseTable(FromBaseTable {
                reference: ObjectReference(vec![Ident {
                    value: "my_table".into(),
                }]),
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn base_table_no_consume_order_by() {
        // Make sure we're not accidentally aliasing a table with a known keyword.
        let node: FromNode = parse_ast("my_table ORDER BY c1").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::BaseTable(FromBaseTable {
                reference: ObjectReference(vec![Ident {
                    value: "my_table".into(),
                }]),
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn base_table_alias_to_keyword() {
        // Allow aliasing to a keyword with explicit AS.
        let node: FromNode = parse_ast("my_table AS ORDER").unwrap();
        let expected = FromNode {
            alias: Some(FromAlias {
                alias: Ident::from_string("ORDER"),
                columns: None,
            }),
            body: FromNodeBody::BaseTable(FromBaseTable {
                reference: ObjectReference(vec![Ident {
                    value: "my_table".into(),
                }]),
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn base_table_alias() {
        let node: FromNode = parse_ast("my_table AS t1").unwrap();
        let expected = FromNode {
            alias: Some(FromAlias {
                alias: Ident { value: "t1".into() },
                columns: None,
            }),
            body: FromNodeBody::BaseTable(FromBaseTable {
                reference: ObjectReference(vec![Ident {
                    value: "my_table".into(),
                }]),
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn base_table_alias_with_cols() {
        let node: FromNode = parse_ast("my_table AS t1(c1, c2,c3)").unwrap();
        let expected = FromNode {
            alias: Some(FromAlias {
                alias: Ident { value: "t1".into() },
                columns: Some(vec![
                    Ident { value: "c1".into() },
                    Ident { value: "c2".into() },
                    Ident { value: "c3".into() },
                ]),
            }),
            body: FromNodeBody::BaseTable(FromBaseTable {
                reference: ObjectReference(vec![Ident {
                    value: "my_table".into(),
                }]),
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn table_func() {
        let node: FromNode = parse_ast("my_table_func('arg1', kw = 'arg2')").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::TableFunction(FromTableFunction {
                reference: ObjectReference(vec![Ident {
                    value: "my_table_func".into(),
                }]),
                args: vec![
                    FunctionArg::Unnamed {
                        arg: Expr::Literal(Literal::SingleQuotedString("arg1".to_string())),
                    },
                    FunctionArg::Named {
                        name: Ident { value: "kw".into() },
                        arg: Expr::Literal(Literal::SingleQuotedString("arg2".to_string())),
                    },
                ],
            }),
        };
        assert_eq!(expected, node)
    }

    #[test]
    fn inner_join_on() {
        let node: FromNode = parse_ast("table1 INNER JOIN table2 ON (c1 = c2)").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::Join(FromJoin {
                left: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table1"]),
                    }),
                }),
                right: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table2"]),
                    }),
                }),
                join_type: JoinType::Inner,
                join_condition: JoinCondition::On(Expr::BinaryExpr {
                    left: Box::new(Expr::Ident(Ident::from_string("c1"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Ident(Ident::from_string("c2"))),
                }),
            }),
        };
        assert_eq!(expected, node);
    }

    #[test]
    fn inner_join_on_no_parens() {
        let node: FromNode = parse_ast("table1 INNER JOIN table2 ON c1 = c2").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::Join(FromJoin {
                left: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table1"]),
                    }),
                }),
                right: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table2"]),
                    }),
                }),
                join_type: JoinType::Inner,
                join_condition: JoinCondition::On(Expr::BinaryExpr {
                    left: Box::new(Expr::Ident(Ident::from_string("c1"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Ident(Ident::from_string("c2"))),
                }),
            }),
        };
        assert_eq!(expected, node);
    }

    #[test]
    fn inner_join_using() {
        let node: FromNode = parse_ast("table1 INNER JOIN table2 USING (c1, c2,c3)").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::Join(FromJoin {
                left: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table1"]),
                    }),
                }),
                right: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["table2"]),
                    }),
                }),
                join_type: JoinType::Inner,
                join_condition: JoinCondition::Using(vec![
                    Ident::from_string("c1"),
                    Ident::from_string("c2"),
                    Ident::from_string("c3"),
                ]),
            }),
        };
        assert_eq!(expected, node);
    }

    #[test]
    fn nested_join() {
        let node: FromNode = parse_ast("t1 LEFT JOIN t2 RIGHT JOIN t3").unwrap();
        let expected = FromNode {
            alias: None,
            body: FromNodeBody::Join(FromJoin {
                left: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::BaseTable(FromBaseTable {
                        reference: ObjectReference::from_strings(["t1"]),
                    }),
                }),
                right: Box::new(FromNode {
                    alias: None,
                    body: FromNodeBody::Join(FromJoin {
                        left: Box::new(FromNode {
                            alias: None,
                            body: FromNodeBody::BaseTable(FromBaseTable {
                                reference: ObjectReference::from_strings(["t2"]),
                            }),
                        }),
                        right: Box::new(FromNode {
                            alias: None,
                            body: FromNodeBody::BaseTable(FromBaseTable {
                                reference: ObjectReference::from_strings(["t3"]),
                            }),
                        }),
                        join_type: JoinType::Right,
                        join_condition: JoinCondition::None,
                    }),
                }),
                join_type: JoinType::Left,
                join_condition: JoinCondition::None,
            }),
        };
        assert_eq!(expected, node, "left:\n{expected:#?}\nright:\n{node:#?}");
    }
}

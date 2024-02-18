use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_error::Result;
use rayexec_parser::{
    ast::{
        AstParseable, FunctionArg, GroupByList, ObjectReference, OrderByList, SelectItem,
        SelectList, SelectNode, TableLike, TableList, TableOrSubquery, WildcardExpr,
    },
    parser::Parser,
    tokens::Tokenizer,
};

/// Parse a string into a part of the AST.
///
/// The provided string should be able to be tokenized. The returned Result is
/// the result of the actual parse.
fn parse_ast<'a, A: AstParseable<'a>>(s: &'a str) -> Result<A> {
    let toks = Tokenizer::new(s).tokenize().unwrap();
    let mut parser = Parser::with_tokens(toks);
    A::parse(&mut parser)
}

#[test]
fn parse_select() {
    assert_debug_snapshot!(parse_ast::<SelectNode>("select 1").unwrap(), @r###"
    SelectNode {
        modifier: None,
        projections: SelectList(
            [
                Expr(
                    Literal(
                        Number(
                            "1",
                        ),
                    ),
                ),
            ],
        ),
        from: TableList(
            [],
        ),
        where_expr: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
    "###);

    // TODO: Incorrect (expr parsing)
    assert_debug_snapshot!(parse_ast::<SelectNode>("SELECT * FROM customer WHERE id = 1 LIMIT 5").unwrap(), @r###"
    SelectNode {
        modifier: None,
        projections: SelectList(
            [
                Wildcard(
                    Wildcard {
                        exclude_cols: [],
                        replace_cols: [],
                    },
                ),
            ],
        ),
        from: TableList(
            [
                TableWithJoins {
                    table: Table(
                        ObjectReference(
                            [
                                Ident {
                                    value: "customer",
                                },
                            ],
                        ),
                    ),
                    joins: [],
                },
            ],
        ),
        where_expr: Some(
            Ident(
                Ident {
                    value: "id",
                },
            ),
        ),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
    "###);

    // TODO: Incorrect (expr parsing)
    assert_debug_snapshot!(parse_ast::<SelectNode>("SELECT c.* FROM customer c WHERE id = 1 LIMIT 5").unwrap(), @r###"
    SelectNode {
        modifier: None,
        projections: SelectList(
            [
                QualifiedWildcard(
                    ObjectReference(
                        [
                            Ident {
                                value: "c",
                            },
                        ],
                    ),
                    Wildcard {
                        exclude_cols: [],
                        replace_cols: [],
                    },
                ),
            ],
        ),
        from: TableList(
            [
                TableWithJoins {
                    table: Table(
                        ObjectReference(
                            [
                                Ident {
                                    value: "customer",
                                },
                            ],
                        ),
                    ),
                    joins: [],
                },
            ],
        ),
        where_expr: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
    "###);

    // TODO: Incorrect (expr parsing)
    assert_debug_snapshot!(parse_ast::<SelectNode>("SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5").unwrap(), @r###"
    SelectNode {
        modifier: None,
        projections: SelectList(
            [
                Expr(
                    Ident(
                        Ident {
                            value: "id",
                        },
                    ),
                ),
                Expr(
                    Ident(
                        Ident {
                            value: "fname",
                        },
                    ),
                ),
                AliasedExpr(
                    Ident(
                        Ident {
                            value: "lname",
                        },
                    ),
                    Ident {
                        value: "FROM",
                    },
                ),
            ],
        ),
        from: TableList(
            [],
        ),
        where_expr: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
    "###);

    // TODO: Incorrect (expr parsing)
    assert_debug_snapshot!(parse_ast::<SelectNode>("SELECT DISTINCT id, fname, lname FROM customer WHERE id = 1 LIMIT 5").unwrap(), @r###"
    SelectNode {
        modifier: Distinct,
        projections: SelectList(
            [
                Expr(
                    Ident(
                        Ident {
                            value: "id",
                        },
                    ),
                ),
                Expr(
                    Ident(
                        Ident {
                            value: "fname",
                        },
                    ),
                ),
                AliasedExpr(
                    Ident(
                        Ident {
                            value: "lname",
                        },
                    ),
                    Ident {
                        value: "FROM",
                    },
                ),
            ],
        ),
        from: TableList(
            [],
        ),
        where_expr: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    }
    "###);
}

#[test]
fn parse_object_reference() {
    // Single ident
    assert_debug_snapshot!(parse_ast::<ObjectReference>("col").unwrap(), @r###"
    ObjectReference(
        [
            Ident {
                value: "col",
            },
        ],
    )
    "###);

    // Compound
    assert_debug_snapshot!(parse_ast::<ObjectReference>("table.col").unwrap(), @r###"
    ObjectReference(
        [
            Ident {
                value: "table",
            },
            Ident {
                value: "col",
            },
        ],
    )
    "###);

    // Deep compound
    assert_debug_snapshot!(parse_ast::<ObjectReference>("database.schema.table.col.property").unwrap(), @r###"
    ObjectReference(
        [
            Ident {
                value: "database",
            },
            Ident {
                value: "schema",
            },
            Ident {
                value: "table",
            },
            Ident {
                value: "col",
            },
            Ident {
                value: "property",
            },
        ],
    )
    "###);
}

#[test]
fn parse_wildcard_expr() {
    // Wildcard
    assert_debug_snapshot!(parse_ast::<WildcardExpr>("*").unwrap(), @"Wildcard");

    // Qualified wildcard
    assert_debug_snapshot!(parse_ast::<WildcardExpr>("table1.*").unwrap(), @r###"
    QualifiedWildcard(
        ObjectReference(
            [
                Ident {
                    value: "table1",
                },
            ],
        ),
    )
    "###);
}

#[test]
fn parse_select_item() {
    // Ident
    assert_debug_snapshot!(parse_ast::<SelectItem>("col1").unwrap(), @r###"
    Expr(
        Ident(
            Ident {
                value: "col1",
            },
        ),
    )
    "###);

    // Aliased ident
    assert_debug_snapshot!(parse_ast::<SelectItem>("col1 alias1").unwrap(), @r###"
    AliasedExpr(
        Ident(
            Ident {
                value: "col1",
            },
        ),
        Ident {
            value: "alias1",
        },
    )
    "###);

    // Aliased ident with AS
    assert_debug_snapshot!(parse_ast::<SelectItem>("col1 as alias1").unwrap(), @r###"
    AliasedExpr(
        Ident(
            Ident {
                value: "col1",
            },
        ),
        Ident {
            value: "alias1",
        },
    )
    "###);

    // Number
    assert_debug_snapshot!(parse_ast::<SelectItem>("1.23").unwrap(), @r###"
    Expr(
        Literal(
            Number(
                "1.23",
            ),
        ),
    )
    "###);

    // String
    assert_debug_snapshot!(parse_ast::<SelectItem>("'string'").unwrap(), @r###"
    Expr(
        Literal(
            SingleQuotedString(
                "string",
            ),
        ),
    )
    "###);

    // Wildcard
    assert_debug_snapshot!(parse_ast::<SelectItem>("*").unwrap(), @r###"
    Wildcard(
        Wildcard {
            exclude_cols: [],
            replace_cols: [],
        },
    )
    "###);

    // Qualifed wildcard
    assert_debug_snapshot!(parse_ast::<SelectItem>("table1.*").unwrap(), @r###"
    QualifiedWildcard(
        ObjectReference(
            [
                Ident {
                    value: "table1",
                },
            ],
        ),
        Wildcard {
            exclude_cols: [],
            replace_cols: [],
        },
    )
    "###);
}

#[test]
fn parse_select_list() {
    // Single ident
    assert_debug_snapshot!(parse_ast::<SelectList>("col1").unwrap(), @r###"
    SelectList(
        [
            Expr(
                Ident(
                    Ident {
                        value: "col1",
                    },
                ),
            ),
        ],
    )
    "###);

    // Many idents
    assert_debug_snapshot!(parse_ast::<SelectList>("col1, col2").unwrap(), @r###"
    SelectList(
        [
            Expr(
                Ident(
                    Ident {
                        value: "col1",
                    },
                ),
            ),
            Expr(
                Ident(
                    Ident {
                        value: "col2",
                    },
                ),
            ),
        ],
    )
    "###);

    // Many idents, trailing comma
    assert_debug_snapshot!(parse_ast::<SelectList>("col1, col2,").unwrap(), @r###"
    SelectList(
        [
            Expr(
                Ident(
                    Ident {
                        value: "col1",
                    },
                ),
            ),
            Expr(
                Ident(
                    Ident {
                        value: "col2",
                    },
                ),
            ),
        ],
    )
    "###);
}

#[test]
fn parse_function_arg() {
    // Unnamed
    assert_debug_snapshot!(parse_ast::<FunctionArg>("123").unwrap(), @r###"
    Unnamed {
        arg: Literal(
            Number(
                "123",
            ),
        ),
    }
    "###);

    // Named (=>)
    assert_debug_snapshot!(parse_ast::<FunctionArg>("my_arg => 123").unwrap(), @r###"
    Named {
        name: Ident {
            value: "my_arg",
        },
        arg: Literal(
            Number(
                "123",
            ),
        ),
    }
    "###);

    // Named (=>)
    assert_debug_snapshot!(parse_ast::<FunctionArg>("my_arg = 123").unwrap(), @r###"
    Named {
        name: Ident {
            value: "my_arg",
        },
        arg: Literal(
            Number(
                "123",
            ),
        ),
    }
    "###);
}

#[test]
fn parse_table_like() {
    // Table
    assert_debug_snapshot!(parse_ast::<TableLike>("table").unwrap(), @r###"
    Table(
        ObjectReference(
            [
                Ident {
                    value: "table",
                },
            ],
        ),
    )
    "###);

    // Qualified table
    assert_debug_snapshot!(parse_ast::<TableLike>("schema.table").unwrap(), @r###"
    Table(
        ObjectReference(
            [
                Ident {
                    value: "schema",
                },
                Ident {
                    value: "table",
                },
            ],
        ),
    )
    "###);

    // Function zero args
    assert_debug_snapshot!(parse_ast::<TableLike>("my_func()").unwrap(), @r###"
    Function {
        name: ObjectReference(
            [
                Ident {
                    value: "my_func",
                },
            ],
        ),
        args: [],
    }
    "###);

    // Function one unnamed arg
    assert_debug_snapshot!(parse_ast::<TableLike>("my_func('arg')").unwrap(), @r###"
    Function {
        name: ObjectReference(
            [
                Ident {
                    value: "my_func",
                },
            ],
        ),
        args: [
            Unnamed {
                arg: Literal(
                    SingleQuotedString(
                        "arg",
                    ),
                ),
            },
        ],
    }
    "###);

    // Function one named arg.
    assert_debug_snapshot!(parse_ast::<TableLike>("my_func(arg = 'arg')").unwrap(), @r###"
    Function {
        name: ObjectReference(
            [
                Ident {
                    value: "my_func",
                },
            ],
        ),
        args: [
            Named {
                name: Ident {
                    value: "arg",
                },
                arg: Literal(
                    SingleQuotedString(
                        "arg",
                    ),
                ),
            },
        ],
    }
    "###);

    // Function named and unnamed args.
    assert_debug_snapshot!(parse_ast::<TableLike>("my_func('arg1', named = 'arg2')").unwrap(), @r###"
    Function {
        name: ObjectReference(
            [
                Ident {
                    value: "my_func",
                },
            ],
        ),
        args: [
            Unnamed {
                arg: Literal(
                    SingleQuotedString(
                        "arg1",
                    ),
                ),
            },
            Named {
                name: Ident {
                    value: "named",
                },
                arg: Literal(
                    SingleQuotedString(
                        "arg2",
                    ),
                ),
            },
        ],
    }
    "###);
}

#[test]
fn parse_table_or_subquery() {
    // Table with no alias
    assert_debug_snapshot!(parse_ast::<TableOrSubquery>("my_table").unwrap(), @r###"
    TableOrSubquery {
        table: Table(
            ObjectReference(
                [
                    Ident {
                        value: "my_table",
                    },
                ],
            ),
        ),
        alias: None,
        col_aliases: None,
    }
    "###);

    // Table with alias
    assert_debug_snapshot!(parse_ast::<TableOrSubquery>("my_table as t1").unwrap(), @r###"
    TableOrSubquery {
        table: Table(
            ObjectReference(
                [
                    Ident {
                        value: "my_table",
                    },
                ],
            ),
        ),
        alias: Some(
            Ident {
                value: "t1",
            },
        ),
        col_aliases: None,
    }
    "###);

    // Table with column aliases
    assert_debug_snapshot!(parse_ast::<TableOrSubquery>("my_table as t1(c1, c2, c3)").unwrap(), @r###"
    TableOrSubquery {
        table: Table(
            ObjectReference(
                [
                    Ident {
                        value: "my_table",
                    },
                ],
            ),
        ),
        alias: Some(
            Ident {
                value: "t1",
            },
        ),
        col_aliases: Some(
            [
                Ident {
                    value: "c1",
                },
                Ident {
                    value: "c2",
                },
                Ident {
                    value: "c3",
                },
            ],
        ),
    }
    "###);
}

#[test]
fn parse_group_by_list() {
    // Group by expressions
    assert_debug_snapshot!(parse_ast::<GroupByList>("col1, col2, 3, 4").unwrap(), @r###"
    Exprs {
        exprs: [
            Expr(
                Ident(
                    Ident {
                        value: "col1",
                    },
                ),
            ),
            Expr(
                Ident(
                    Ident {
                        value: "col2",
                    },
                ),
            ),
            Expr(
                Literal(
                    Number(
                        "3",
                    ),
                ),
            ),
            Expr(
                Literal(
                    Number(
                        "4",
                    ),
                ),
            ),
        ],
    }
    "###);

    // Group by ident which happens to match a keyword.
    assert_debug_snapshot!(parse_ast::<GroupByList>("last").unwrap(), @r###"
    Exprs {
        exprs: [
            Expr(
                Ident(
                    Ident {
                        value: "last",
                    },
                ),
            ),
        ],
    }
    "###);

    // Group by all
    assert_debug_snapshot!(parse_ast::<GroupByList>("ALL").unwrap(), @"All");

    // Group by cube
    assert_debug_snapshot!(parse_ast::<GroupByList>("CUBE (col1, 2)").unwrap(), @r###"
    Exprs {
        exprs: [
            Cube(
                [
                    Ident(
                        Ident {
                            value: "col1",
                        },
                    ),
                    Literal(
                        Number(
                            "2",
                        ),
                    ),
                ],
            ),
        ],
    }
    "###);

    // Group by rollup
    assert_debug_snapshot!(parse_ast::<GroupByList>("ROLLUP (col1, 2)").unwrap(), @r###"
    Exprs {
        exprs: [
            Rollup(
                [
                    Ident(
                        Ident {
                            value: "col1",
                        },
                    ),
                    Literal(
                        Number(
                            "2",
                        ),
                    ),
                ],
            ),
        ],
    }
    "###);

    // Group by grouping sets
    assert_debug_snapshot!(parse_ast::<GroupByList>("GROUPING SETS (col1, 2)").unwrap(), @r###"
    Exprs {
        exprs: [
            GroupingSets(
                [
                    Ident(
                        Ident {
                            value: "col1",
                        },
                    ),
                    Literal(
                        Number(
                            "2",
                        ),
                    ),
                ],
            ),
        ],
    }
    "###);

    // Mix of expressions
    assert_debug_snapshot!(parse_ast::<GroupByList>("col1, CUBE(col2, 3)").unwrap(), @r###"
    Exprs {
        exprs: [
            Expr(
                Ident(
                    Ident {
                        value: "col1",
                    },
                ),
            ),
            Cube(
                [
                    Ident(
                        Ident {
                            value: "col2",
                        },
                    ),
                    Literal(
                        Number(
                            "3",
                        ),
                    ),
                ],
            ),
        ],
    }
    "###);
}

#[test]
fn parse_order_by_list() {
    assert_debug_snapshot!(parse_ast::<OrderByList>("col1").unwrap(), @r###"
    Exprs {
        exprs: [
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col1",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: None,
                },
            },
        ],
    }
    "###);

    assert_debug_snapshot!(parse_ast::<OrderByList>("col1, col2").unwrap(), @r###"
    Exprs {
        exprs: [
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col1",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: None,
                },
            },
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col2",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: None,
                },
            },
        ],
    }
    "###);

    assert_debug_snapshot!(parse_ast::<OrderByList>("col1 NULLS LAST, col2").unwrap(), @r###"
    Exprs {
        exprs: [
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col1",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: Some(
                        Last,
                    ),
                },
            },
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col2",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: None,
                },
            },
        ],
    }
    "###);

    assert_debug_snapshot!(parse_ast::<OrderByList>("col1 ASC NULLS LAST, col2").unwrap(), @r###"
    Exprs {
        exprs: [
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col1",
                    },
                ),
                options: OrderByOptions {
                    asc: Some(
                        Ascending,
                    ),
                    nulls: Some(
                        Last,
                    ),
                },
            },
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col2",
                    },
                ),
                options: OrderByOptions {
                    asc: None,
                    nulls: None,
                },
            },
        ],
    }
    "###);

    assert_debug_snapshot!(parse_ast::<OrderByList>("col1 ASC NULLS LAST, col2 DESC").unwrap(), @r###"
    Exprs {
        exprs: [
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col1",
                    },
                ),
                options: OrderByOptions {
                    asc: Some(
                        Ascending,
                    ),
                    nulls: Some(
                        Last,
                    ),
                },
            },
            OrderByExpr {
                expr: Ident(
                    Ident {
                        value: "col2",
                    },
                ),
                options: OrderByOptions {
                    asc: Some(
                        Descending,
                    ),
                    nulls: None,
                },
            },
        ],
    }
    "###);
}

#[test]
fn parse_table_list() {
    assert_debug_snapshot!(parse_ast::<TableList>("my_table").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "my_table",
                            },
                        ],
                    ),
                ),
                joins: [],
            },
        ],
    )
    "###);

    // Alias
    // TODO: Column aliases
    assert_debug_snapshot!(parse_ast::<TableList>("t1 as a1(c1, c2, c3)").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [],
            },
        ],
    )
    "###);

    // Implicit cross join
    assert_debug_snapshot!(parse_ast::<TableList>("t1, t2").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [],
            },
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t2",
                            },
                        ],
                    ),
                ),
                joins: [],
            },
        ],
    )
    "###);

    // Cross join
    assert_debug_snapshot!(parse_ast::<TableList>("t1 cross join t2").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Cross,
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Implicit inner
    assert_debug_snapshot!(parse_ast::<TableList>("t1 join t2").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Inner(
                                None,
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Inner
    assert_debug_snapshot!(parse_ast::<TableList>("t1 inner join t2 on true").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Inner(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Left
    assert_debug_snapshot!(parse_ast::<TableList>("t1 left join t2 on true").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Left(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Right
    assert_debug_snapshot!(parse_ast::<TableList>("t1 right join t2 on true").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Right(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Full
    assert_debug_snapshot!(parse_ast::<TableList>("t1 full join t2 on true").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Full(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Full optional outer
    assert_debug_snapshot!(parse_ast::<TableList>("t1 full outer join t2 on true").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Full(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Inner using
    assert_debug_snapshot!(parse_ast::<TableList>("t1 full outer join t2 using (c1)").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Full(
                                Using(
                                    [
                                        Ident {
                                            value: "c1",
                                        },
                                    ],
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
        ],
    )
    "###);

    // Inner and implicit cross join
    assert_debug_snapshot!(parse_ast::<TableList>("t1 inner join t2 on true, t3").unwrap(), @r###"
    TableList(
        [
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t1",
                            },
                        ],
                    ),
                ),
                joins: [
                    Join {
                        join: JoinOperation {
                            join_modifier: None,
                            join_type: Inner(
                                On(
                                    Literal(
                                        Boolean(
                                            true,
                                        ),
                                    ),
                                ),
                            ),
                        },
                        table: Table(
                            ObjectReference(
                                [
                                    Ident {
                                        value: "t2",
                                    },
                                ],
                            ),
                        ),
                    },
                ],
            },
            TableWithJoins {
                table: Table(
                    ObjectReference(
                        [
                            Ident {
                                value: "t3",
                            },
                        ],
                    ),
                ),
                joins: [],
            },
        ],
    )
    "###);
}

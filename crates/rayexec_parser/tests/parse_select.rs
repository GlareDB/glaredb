use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_error::Result;
use rayexec_parser::{
    ast::{
        AstParseable, FunctionArg, GroupByExpr, ObjectReference, SelectItem, SelectList, TableLike,
        TableOrSubquery, WildcardExpr,
    },
    parser::Parser,
};

/// Parse a string into a part of the AST.
///
/// The provided string should be able to be tokenized. The returned Result is
/// the result of the actual parse.
fn parse_ast<'a, A: AstParseable<'a>>(s: &'a str) -> Result<A> {
    let mut parser = Parser::with_sql_string(s).unwrap();
    A::parse(&mut parser)
}

#[test]
fn parse_select_simple() {
    // assert_debug_snapshot!(parse("select 1").unwrap(), @"");
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
fn parse_group_by_expr() {
    // Group by expressions
    assert_debug_snapshot!(parse_ast::<GroupByExpr>("col1, col2, 3, 4").unwrap(), @r###"
    Exprs(
        [
            Ident(
                Ident {
                    value: "col1",
                },
            ),
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
            Literal(
                Number(
                    "4",
                ),
            ),
        ],
    )
    "###);

    // Group by all
    assert_debug_snapshot!(parse_ast::<GroupByExpr>("ALL").unwrap(), @"All");

    // Group by cube
    assert_debug_snapshot!(parse_ast::<GroupByExpr>("CUBE (col1, 2)").unwrap(), @r###"
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
    )
    "###);

    // Group by rollup
    assert_debug_snapshot!(parse_ast::<GroupByExpr>("ROLLUP (col1, 2)").unwrap(), @r###"
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
    )
    "###);

    // Group by grouping sets
    assert_debug_snapshot!(parse_ast::<GroupByExpr>("GROUPING SETS (col1, 2)").unwrap(), @r###"
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
    )
    "###);
}

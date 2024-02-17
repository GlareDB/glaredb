use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_error::Result;
use rayexec_parser::{
    ast::{AstParseable, SelectItem, WildcardExpr},
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

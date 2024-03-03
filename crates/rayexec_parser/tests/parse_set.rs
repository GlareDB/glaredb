use insta::{assert_debug_snapshot, assert_display_snapshot};
use rayexec_parser::parser::parse;

#[test]
fn parse_set_simple_to_string() {
    assert_debug_snapshot!(parse("set my_var to 'my_value'").unwrap(), @r###"
    [
        SetVariable {
            reference: ObjectReference(
                [
                    Ident {
                        value: "my_var",
                    },
                ],
            ),
            value: Literal(
                SingleQuotedString(
                    "my_value",
                ),
            ),
        },
    ]
    "###)
}

#[test]
fn parse_set_simple_eq_string() {
    assert_debug_snapshot!(parse("set my_var = 'my_value'").unwrap(), @r###"
    [
        SetVariable {
            reference: ObjectReference(
                [
                    Ident {
                        value: "my_var",
                    },
                ],
            ),
            value: Literal(
                SingleQuotedString(
                    "my_value",
                ),
            ),
        },
    ]
    "###)
}

#[test]
fn parse_set_compound_eq_string() {
    assert_debug_snapshot!(parse("set my.var = 'my_value'").unwrap(), @r###"
    [
        SetVariable {
            reference: ObjectReference(
                [
                    Ident {
                        value: "my",
                    },
                    Ident {
                        value: "var",
                    },
                ],
            ),
            value: Literal(
                SingleQuotedString(
                    "my_value",
                ),
            ),
        },
    ]
    "###)
}

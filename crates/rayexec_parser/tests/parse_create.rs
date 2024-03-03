use insta::{assert_debug_snapshot};
use rayexec_parser::parser::parse;

#[test]
fn parse_create_schema() {
    assert_debug_snapshot!(parse("create schema test_schema").unwrap(), @r###"
    [
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "test_schema",
                    },
                ],
            ),
            if_not_exists: false,
        },
    ]
    "###);
}

#[test]
fn parse_create_schema_compound() {
    assert_debug_snapshot!(parse("create schema test_database.test_schema;").unwrap(), @r###"
    [
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "test_database",
                    },
                    Ident {
                        value: "test_schema",
                    },
                ],
            ),
            if_not_exists: false,
        },
    ]
    "###);
}

#[test]
fn parse_create_schema_multiple_statements() {
    assert_debug_snapshot!(parse(
        "create schema schema1; create schema schema2 ;;create schema schema3"
    ).unwrap(), @r###"
    [
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "schema1",
                    },
                ],
            ),
            if_not_exists: false,
        },
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "schema2",
                    },
                ],
            ),
            if_not_exists: false,
        },
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "schema3",
                    },
                ],
            ),
            if_not_exists: false,
        },
    ]
    "###);
}

#[test]
fn parse_create_schema_if_not_exists() {
    assert_debug_snapshot!(parse("create schema if not exists schema1").unwrap(), @r###"
    [
        CreateSchema {
            reference: ObjectReference(
                [
                    Ident {
                        value: "schema1",
                    },
                ],
            ),
            if_not_exists: true,
        },
    ]
    "###);
}

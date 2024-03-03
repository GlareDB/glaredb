use insta::{assert_debug_snapshot};
use rayexec_parser::tokens::Tokenizer;

#[test]
fn tokenize_simple_string() {
    assert_debug_snapshot!(Tokenizer::new("select 'hello'").tokenize().unwrap(), @r###"
        [
            TokenWithLocation {
                token: Word(
                    Word {
                        value: "select",
                        quote: None,
                        keyword: Some(
                            SELECT,
                        ),
                    },
                ),
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Whitespace,
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: SingleQuotedString(
                    "hello",
                ),
                line: 0,
                col: 0,
            },
        ]
        "###)
}

#[test]
fn tokenize_simple_from() {
    assert_debug_snapshot!(Tokenizer::new("select * from my_table;").tokenize().unwrap(), @r###"
        [
            TokenWithLocation {
                token: Word(
                    Word {
                        value: "select",
                        quote: None,
                        keyword: Some(
                            SELECT,
                        ),
                    },
                ),
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Whitespace,
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Mul,
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Whitespace,
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Word(
                    Word {
                        value: "from",
                        quote: None,
                        keyword: Some(
                            FROM,
                        ),
                    },
                ),
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Whitespace,
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: Word(
                    Word {
                        value: "my_table",
                        quote: None,
                        keyword: None,
                    },
                ),
                line: 0,
                col: 0,
            },
            TokenWithLocation {
                token: SemiColon,
                line: 0,
                col: 0,
            },
        ]
        "###);
}

#[test]
fn tokenize_simple_from_compound() {
    assert_debug_snapshot!(Tokenizer::new("select * from my_schema.my_table;").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Word(
                Word {
                    value: "select",
                    quote: None,
                    keyword: Some(
                        SELECT,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Mul,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "from",
                    quote: None,
                    keyword: Some(
                        FROM,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "my_schema",
                    quote: None,
                    keyword: None,
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Period,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "my_table",
                    quote: None,
                    keyword: None,
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: SemiColon,
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_multiline() {
    assert_debug_snapshot!(Tokenizer::new(r###"select *
from my_schema.my_table;"###).tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Word(
                Word {
                    value: "select",
                    quote: None,
                    keyword: Some(
                        SELECT,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Mul,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "from",
                    quote: None,
                    keyword: Some(
                        FROM,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "my_schema",
                    quote: None,
                    keyword: None,
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Period,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "my_table",
                    quote: None,
                    keyword: None,
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: SemiColon,
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_compound() {
    assert_debug_snapshot!(Tokenizer::new("database.schema.table.property").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Word(
                Word {
                    value: "database",
                    quote: None,
                    keyword: Some(
                        DATABASE,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Period,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "schema",
                    quote: None,
                    keyword: Some(
                        SCHEMA,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Period,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "table",
                    quote: None,
                    keyword: Some(
                        TABLE,
                    ),
                },
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Period,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Word(
                Word {
                    value: "property",
                    quote: None,
                    keyword: None,
                },
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_integer() {
    assert_debug_snapshot!(Tokenizer::new("12345").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                "12345",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_float() {
    assert_debug_snapshot!(Tokenizer::new("123.45").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                "123.45",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_float_period_start() {
    assert_debug_snapshot!(Tokenizer::new(".123").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                ".123",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###)
}

#[test]
fn tokenize_number_too_many_periods() {
    // TODO: We should likely error on this. Note that sqlparser-rs tokenizes to
    // the same thing.
    assert_debug_snapshot!(Tokenizer::new("123.45.67").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                "123.45",
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Number(
                ".67",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);
}

#[test]
fn tokenize_compound_operators() {
    assert_debug_snapshot!(Tokenizer::new("1 >= 2").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                "1",
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: GtEq,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Whitespace,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Number(
                "2",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);

    assert_debug_snapshot!(Tokenizer::new("1>=2").tokenize().unwrap(), @r###"
    [
        TokenWithLocation {
            token: Number(
                "1",
            ),
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: GtEq,
            line: 0,
            col: 0,
        },
        TokenWithLocation {
            token: Number(
                "2",
            ),
            line: 0,
            col: 0,
        },
    ]
    "###);
}

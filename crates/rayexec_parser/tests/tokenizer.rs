use insta::assert_debug_snapshot;
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
                    value: "my_schema.my_table",
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
                    value: "my_schema.my_table",
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

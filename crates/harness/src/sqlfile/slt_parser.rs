use glaredb_error::Result;

use super::parser::{DelimitedBy, DelimitedLines, Location, ParseableRecord, Parser};

/// Records that can be found in an SLT file.
#[derive(Debug, PartialEq, Eq)]
pub enum SltRecord<'a> {
    Statement(RecordStatement<'a>),
    Query(RecordQuery<'a>),
    Halt(RecordHalt<'a>),
}

impl<'a> SltRecord<'a> {
    pub fn parse_many(filename: &'a str, input: &'a str) -> Result<Vec<SltRecord<'a>>> {
        let mut records = Vec::new();
        let mut parser = Parser::new(filename, input);

        loop {
            let record = parser
                .dispatch_parse(|parser, prefix| match prefix {
                    RecordStatement::RECORD_START => {
                        Ok(SltRecord::Statement(RecordStatement::parse(parser)?))
                    }
                    RecordQuery::RECORD_START => Ok(SltRecord::Query(RecordQuery::parse(parser)?)),
                    RecordHalt::RECORD_START => Ok(SltRecord::Halt(RecordHalt::parse(parser)?)),
                    other => panic!("Unexpected prefix: {other}"),
                })
                .unwrap();

            match record {
                Some(record) => records.push(record),
                None => break,
            }
        }

        Ok(records)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    NoSort,
    RowSort,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedError<'a> {
    Empty,
    Inline(&'a str),
    Multiline(Vec<&'a str>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExpectedResults<'a> {
    Empty,
    Multiline(Vec<&'a str>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatementExpect<'a> {
    Ok,
    Error(ExpectedError<'a>),
}

/// The valid types are:
/// - 'T' - text, varchar results
/// - 'I' - integers
/// - 'R' - floating point numbers
///
/// Any other types are represented with `?`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    Text,
    Integer,
    Float,
    Any,
}

impl ColumnType {
    pub fn from_char(value: char) -> Option<Self> {
        match value {
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::Float),
            _ => Some(Self::Any),
        }
    }

    pub fn to_char(&self) -> char {
        match self {
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::Float => 'R',
            Self::Any => '?',
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordHalt<'a> {
    pub loc: Location<'a>,
}

impl<'a> ParseableRecord<'a> for RecordHalt<'a> {
    const RECORD_START: &'static str = "halt";

    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;
        if !line.line.is_empty() {
            return line
                .loc
                .emit_error("'halt' record should not have extra labels");
        }

        Ok(RecordHalt { loc: line.loc })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordStatement<'a> {
    pub loc: Location<'a>,
    pub sql: DelimitedLines<'a>,
    pub expected: StatementExpect<'a>,
}

impl<'a> ParseableRecord<'a> for RecordStatement<'a> {
    const RECORD_START: &'static str = "statement";

    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;

        let mut labels = line.line.splitn(2, ' ');
        let mut expected = match labels.next() {
            Some("ok") => {
                if labels.next().is_some() {
                    return line
                        .loc
                        .emit_error("'statement ok' should not have any extra labels");
                }
                StatementExpect::Ok
            }
            Some("error") => match labels.next() {
                Some(rem) => StatementExpect::Error(ExpectedError::Inline(rem)),
                None => StatementExpect::Error(ExpectedError::Empty),
            },
            other => {
                return line.loc.emit_error(format!(
                    "'statement' should have an 'ok' or 'error' label, got '{other:?}'"
                ));
            }
        };

        let sql = parser.take_lines_delimited()?;
        match (sql.delimited_by, &expected) {
            (DelimitedBy::Dashes, StatementExpect::Error(ExpectedError::Empty)) => {
                // Multiline error.
                let error = parser.take_lines_delimited()?;
                if error.delimited_by == DelimitedBy::Dashes {
                    return line
                        .loc
                        .emit_error("Unexpected delimiter when reading multiline error");
                }

                expected = StatementExpect::Error(ExpectedError::Multiline(error.lines))
            }
            (DelimitedBy::Dashes, StatementExpect::Error(_)) => {
                return line
                    .loc
                    .emit_error("Eror already defined inline, cannot also include mutline error");
            }
            (DelimitedBy::Dashes, StatementExpect::Ok) => {
                return line
                    .loc
                    .emit_error("'statement' should not have dashes for delimiting results, use 'query' if results should be checked");
            }
            (_, _) => {
                // End of this record.
                ()
            }
        }

        Ok(RecordStatement {
            loc: line.loc,
            sql,
            expected,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordQuery<'a> {
    pub loc: Location<'a>,
    pub sql: Vec<&'a str>,
    pub types: Vec<ColumnType>,
    pub sort_mode: SortMode,
    pub results: Vec<&'a str>,
}

impl<'a> ParseableRecord<'a> for RecordQuery<'a> {
    const RECORD_START: &'static str = "query";

    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;
        let mut labels = line.line.split(' ');
        let types = match labels.next() {
            Some(s) => s
                .chars()
                .map(|c| {
                    ColumnType::from_char(c)
                        .ok_or_else(|| line.loc.format_error(format!("Unexpected type: 'c'")))
                })
                .collect::<Result<Vec<_>>>()?,
            None => return line.loc.emit_error("Missing types for 'query'"),
        };

        let sort_mode = match labels.next() {
            Some("nosort") | None => SortMode::NoSort,
            Some("rowsort") => SortMode::RowSort,
            Some(other) => {
                return line
                    .loc
                    .emit_error(format!("Unexpected sort mode: '{other}'"));
            }
        };

        if let Some(rem) = labels.next() {
            return line.loc.emit_error(format!(
                "Unexpected remaining options for 'query' record: '{rem}'"
            ));
        }

        let sql = parser.take_lines_delimited()?;
        if sql.delimited_by == DelimitedBy::Newline {
            return line.loc.emit_error("Missing expected results for query");
        }

        let results = parser.take_lines_delimited()?;
        if results.delimited_by == DelimitedBy::Dashes {
            return line
                .loc
                .emit_error("Expected newline after expected results, got dash delimiters");
        }

        Ok(RecordQuery {
            loc: line.loc,
            sql: sql.lines,
            types,
            sort_mode,
            results: results.lines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_parse_ok() {
        struct TestCase {
            input: &'static str,
            expected: RecordStatement<'static>,
        }

        let test_cases = [
            TestCase {
                input: r#"statement ok
SELECT 1;"#,
                expected: RecordStatement {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: DelimitedLines {
                        lines: vec!["SELECT 1;"],
                        delimited_by: DelimitedBy::Eof,
                    },
                    expected: StatementExpect::Ok,
                },
            },
            TestCase {
                input: r#"statement ok
SELECT 1,
2;"#,
                expected: RecordStatement {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: DelimitedLines {
                        lines: vec!["SELECT 1,", "2;"],
                        delimited_by: DelimitedBy::Eof,
                    },
                    expected: StatementExpect::Ok,
                },
            },
            TestCase {
                input: r#"statement ok
SELECT 1,
2;

statement error
SELECT 'different record';"#,
                expected: RecordStatement {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: DelimitedLines {
                        lines: vec!["SELECT 1,", "2;"],
                        delimited_by: DelimitedBy::Newline,
                    },
                    expected: StatementExpect::Ok,
                },
            },
            TestCase {
                input: r#"statement error my inline error
SELECT 1,
2;

statement error
SELECT 'different record';"#,
                expected: RecordStatement {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: DelimitedLines {
                        lines: vec!["SELECT 1,", "2;"],
                        delimited_by: DelimitedBy::Newline,
                    },
                    expected: StatementExpect::Error(ExpectedError::Inline("my inline error")),
                },
            },
            TestCase {
                input: r#"statement error
SELECT 1,
2;
----
my
multiline
error

statement error
SELECT 'different record';"#,
                expected: RecordStatement {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: DelimitedLines {
                        lines: vec!["SELECT 1,", "2;"],
                        delimited_by: DelimitedBy::Dashes,
                    },
                    expected: StatementExpect::Error(ExpectedError::Multiline(vec![
                        "my",
                        "multiline",
                        "error",
                    ])),
                },
            },
        ];

        for test_case in test_cases {
            let mut parser = Parser::new("test", test_case.input);
            let got = RecordStatement::parse(&mut parser).unwrap();
            assert_eq!(test_case.expected, got);
        }
    }

    #[test]
    fn query_parse_ok() {
        struct TestCase {
            input: &'static str,
            expected: RecordQuery<'static>,
        }

        let test_cases = [
            TestCase {
                input: r#"query III
SELECT 1,2,3;
----
1 2 3
"#,
                expected: RecordQuery {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: vec!["SELECT 1,2,3;"],
                    types: vec![
                        ColumnType::Integer,
                        ColumnType::Integer,
                        ColumnType::Integer,
                    ],
                    sort_mode: SortMode::NoSort,
                    results: vec!["1 2 3"],
                },
            },
            TestCase {
                input: r#"query III rowsort
SELECT 1,2,3;
----
1 2 3
"#,
                expected: RecordQuery {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: vec!["SELECT 1,2,3;"],
                    types: vec![
                        ColumnType::Integer,
                        ColumnType::Integer,
                        ColumnType::Integer,
                    ],
                    sort_mode: SortMode::RowSort,
                    results: vec!["1 2 3"],
                },
            },
            TestCase {
                input: r#"query III rowsort
SELECT a,b,c;
----
1 2 3
4 5 6

statement error
SELECT 'different record';
"#,
                expected: RecordQuery {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: vec!["SELECT a,b,c;"],
                    types: vec![
                        ColumnType::Integer,
                        ColumnType::Integer,
                        ColumnType::Integer,
                    ],
                    sort_mode: SortMode::RowSort,
                    results: vec!["1 2 3", "4 5 6"],
                },
            },
            TestCase {
                input: r#"query RTI rowsort
SELECT a,b,c;
----
1.0 ggg 3
4.0 hhh 6
"#,
                expected: RecordQuery {
                    loc: Location {
                        line: 1,
                        file: "test",
                    },
                    sql: vec!["SELECT a,b,c;"],
                    types: vec![ColumnType::Float, ColumnType::Text, ColumnType::Integer],
                    sort_mode: SortMode::RowSort,
                    results: vec!["1.0 ggg 3", "4.0 hhh 6"],
                },
            },
        ];

        for test_case in test_cases {
            let mut parser = Parser::new("test", test_case.input);
            let got = RecordQuery::parse(&mut parser).unwrap();
            assert_eq!(test_case.expected, got);
        }
    }

    #[test]
    fn parse_slt_multiple_records() {
        let input = r#"
# Simple SLT-like input.

statement ok
SELECT 4;

query ITT
SELECT a,
b,
c;
----
hhh 4 5
jjj 6 7

halt

statement error does not exist
select func_does_not_exist();

"#;

        let records = SltRecord::parse_many("test", input).unwrap();
        assert_eq!(4, records.len());

        assert_eq!(
            SltRecord::Statement(RecordStatement {
                loc: Location {
                    line: 4,
                    file: "test"
                },
                sql: DelimitedLines {
                    lines: vec!["SELECT 4;"],
                    delimited_by: DelimitedBy::Newline,
                },
                expected: StatementExpect::Ok,
            }),
            records[0]
        );
        assert_eq!(
            SltRecord::Query(RecordQuery {
                loc: Location {
                    line: 7,
                    file: "test"
                },
                sql: vec!["SELECT a,", "b,", "c;"],
                results: vec!["hhh 4 5", "jjj 6 7"],
                types: vec![ColumnType::Integer, ColumnType::Text, ColumnType::Text],
                sort_mode: SortMode::NoSort,
            }),
            records[1]
        );
        assert_eq!(
            SltRecord::Halt(RecordHalt {
                loc: Location {
                    line: 15,
                    file: "test"
                },
            }),
            records[2]
        );
        assert_eq!(
            SltRecord::Statement(RecordStatement {
                loc: Location {
                    line: 17,
                    file: "test"
                },
                sql: DelimitedLines {
                    lines: vec!["select func_does_not_exist();"],
                    delimited_by: DelimitedBy::Newline,
                },
                expected: StatementExpect::Error(ExpectedError::Inline("does not exist"))
            }),
            records[3]
        );
    }

    #[test]
    fn parse_slt_query_rowsort() {
        let input = r#"query II rowsort
select 1,2
----
1 2

query II nosort
select 4,5
----
4 5"#;

        let records = SltRecord::parse_many("test", input).unwrap();
        assert_eq!(2, records.len());

        assert_eq!(
            SltRecord::Query(RecordQuery {
                loc: Location {
                    line: 1,
                    file: "test"
                },
                sql: vec!["select 1,2"],
                results: vec!["1 2"],
                types: vec![ColumnType::Integer, ColumnType::Integer],
                sort_mode: SortMode::RowSort,
            }),
            records[0]
        );
        assert_eq!(
            SltRecord::Query(RecordQuery {
                loc: Location {
                    line: 6,
                    file: "test"
                },
                sql: vec!["select 4,5"],
                results: vec!["4 5"],
                types: vec![ColumnType::Integer, ColumnType::Integer],
                sort_mode: SortMode::NoSort,
            }),
            records[1]
        );
    }
}

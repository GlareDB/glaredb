use std::iter::Peekable;
use std::str::Lines;

use glaredb_error::{DbError, Result};

use super::column_type::ColumnType;

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

pub trait ParseableRecord<'a>: Sized {
    /// String representing the start of a record.
    const RECORD_START: &'static str;

    /// Parses the record from the provided state.
    ///
    /// The parser will be on the line indicated by `RECORD_START`.
    fn parse(parser: &mut Parser<'a>) -> Result<Self>;
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordHalt {
    pub loc: usize,
}

impl<'a> ParseableRecord<'a> for RecordHalt {
    const RECORD_START: &'static str = "halt";

    fn parse(parser: &mut Parser) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;
        if !line.line.is_empty() {
            return line.emit_error("'halt' record should not have extra labels");
        }

        Ok(RecordHalt { loc: line.loc })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordStatement<'a> {
    pub loc: usize,
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
                    return line.emit_error("'statement ok' should not have any extra labels");
                }
                StatementExpect::Ok
            }
            Some("error") => match labels.next() {
                Some(rem) => StatementExpect::Error(ExpectedError::Inline(rem)),
                None => StatementExpect::Error(ExpectedError::Empty),
            },
            _ => return line.emit_error("'statement' should have an 'ok' or 'error' label"),
        };

        let sql = parser.take_lines_delimited()?;
        match (sql.delimited_by, &expected) {
            (DelimitedBy::Dashes, StatementExpect::Error(ExpectedError::Empty)) => {
                // Multiline error.
                let error = parser.take_lines_delimited()?;
                if error.delimited_by == DelimitedBy::Dashes {
                    return line.emit_error("Unexpected delimiter when reading multiline error");
                }

                expected = StatementExpect::Error(ExpectedError::Multiline(error.lines))
            }
            (DelimitedBy::Dashes, StatementExpect::Error(_)) => {
                return line
                    .emit_error("Eror already defined inline, cannot also include mutline error");
            }
            (DelimitedBy::Dashes, StatementExpect::Ok) => {
                return line
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
    pub loc: usize,
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
                        .ok_or_else(|| line.format_error(format!("Unexpected type: 'c'")))
                })
                .collect::<Result<Vec<_>>>()?,
            None => unimplemented!(),
        };

        let sort_mode = match labels.next() {
            Some("nosort") | None => SortMode::NoSort,
            Some("rowsort") => SortMode::RowSort,
            Some(other) => return line.emit_error(format!("Unexpected sort mode: '{other}'")),
        };

        if let Some(rem) = labels.next() {
            return line.emit_error(format!(
                "Unexpected remaining options for 'query' record: '{rem}'"
            ));
        }

        let sql = parser.take_lines_delimited()?;
        if sql.delimited_by == DelimitedBy::Newline {
            return line.emit_error("Missing expected results for query");
        }

        let results = parser.take_lines_delimited()?;
        if results.delimited_by == DelimitedBy::Dashes {
            return line.emit_error("Expected newline after expected results, got dash delimiters");
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

#[derive(Debug, PartialEq, Eq)]
pub struct TrimmedLine<'a> {
    /// File this line is from.
    pub file: &'a str,
    /// Line within the file.
    pub loc: usize,
    /// The line itself, with a prefix trimmed.
    pub line: &'a str,
}

impl TrimmedLine<'_> {
    pub fn emit_error<T>(&self, msg: impl Into<String>) -> Result<T> {
        Err(self.format_error(msg))
    }

    pub fn format_error(&self, msg: impl Into<String>) -> DbError {
        DbError::new(msg).with_field("location", format!("{}:{}", self.file, self.loc))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelimitedBy {
    Newline,
    Dashes,
    Eof,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DelimitedLines<'a> {
    pub lines: Vec<&'a str>,
    pub delimited_by: DelimitedBy,
}

#[derive(Debug)]
pub struct Parser<'a> {
    file: &'a str,
    curr_loc: usize,
    lines: Peekable<Lines<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(file: &'a str, input: &'a str) -> Self {
        let lines = input.lines().peekable();
        Parser {
            file,
            curr_loc: 1,
            lines,
        }
    }

    fn take_lines_delimited(&mut self) -> Result<DelimitedLines<'a>> {
        let curr = self.curr_loc;
        let mut lines = Vec::new();
        while let Some(line) = self.lines.next() {
            self.curr_loc += 1;
            match line.trim() {
                "----" => {
                    return Ok(DelimitedLines {
                        lines,
                        delimited_by: DelimitedBy::Dashes,
                    });
                }
                "" => {
                    return Ok(DelimitedLines {
                        lines,
                        delimited_by: DelimitedBy::Newline,
                    });
                }
                _ => lines.push(line),
            }
        }

        Ok(DelimitedLines {
            lines,
            delimited_by: DelimitedBy::Eof,
        })
    }

    fn take_next_and_trim_start(&mut self, trim: &str) -> Result<TrimmedLine<'a>> {
        let loc = self.curr_loc;
        let line = self
            .lines
            .next()
            .ok_or_else(|| DbError::new("Unexpected end of input"))?;
        self.curr_loc += 1;

        let trimmed = line.trim().trim_start_matches(trim).trim();

        Ok(TrimmedLine {
            file: self.file,
            loc,
            line: trimmed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_statements_parse_ok() {
        struct TestCase {
            input: &'static str,
            expected: RecordStatement<'static>,
        }

        let test_cases = [
            TestCase {
                input: r#"statement ok
SELECT 1;"#,
                expected: RecordStatement {
                    loc: 1,
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
                    loc: 1,
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
                    loc: 1,
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
                    loc: 1,
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
                    loc: 1,
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
}

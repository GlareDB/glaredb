use glaredb_error::{DbError, Result};

use super::parser::{Location, ParseableRecord, Parser};
use crate::sqlfile::parser::DelimitedBy;

/// Records found in a bench file.
#[derive(Debug, PartialEq, Eq)]
pub enum BenchRecord<'a> {
    Setup(RecordSetup<'a>),
    Run(RecordRun<'a>),
}

impl<'a> BenchRecord<'a> {
    pub fn parse_many(filename: &'a str, input: &'a str) -> Result<Vec<BenchRecord<'a>>> {
        let mut records = Vec::new();
        let mut parser = Parser::new(filename, input);

        loop {
            let record = parser.dispatch_parse(|parser, prefix| match prefix {
                RecordSetup::RECORD_START => Ok(BenchRecord::Setup(RecordSetup::parse(parser)?)),
                RecordRun::RECORD_START => Ok(BenchRecord::Run(RecordRun::parse(parser)?)),
                other => Err(DbError::new(format!("Unexpected prefix: {other}"))),
            })?;

            match record {
                Some(record) => records.push(record),
                None => break,
            }
        }

        Ok(records)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordSetup<'a> {
    pub loc: Location<'a>,
    pub sql: Vec<&'a str>,
}

impl<'a> ParseableRecord<'a> for RecordSetup<'a> {
    const RECORD_START: &'static str = "setup";

    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;
        if !line.line.is_empty() {
            return line
                .loc
                .emit_error("'setup' should not have any extra labels");
        }

        let sql = parser.take_lines_delimited()?;
        if sql.delimited_by == DelimitedBy::Dashes {
            return line
                .loc
                .emit_error("'setup' should be delimited by a newline or end-of-file");
        }

        Ok(RecordSetup {
            loc: line.loc,
            sql: sql.lines,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RecordRun<'a> {
    pub loc: Location<'a>,
    pub sql: Vec<&'a str>,
}

impl<'a> ParseableRecord<'a> for RecordRun<'a> {
    const RECORD_START: &'static str = "run";

    fn parse(parser: &mut Parser<'a>) -> Result<Self> {
        let line = parser.take_next_and_trim_start(Self::RECORD_START)?;
        if !line.line.is_empty() {
            return line
                .loc
                .emit_error("'run' should not have any extra labels");
        }

        let sql = parser.take_lines_delimited()?;
        if sql.delimited_by == DelimitedBy::Dashes {
            return line
                .loc
                .emit_error("'run' should be delimited by a newline or end-of-file");
        }

        Ok(RecordRun {
            loc: line.loc,
            sql: sql.lines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_run() {
        let contents = r#"run
SELECT * FROM hello
"#;

        let records = BenchRecord::parse_many("test", contents).unwrap();
        assert_eq!(1, records.len());

        assert_eq!(
            BenchRecord::Run(RecordRun {
                loc: Location::new("test", 1),
                sql: vec!["SELECT * FROM hello"],
            }),
            records[0]
        );
    }

    #[test]
    fn multiple_setups_and_runs() {
        let contents = r#"
# This is a comment.

setup
CREATE TABLE hello (a INT)

setup
CREATE TABLE hello2 (a INT)

run
SELECT * FROM hello

run
SELECT * FROM hello2
"#;

        let records = BenchRecord::parse_many("test", contents).unwrap();
        assert_eq!(4, records.len());

        assert_eq!(
            BenchRecord::Setup(RecordSetup {
                loc: Location::new("test", 4),
                sql: vec!["CREATE TABLE hello (a INT)"],
            }),
            records[0],
        );
        assert_eq!(
            BenchRecord::Setup(RecordSetup {
                loc: Location::new("test", 7),
                sql: vec!["CREATE TABLE hello2 (a INT)"],
            }),
            records[1],
        );
        assert_eq!(
            BenchRecord::Run(RecordRun {
                loc: Location::new("test", 10),
                sql: vec!["SELECT * FROM hello"],
            }),
            records[2],
        );
        assert_eq!(
            BenchRecord::Run(RecordRun {
                loc: Location::new("test", 13),
                sql: vec!["SELECT * FROM hello2"],
            }),
            records[3],
        );
    }

    #[test]
    fn multline_setup() {
        let contents = r#"
setup
CREATE TABLE
  hello (a INT)

run
SELECT * FROM hello"#;

        let records = BenchRecord::parse_many("test", contents).unwrap();
        assert_eq!(2, records.len());

        assert_eq!(
            BenchRecord::Setup(RecordSetup {
                loc: Location::new("test", 2),
                sql: vec!["CREATE TABLE", "  hello (a INT)"],
            }),
            records[0],
        );
        assert_eq!(
            BenchRecord::Run(RecordRun {
                loc: Location::new("test", 6),
                sql: vec!["SELECT * FROM hello"],
            }),
            records[1],
        );
    }
}
